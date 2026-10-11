//! The [`SubjectCipher`] adapter — the audit-side rationale/PII sealer and the home
//! of the crypto-shred DEK store.
//!
//! Envelope encryption: a random per-subject **DEK** AEAD-encrypts the PII; the DEK
//! is itself **wrapped** and the wrapped blob stored in `subject_keys` (one DEK per
//! subject, get-or-create). The plane only ever holds ciphertext thereafter, and
//! crypto-shred ([`PgKeyVault::destroy_subject_key`](super::PgKeyVault) = delete the
//! row) destroys the wrapped DEK, making every envelope for that subject permanently
//! undecryptable.
//!
//! The only thing that differs between deployments is **who holds the KEK** — that
//! is the [`KekCustodian`] seam:
//!
//! * [`LocalKek`] — the env-KEK AES-256-GCM wrap ([`super::envelope`]). The
//!   wrapped DEK and its nonce both live in `subject_keys`. This is the local/dev
//!   fallback ([`AesGcmSubjectCipher`]); the raw KEK sits in the service env, so it
//!   protects against an app-credential attacker but **not** a DB/infra operator.
//! * [`KmsKek`] — the DEK is wrapped/unwrapped by **KMS** ([`KmsCipher`]) under a
//!   principal the ledger DB role cannot assume ([`KmsSubjectCipher`], issue #482).
//!   No raw key material ever enters audit's env or memory; the `subject_keys`
//!   schema is unchanged (the KMS ciphertext blob goes in `wrapped_dek`, no nonce).
//!
//! The composition root picks the custodian by config; the ingest path and the
//! domain are untouched either way.
//!
//! Switching custody (#485): each row records the custodian that wrapped it
//! (`subject_keys.custodian`; a row from before that column is read by its shape —
//! an AES-GCM wrap has a nonce, a KMS blob none). A row is unwrapped by **its**
//! custodian when the cipher holds it as a reader, then re-wrapped under the
//! configured one, so env KEK → KMS migrates row by row as subjects are sealed.
//! Unwrapped DEKs are cached ([`DekCache`]) against the row's wrapped bytes: the
//! row is still read on every seal (a crypto-shred is seen at once), but KMS is
//! asked once per subject per process, not once per seal.

use std::collections::HashMap;
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};
use std::time::{Duration, Instant};

use async_trait::async_trait;
use chrono::Utc;
use postgres_storage::TransactionManager;

use crate::application::port::SubjectCipher;
use crate::domain::{PiiEnvelope, SubjectKeyRef, SubjectPseudonym};
use crate::error::AuditError;
use crate::infrastructure::envelope::{self, KEY_LEN};
use crate::infrastructure::kms::KmsCipher;

/// A wrapped DEK as persisted in `subject_keys`: the opaque wrapped-key bytes plus
/// an optional wrap nonce (used by the local AES-GCM custodian; empty for KMS,
/// which carries its own nonce inside the ciphertext blob).
pub struct WrappedDek {
    pub wrapped: Vec<u8>,
    pub nonce: Vec<u8>,
}

/// Custody of the **key-encryption key** (KEK) that wraps the per-subject DEKs.
/// This is the seam issue #482 swaps: the in-process env KEK ([`LocalKek`]) vs KMS
/// ([`KmsKek`]). The DEK plaintext only ever exists transiently in the cipher; the
/// store holds the wrapped form.
#[async_trait]
pub trait KekCustodian: Send + Sync + 'static {
    /// Wrap a freshly-minted DEK for storage.
    async fn wrap(&self, dek: &[u8; KEY_LEN]) -> Result<WrappedDek, AuditError>;
    /// Unwrap a stored DEK back to plaintext for sealing.
    async fn unwrap(&self, wrapped: &[u8], nonce: &[u8]) -> Result<[u8; KEY_LEN], AuditError>;
}

/// In-process KEK custody: AES-256-GCM wrap under a KEK read from the service env.
/// The local/dev fallback (not operator-proof — the KEK lives beside the service).
pub struct LocalKek {
    kek: [u8; KEY_LEN],
}

impl LocalKek {
    pub fn new(kek: [u8; KEY_LEN]) -> Self {
        Self { kek }
    }
}

#[async_trait]
impl KekCustodian for LocalKek {
    async fn wrap(&self, dek: &[u8; KEY_LEN]) -> Result<WrappedDek, AuditError> {
        let sealed = envelope::seal(&self.kek, dek)?;
        Ok(WrappedDek {
            wrapped: sealed.ciphertext,
            nonce: sealed.nonce,
        })
    }

    async fn unwrap(&self, wrapped: &[u8], nonce: &[u8]) -> Result<[u8; KEY_LEN], AuditError> {
        let dek = envelope::open(&self.kek, wrapped, nonce)?;
        to_key(dek)
    }
}

/// KMS KEK custody (issue #482): the DEK is wrapped/unwrapped by KMS under a key the
/// ledger DB role cannot assume. Audit never holds the raw KEK; the stored
/// `wrapped_dek` is the opaque KMS ciphertext blob (KMS recovers the key id from it
/// on decrypt, so no nonce column is needed).
pub struct KmsKek {
    kms: Arc<dyn KmsCipher>,
    key_id: String,
}

impl KmsKek {
    pub fn new(kms: Arc<dyn KmsCipher>, key_id: String) -> Self {
        Self { kms, key_id }
    }
}

#[async_trait]
impl KekCustodian for KmsKek {
    async fn wrap(&self, dek: &[u8; KEY_LEN]) -> Result<WrappedDek, AuditError> {
        let wrapped = self.kms.encrypt(&self.key_id, dek).await?;
        Ok(WrappedDek {
            wrapped,
            nonce: Vec::new(),
        })
    }

    async fn unwrap(&self, wrapped: &[u8], _nonce: &[u8]) -> Result<[u8; KEY_LEN], AuditError> {
        let dek = self.kms.decrypt(wrapped).await?;
        to_key(dek)
    }
}

/// Who wrapped a stored DEK (#485).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Custodian {
    /// The env KEK (AES-256-GCM, [`LocalKek`]).
    Local,
    /// KMS ([`KmsKek`]).
    Kms,
}

impl Custodian {
    fn tag(self) -> &'static str {
        match self {
            Self::Local => "local",
            Self::Kms => "kms",
        }
    }

    /// The row's custodian: its tag, else its shape (an AES-GCM wrap carries a
    /// nonce; a KMS blob carries its own and stores none).
    fn of_row(tag: Option<&str>, nonce: &[u8]) -> Self {
        match tag {
            Some("local") => Self::Local,
            Some("kms") => Self::Kms,
            _ if nonce.is_empty() => Self::Kms,
            _ => Self::Local,
        }
    }
}

/// Unwrapped DEKs, each kept with the wrapped bytes it came from: a hit needs the
/// row read on this seal to hold the same bytes, so a crypto-shredded (deleted) or
/// re-wrapped row is never served from memory. Entries expire after `ttl`, which
/// bounds how long a DEK outlives its row in memory.
/// A cached DEK: the wrapped bytes it came from, the DEK, when it was cached.
type CachedDek = (Vec<u8>, [u8; KEY_LEN], Instant);

pub struct DekCache {
    entries: Mutex<HashMap<String, CachedDek>>,
    ttl:     Duration,
    cap:     usize,
}

impl DekCache {
    pub fn new(ttl: Duration, cap: usize) -> Self {
        Self { entries: Mutex::new(HashMap::new()), ttl, cap: cap.max(1) }
    }

    /// A poisoned lock still holds a consistent map (no operation here panics
    /// midway), so it is used as is rather than panicking the seal path.
    fn entries(&self) -> MutexGuard<'_, HashMap<String, CachedDek>> {
        self.entries.lock().unwrap_or_else(PoisonError::into_inner)
    }

    fn get(&self, key_ref: &str, wrapped: &[u8]) -> Option<[u8; KEY_LEN]> {
        let mut entries = self.entries();
        match entries.get(key_ref) {
            Some((w, dek, at)) if w == wrapped && at.elapsed() < self.ttl => Some(*dek),
            Some(_) => {
                entries.remove(key_ref);
                None
            }
            None => None,
        }
    }

    fn put(&self, key_ref: &str, wrapped: Vec<u8>, dek: [u8; KEY_LEN]) {
        let mut entries = self.entries();
        if entries.len() >= self.cap {
            let ttl = self.ttl;
            entries.retain(|_, (_, _, at)| at.elapsed() < ttl);
            if entries.len() >= self.cap {
                entries.clear();
            }
        }
        entries.insert(key_ref.to_owned(), (wrapped, dek, Instant::now()));
    }

    fn evict(&self, key_ref: &str) {
        self.entries().remove(key_ref);
    }
}

impl Default for DekCache {
    /// Ten minutes, 10 000 subjects.
    fn default() -> Self {
        Self::new(Duration::from_secs(600), 10_000)
    }
}

fn to_key(bytes: Vec<u8>) -> Result<[u8; KEY_LEN], AuditError> {
    bytes.try_into().map_err(|_| AuditError::DomainViolation {
        field: "dek".to_owned(),
        message: "unwrapped DEK has the wrong length".to_owned(),
    })
}

/// Get-or-create the wrapped DEK for a subject. Fills a NULL/absent row exactly
/// once (a lost race keeps the winner's DEK — never overwrites an existing one, so
/// prior ciphertexts stay decryptable), then the caller re-reads the canonical DEK.
const UPSERT_DEK_SQL: &str = r#"
INSERT INTO subject_keys (key_ref, created_at_ms, wrapped_dek, wrap_nonce, custodian)
VALUES ($1, $2, $3, $4, $5)
ON CONFLICT (key_ref) DO UPDATE
    SET wrapped_dek = EXCLUDED.wrapped_dek, wrap_nonce = EXCLUDED.wrap_nonce,
        custodian = EXCLUDED.custodian
    WHERE subject_keys.wrapped_dek IS NULL
"#;

const FETCH_DEK_SQL: &str =
    "SELECT wrapped_dek, wrap_nonce, custodian FROM subject_keys WHERE key_ref = $1";

/// Re-wraps a DEK under the configured custodian, only while the row still holds
/// the wrap that was read (a racing re-wrap or shred wins).
const REWRAP_DEK_SQL: &str = r#"
UPDATE subject_keys SET wrapped_dek = $2, wrap_nonce = $3, custodian = $4
WHERE key_ref = $1 AND wrapped_dek = $5
"#;

/// `(wrapped_dek, wrap_nonce, custodian)` — all nullable (a row may pre-exist
/// without a DEK, or predate the custodian tag).
type WrappedDekRow = (Option<Vec<u8>>, Option<Vec<u8>>, Option<String>);

/// The envelope-encryption [`SubjectCipher`], generic over who holds the KEK. The
/// DEK store + AEAD sealing are identical across deployments; only the
/// [`KekCustodian`] differs (env KEK vs KMS).
pub struct EnvelopeCipher {
    tx: TransactionManager,
    /// The configured custodian: it wraps every new DEK and re-wraps the others.
    custodian: Custodian,
    kek: Arc<dyn KekCustodian>,
    /// Other custodians this cipher can still unwrap (a custody migration).
    readers: HashMap<Custodian, Arc<dyn KekCustodian>>,
    cache: DekCache,
}

impl EnvelopeCipher {
    pub fn new(tx: TransactionManager, custodian: Custodian, kek: Arc<dyn KekCustodian>) -> Self {
        Self { tx, custodian, kek, readers: HashMap::new(), cache: DekCache::default() }
    }

    /// Also unwraps rows `custodian` wrapped, re-wrapping them under the
    /// configured custodian (#485).
    pub fn reading(mut self, custodian: Custodian, kek: Arc<dyn KekCustodian>) -> Self {
        if custodian != self.custodian {
            self.readers.insert(custodian, kek);
        }
        self
    }

    fn unwrapper(&self, custodian: Custodian) -> Result<&Arc<dyn KekCustodian>, AuditError> {
        if custodian == self.custodian {
            return Ok(&self.kek);
        }
        self.readers.get(&custodian).ok_or_else(|| {
            tracing::error!(
                row = custodian.tag(),
                configured = self.custodian.tag(),
                "a subject DEK was wrapped by a custodian this cipher cannot read (#485): configure it as a reader"
            );
            AuditError::KeyVaultUnavailable
        })
    }

    /// The per-subject key reference — all of a subject's PII shares one DEK, so a
    /// single shred erases all of it.
    fn key_ref(subject: &SubjectPseudonym) -> String {
        format!("dek:{}", subject.as_str())
    }

    /// Resolve the subject's plaintext DEK, creating + wrapping one on first use.
    async fn dek_for(&self, key_ref: &str) -> Result<[u8; KEY_LEN], AuditError> {
        if let Some(dek) = self.fetch_dek(key_ref).await? {
            return Ok(dek);
        }
        // Create + wrap a fresh DEK; the upsert fills a NULL/absent row only.
        let dek = envelope::random_key()?;
        let wrapped = self.kek.wrap(&dek).await?;
        sqlx::query(UPSERT_DEK_SQL)
            .bind(key_ref)
            .bind(Utc::now().timestamp_millis())
            .bind(&wrapped.wrapped)
            .bind(&wrapped.nonce)
            .bind(self.custodian.tag())
            .execute(self.tx.pool())
            .await
            .map_err(|_| AuditError::KeyVaultUnavailable)?;
        // The canonical DEK: ours when our wrap is the one stored (no unwrap
        // needed), else a racing writer's that won.
        match self.read_row(key_ref).await? {
            Some((stored, _, _)) if stored == wrapped.wrapped => {
                self.cache.put(key_ref, stored, dek);
                Ok(dek)
            }
            _ => self.fetch_dek(key_ref).await?.ok_or(AuditError::KeyVaultUnavailable),
        }
    }

    async fn read_row(&self, key_ref: &str) -> Result<Option<(Vec<u8>, Vec<u8>, Option<String>)>, AuditError> {
        let row: Option<WrappedDekRow> = sqlx::query_as(FETCH_DEK_SQL)
            .bind(key_ref)
            .fetch_optional(self.tx.pool())
            .await
            .map_err(|_| AuditError::KeyVaultUnavailable)?;
        // The local custodian needs the nonce; KMS ignores it (and may have stored
        // an empty one). Default to empty so a NULL nonce is fine.
        Ok(row.and_then(|(wrapped, nonce, custodian)| Some((wrapped?, nonce.unwrap_or_default(), custodian))))
    }

    async fn fetch_dek(&self, key_ref: &str) -> Result<Option<[u8; KEY_LEN]>, AuditError> {
        // No row (shredded, or never minted), or a row without a wrapped DEK yet.
        let Some((wrapped, nonce, tag)) = self.read_row(key_ref).await? else {
            self.cache.evict(key_ref);
            return Ok(None);
        };
        if let Some(dek) = self.cache.get(key_ref, &wrapped) {
            return Ok(Some(dek));
        }
        let custodian = Custodian::of_row(tag.as_deref(), &nonce);
        let dek = self.unwrapper(custodian)?.unwrap(&wrapped, &nonce).await?;
        let stored = if custodian == self.custodian { wrapped } else { self.rewrap(key_ref, &dek, wrapped).await };
        self.cache.put(key_ref, stored, dek);
        Ok(Some(dek))
    }

    /// Re-wraps a DEK another custodian wrapped under the configured one (#485);
    /// the wrapped bytes the row now holds. Best-effort: on failure the row keeps
    /// its old wrap and is migrated at a later seal.
    async fn rewrap(&self, key_ref: &str, dek: &[u8; KEY_LEN], old: Vec<u8>) -> Vec<u8> {
        let fresh = match self.kek.wrap(dek).await {
            Ok(fresh) => fresh,
            Err(error) => {
                tracing::warn!(%error, "subject DEK re-wrap failed; kept its old custodian");
                return old;
            }
        };
        let updated = sqlx::query(REWRAP_DEK_SQL)
            .bind(key_ref)
            .bind(&fresh.wrapped)
            .bind(&fresh.nonce)
            .bind(self.custodian.tag())
            .bind(&old)
            .execute(self.tx.pool())
            .await;
        match updated {
            Ok(done) if done.rows_affected() == 1 => fresh.wrapped,
            Ok(_) => old, // a racing re-wrap or a shred got there first
            Err(error) => {
                tracing::warn!(%error, "subject DEK re-wrap not stored; kept its old custodian");
                old
            }
        }
    }

    async fn seal_impl(
        &self,
        subject: &SubjectPseudonym,
        plaintext: &str,
    ) -> Result<PiiEnvelope, AuditError> {
        let key_ref = Self::key_ref(subject);
        let dek = self.dek_for(&key_ref).await?;
        let sealed = envelope::seal(&dek, plaintext.as_bytes())?;
        Ok(PiiEnvelope::sealed(
            SubjectKeyRef::new(key_ref)?,
            sealed.ciphertext,
            sealed.nonce,
            envelope::ALGORITHM,
        ))
    }
}

#[async_trait]
impl SubjectCipher for EnvelopeCipher {
    async fn seal(
        &self,
        subject: &SubjectPseudonym,
        plaintext: &str,
    ) -> Result<PiiEnvelope, AuditError> {
        self.seal_impl(subject, plaintext).await
    }
}

/// The local/dev [`SubjectCipher`]: env-KEK AES-256-GCM DEK wrapping. The fallback
/// kept for local dev and CI; production prefers [`KmsSubjectCipher`].
pub struct AesGcmSubjectCipher(EnvelopeCipher);

impl AesGcmSubjectCipher {
    pub fn new(tx: TransactionManager, kek: [u8; KEY_LEN]) -> Self {
        Self(EnvelopeCipher::new(tx, Custodian::Local, Arc::new(LocalKek::new(kek))))
    }
}

#[async_trait]
impl SubjectCipher for AesGcmSubjectCipher {
    async fn seal(
        &self,
        subject: &SubjectPseudonym,
        plaintext: &str,
    ) -> Result<PiiEnvelope, AuditError> {
        self.0.seal_impl(subject, plaintext).await
    }
}

/// The production [`SubjectCipher`] (issue #482): the per-subject DEK is wrapped by
/// KMS, so no raw KEK ever lives in audit's env or memory. The `subject_keys`
/// schema and crypto-shred ("delete the wrapped-DEK row") are unchanged.
pub struct KmsSubjectCipher(EnvelopeCipher);

impl KmsSubjectCipher {
    pub fn new(tx: TransactionManager, kms: Arc<dyn KmsCipher>, key_id: String) -> Self {
        Self(EnvelopeCipher::new(tx, Custodian::Kms, Arc::new(KmsKek::new(kms, key_id))))
    }

    /// Also reads the DEKs the env KEK wrapped before custody moved to KMS, and
    /// re-wraps each under KMS at its subject's next seal (#485).
    pub fn reading_local(self, kek: [u8; KEY_LEN]) -> Self {
        Self(self.0.reading(Custodian::Local, Arc::new(LocalKek::new(kek))))
    }
}

#[async_trait]
impl SubjectCipher for KmsSubjectCipher {
    async fn seal(
        &self,
        subject: &SubjectPseudonym,
        plaintext: &str,
    ) -> Result<PiiEnvelope, AuditError> {
        self.0.seal_impl(subject, plaintext).await
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Mutex;

    use super::*;

    /// An in-memory KMS cipher fake: "wrapping" prefixes a marker so the test can
    /// assert the KMS path was taken, and unwrap strips it. Records call counts.
    #[derive(Default)]
    struct FakeKms {
        encrypts: Mutex<usize>,
        decrypts: Mutex<usize>,
    }

    const MARKER: &[u8] = b"KMSWRAP::";

    #[async_trait]
    impl KmsCipher for FakeKms {
        async fn encrypt(&self, _key_id: &str, plaintext: &[u8]) -> Result<Vec<u8>, AuditError> {
            *self.encrypts.lock().unwrap() += 1;
            let mut blob = MARKER.to_vec();
            blob.extend_from_slice(plaintext);
            Ok(blob)
        }

        async fn decrypt(&self, ciphertext: &[u8]) -> Result<Vec<u8>, AuditError> {
            *self.decrypts.lock().unwrap() += 1;
            ciphertext
                .strip_prefix(MARKER)
                .map(<[u8]>::to_vec)
                .ok_or(AuditError::KeyVaultUnavailable)
        }
    }

    #[tokio::test]
    async fn kms_kek_wraps_via_the_client_with_no_nonce() {
        let kms: Arc<dyn KmsCipher> = Arc::new(FakeKms::default());
        let kek = KmsKek::new(Arc::clone(&kms), "alias/audit-dek".to_owned());

        let dek = [9u8; KEY_LEN];
        let wrapped = kek.wrap(&dek).await.unwrap();
        assert!(wrapped.wrapped.starts_with(MARKER), "wrap must go through KMS");
        assert!(wrapped.nonce.is_empty(), "KMS path stores no wrap nonce");

        let unwrapped = kek.unwrap(&wrapped.wrapped, &wrapped.nonce).await.unwrap();
        assert_eq!(unwrapped, dek);
    }

    #[tokio::test]
    async fn local_kek_round_trips_with_a_nonce() {
        let kek = LocalKek::new([3u8; KEY_LEN]);
        let dek = [4u8; KEY_LEN];
        let wrapped = kek.wrap(&dek).await.unwrap();
        assert!(!wrapped.nonce.is_empty(), "AES-GCM wrap carries a nonce");
        assert_eq!(kek.unwrap(&wrapped.wrapped, &wrapped.nonce).await.unwrap(), dek);
    }

    #[tokio::test]
    async fn kms_unwrap_rejects_a_foreign_blob() {
        let kms: Arc<dyn KmsCipher> = Arc::new(FakeKms::default());
        let kek = KmsKek::new(kms, "alias/audit-dek".to_owned());
        // A blob KMS cannot decrypt (e.g. wrong key) surfaces as vault-unavailable.
        let err = kek.unwrap(b"not-a-kms-blob", &[]).await.unwrap_err();
        assert!(matches!(err, AuditError::KeyVaultUnavailable));
    }
}
