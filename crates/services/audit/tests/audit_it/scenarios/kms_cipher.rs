//! Issue #482 — crypto-shred end-to-end against the **KMS-backed** subject cipher,
//! over real Postgres. The KMS calls are mocked (no LocalStack needed in CI), but
//! the DEK store, the hash-chained ledger and the shred are the real adapters.
//!
//! What this proves: when KMS holds KEK custody, sealing a subject's PII mints a DEK
//! and asks KMS to *wrap* it (the opaque blob — never a raw KEK — lands in
//! `subject_keys`); the record chains as normal; and crypto-shred (delete the
//! wrapped-DEK row) makes the PII permanently unreadable while the chain still
//! verifies. The `subject_keys` schema is unchanged from the env-KEK path.

use std::sync::Mutex;

use async_trait::async_trait;

use audit::application::IntegrityStatus;
use audit::application::port::SubjectCipher;
use audit::domain::{EventCategory, SubjectKeyRef, SubjectPseudonym};
use audit::error::AuditError;
use audit::infrastructure::{KmsCipher, KmsSubjectCipher};

use postgres_storage::TransactionManager;

use crate::audit_it::harness::{Harness, fresh_tenant, partition_for, pii_event};

/// A stand-in for KMS: "wrapping" prefixes a marker (so we can assert the blob is
/// the KMS ciphertext, not a raw key) and counts the calls. The raw DEK is only
/// ever the plaintext argument — it never lands in the store.
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
async fn crypto_shred_works_against_the_kms_backed_cipher() {
    let h = Harness::start().await;

    let kms = std::sync::Arc::new(FakeKms::default());
    let cipher = KmsSubjectCipher::new(
        TransactionManager::new(h.pool.clone()),
        kms.clone(),
        "alias/audit-dek".to_owned(),
    );

    let tenant = fresh_tenant();
    let subject = SubjectPseudonym::new("subj-1").unwrap();

    // First seal mints a DEK and KMS-wraps it; the wrapped blob persists in real PG.
    let envelope = cipher.seal(&subject, "violates harassment policy 3.2").await.unwrap();
    assert!(!envelope.ciphertext().is_empty());
    assert_eq!(*kms.encrypts.lock().unwrap(), 1, "the DEK was wrapped via KMS");

    let key_ref = format!("dek:{}", subject.as_str());
    let key = SubjectKeyRef::new(key_ref.clone()).unwrap();

    // The stored wrapped DEK is the KMS ciphertext blob, never a raw key.
    let wrapped: Option<Vec<u8>> =
        sqlx::query_scalar("SELECT wrapped_dek FROM subject_keys WHERE key_ref = $1")
            .bind(&key_ref)
            .fetch_one(&h.pool)
            .await
            .unwrap();
    assert!(
        wrapped.as_deref().unwrap().starts_with(MARKER),
        "subject_keys must hold the KMS-wrapped DEK, not raw key material"
    );

    // Chain a record that references the sealed PII, then verify it stands.
    h.ingest().ingest(pii_event(&tenant, "kms-pii-1", &key_ref)).await.unwrap();
    let partition = partition_for(&tenant, EventCategory::DataAccess);
    assert!(h.key_vault.key_exists(&key).await.unwrap());
    assert_eq!(
        h.verify().verify_partition(&partition).await.unwrap().status,
        IntegrityStatus::Verified
    );

    // Crypto-shred: destroy the wrapped DEK. Even KMS can no longer recover it.
    h.shred().shred(&subject, &key, &[]).await.unwrap();

    // The DEK is gone (PII permanently undecryptable)...
    assert!(!h.key_vault.key_exists(&key).await.unwrap());
    // ...but the ledger row is untouched, so the chain STILL verifies.
    assert_eq!(
        h.verify().verify_partition(&partition).await.unwrap().status,
        IntegrityStatus::Verified
    );
}

async fn row(h: &Harness, key_ref: &str) -> (Vec<u8>, Vec<u8>, Option<String>) {
    sqlx::query_as::<_, (Option<Vec<u8>>, Option<Vec<u8>>, Option<String>)>(
        "SELECT wrapped_dek, wrap_nonce, custodian FROM subject_keys WHERE key_ref = $1",
    )
    .bind(key_ref)
    .fetch_one(&h.pool)
    .await
    .map(|(w, n, c)| (w.unwrap_or_default(), n.unwrap_or_default(), c))
    .unwrap()
}

/// #485: moving KEK custody from the env KEK to KMS strands no DEK. A row the env
/// KEK wrapped — tagged, or from before the tag — is unwrapped with it and
/// re-wrapped under KMS at the subject's next seal, keeping the same DEK (every
/// earlier envelope still opens). Without the env KEK as a reader, the seal fails
/// closed rather than minting over the subject's key.
#[tokio::test]
async fn kek_custody_moves_from_the_env_kek_to_kms_row_by_row() {
    use audit::infrastructure::envelope;
    use audit::infrastructure::AesGcmSubjectCipher;

    let h = Harness::start().await;
    let kek = [7u8; 32];
    let local = AesGcmSubjectCipher::new(TransactionManager::new(h.pool.clone()), kek);
    let subject = SubjectPseudonym::new(format!("subj-{}", uuid::Uuid::now_v7())).unwrap();
    let key_ref = format!("dek:{}", subject.as_str());

    let before = local.seal(&subject, "sealed under the env KEK").await.unwrap();
    let (wrapped, nonce, tag) = row(&h, &key_ref).await;
    assert_eq!(tag.as_deref(), Some("local"));
    let dek: [u8; 32] = envelope::open(&kek, &wrapped, &nonce).unwrap().try_into().unwrap();
    // A row from before the custodian column: read by its shape.
    sqlx::query("UPDATE subject_keys SET custodian = NULL WHERE key_ref = $1").bind(&key_ref).execute(&h.pool).await.unwrap();

    let kms = std::sync::Arc::new(FakeKms::default());
    let unreadable = KmsSubjectCipher::new(TransactionManager::new(h.pool.clone()), kms.clone(), "alias/audit-dek".to_owned());
    assert!(matches!(unreadable.seal(&subject, "x").await, Err(AuditError::KeyVaultUnavailable)), "fails closed");

    let migrating = KmsSubjectCipher::new(TransactionManager::new(h.pool.clone()), kms.clone(), "alias/audit-dek".to_owned())
        .reading_local(kek);
    migrating.seal(&subject, "sealed after the move").await.unwrap();
    let (wrapped, nonce, tag) = row(&h, &key_ref).await;
    assert_eq!(tag.as_deref(), Some("kms"), "re-wrapped under KMS");
    assert!(wrapped.starts_with(MARKER) && nonce.is_empty());
    assert_eq!(&wrapped[MARKER.len()..], &dek, "the same DEK, now KMS-wrapped");
    assert!(envelope::open(&dek, before.ciphertext(), before.nonce()).is_ok(), "earlier envelopes still open");
}

/// #485: the unwrapped DEK is cached against the row's wrapped bytes — KMS is
/// asked once per subject per process — while a crypto-shred (the row deleted)
/// is seen on the very next seal.
#[tokio::test]
async fn kms_is_asked_once_per_subject_and_a_shred_is_never_served_from_cache() {
    let h = Harness::start().await;
    let kms = std::sync::Arc::new(FakeKms::default());
    let cipher = KmsSubjectCipher::new(TransactionManager::new(h.pool.clone()), kms.clone(), "alias/audit-dek".to_owned());
    let subject = SubjectPseudonym::new(format!("subj-{}", uuid::Uuid::now_v7())).unwrap();
    let key = SubjectKeyRef::new(format!("dek:{}", subject.as_str())).unwrap();

    for _ in 0..3 {
        cipher.seal(&subject, "pii").await.unwrap();
    }
    assert_eq!((*kms.encrypts.lock().unwrap(), *kms.decrypts.lock().unwrap()), (1, 0), "minted once, never unwrapped");

    // Another process: one unwrap, then the cache.
    let other = KmsSubjectCipher::new(TransactionManager::new(h.pool.clone()), kms.clone(), "alias/audit-dek".to_owned());
    for _ in 0..3 {
        other.seal(&subject, "pii").await.unwrap();
    }
    assert_eq!(*kms.decrypts.lock().unwrap(), 1);

    // Shredded: the next seal mints a new DEK instead of using the cached one.
    h.shred().shred(&subject, &key, &[]).await.unwrap();
    other.seal(&subject, "pii").await.unwrap();
    assert_eq!(*kms.encrypts.lock().unwrap(), 2, "a fresh DEK after the shred");
}
