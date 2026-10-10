//! The production [`CheckpointAnchor`] (issue #483): a Merkle checkpoint root,
//! **signed in KMS's trust domain** and anchored to an **independent witness**.
//!
//! Why this exists: the v1 [`PgCheckpointAnchor`](super::PgCheckpointAnchor) stores
//! the anchor pointer in the *same* Postgres as the ledger, unsigned. A DB operator
//! who tampers a ledger row can rewrite that pointer too, and `verify_global` won't
//! catch them. Here the authority is an external copy the operator does not control:
//!
//! * **Sign** — the checkpoint's canonical bytes are signed by a [`KmsSigner`]
//!   (asymmetric KMS key, or the local HMAC fallback for dev), under a principal
//!   distinct from the ledger DB role.
//! * **Anchor** — the signed checkpoint is published to a [`Witness`]: a
//!   cross-account WORM bucket (S3 Object Lock, compliance mode). The Postgres
//!   pointer may remain as a *convenience index*, but the witness copy is the
//!   authority that `latest_anchored` reads back.
//! * **Verify** — `latest_anchored` validates the signature before returning the
//!   root; a forged witness entry (bad signature) is reported as
//!   `CheckpointVerificationFailed`. The verifier then reconciles the live heads
//!   against that root, so tampering both the ledger row *and* the Postgres pointer
//!   is still caught (the witness still holds the original signed root).

use std::sync::Arc;

use async_trait::async_trait;
use reqwest::header::CONTENT_TYPE;
use rusty_s3::actions::ListObjectsV2;
use rusty_s3::{Bucket, Credentials, S3Action, UrlStyle};
use serde::{Deserialize, Serialize};
use url::Url;

use crate::application::port::CheckpointAnchor;
use crate::domain::MerkleCheckpoint;
use crate::domain::value_object::CanonicalWriter;
use crate::error::AuditError;
use crate::infrastructure::kms::KmsSigner;
use crate::infrastructure::object_lock_archive::ObjectLockConfig;
use crate::infrastructure::pg_anchor::PgCheckpointAnchor;

/// A checkpoint plus the signature over its canonical bytes — the unit published to,
/// and read back from, the external witness.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SignedCheckpoint {
    pub checkpoint: MerkleCheckpoint,
    /// Base64 of the raw signature bytes (KMS or HMAC).
    pub signature_b64: String,
    /// The signing key id recorded at anchor time — an informational/audit
    /// breadcrumb only. Verification pins to the anchor's *configured* signing key,
    /// never this field (which the witness operator could otherwise control).
    pub key_id: String,
    /// Signing algorithm tag, e.g. `ECDSA_SHA_256` or `HMAC_SHA_256` (dev).
    pub algorithm: String,
}

/// The canonical bytes signed over a checkpoint. Covers the whole checkpoint
/// (root + head count + timestamp), not just the root, so altering any field in a
/// witness entry invalidates the signature.
///
/// Built with the domain's [`CanonicalWriter`] — the same length-prefixed,
/// fixed-order encoding the hash chain and the Merkle root use — *not* `serde_json`.
/// A JSON message would tie the signature to incidental library formatting
/// (field order, `DateTime` rendering, whitespace): a `serde_json`/`chrono` bump
/// could change the bytes across the store→read→re-serialize round trip and fail
/// verification of every previously-anchored checkpoint — a fleet-wide false tamper
/// alarm. The encoding is also infallible, so there is no error to swallow.
pub fn canonical_bytes(checkpoint: &MerkleCheckpoint) -> Vec<u8> {
    let mut w = CanonicalWriter::new();
    w.str(checkpoint.root().as_str())
        .u64(checkpoint.head_count())
        .i64(checkpoint.created_at().timestamp_millis());
    w.as_bytes().to_vec()
}

/// The independent witness the signed checkpoint is anchored to. The authority lives
/// here, in a trust domain the ledger DB operator does not control.
#[async_trait]
pub trait Witness: Send + Sync + 'static {
    /// Publish a signed checkpoint as an immutable witness entry.
    async fn publish(&self, signed: &SignedCheckpoint) -> Result<(), AuditError>;
    /// The most recently anchored signed checkpoint, if any.
    async fn latest(&self) -> Result<Option<SignedCheckpoint>, AuditError>;
}

/// The cross-account WORM-bucket witness (S3/MinIO Object Lock, compliance mode).
/// Each checkpoint is one immutable object keyed by an inverted timestamp (see
/// [`witness_key`]); "latest" is the lexicographically *smallest* key under the
/// prefix, so it is always on the first listing page.
pub struct ObjectLockWitness {
    bucket: Bucket,
    credentials: Credentials,
    http: reqwest::Client,
    presign_ttl: std::time::Duration,
}

const WITNESS_PREFIX: &str = "checkpoints/";

/// The witness object key for a checkpoint. The timestamp is stored **inverted**
/// (`u64::MAX - millis`, zero-padded to 20 digits) so the newest checkpoint sorts
/// *first* under the prefix. "Latest" is then the lexicographically *smallest* key,
/// always retrievable from the first page of a `ListObjectsV2` — S3 caps a page at
/// 1000 keys in ascending order, so a forward timestamp would bury the newest beyond
/// page 1 once >1000 checkpoints accumulate in the never-pruned WORM bucket, silently
/// returning a stale root.
fn witness_key(created_at_millis: i64) -> String {
    let inverted = u64::MAX - (created_at_millis.max(0) as u64);
    format!("{WITNESS_PREFIX}{inverted:020}.json")
}

impl ObjectLockWitness {
    pub fn new(config: ObjectLockConfig) -> Result<Self, AuditError> {
        let endpoint = Url::parse(&config.endpoint).map_err(|_| AuditError::AnchorWitnessUnavailable)?;
        let bucket = Bucket::new(endpoint, UrlStyle::Path, config.bucket, config.region)
            .map_err(|_| AuditError::AnchorWitnessUnavailable)?;
        let credentials = Credentials::new(config.access_key, config.secret_key);
        let http = reqwest::Client::builder()
            .timeout(config.request_timeout)
            .build()
            .map_err(|_| AuditError::AnchorWitnessUnavailable)?;
        Ok(Self {
            bucket,
            credentials,
            http,
            presign_ttl: config.presign_ttl,
        })
    }

    /// Verify the witness bucket is reachable, creating it only when absent.
    ///
    /// PROBE FIRST (HeadBucket — `s3:ListBucket` is granted), CreateBucket only
    /// on 404 (local/test). The witness keys deliberately have NO
    /// s3:CreateBucket against provisioned AWS, so the old create-first probe
    /// 403'd and fail-closed the service — found live on the staging bring-up.
    pub async fn ensure_bucket(&self) -> Result<(), AuditError> {
        let head = self.bucket.head_bucket(Some(&self.credentials)).sign(self.presign_ttl);
        let resp = self
            .http
            .head(head)
            .send()
            .await
            .map_err(|_| AuditError::AnchorWitnessUnavailable)?;
        if resp.status().is_success() {
            return Ok(());
        }
        if resp.status() != reqwest::StatusCode::NOT_FOUND {
            return Err(AuditError::AnchorWitnessUnavailable);
        }
        let url = self.bucket.create_bucket(&self.credentials).sign(self.presign_ttl);
        let resp = self
            .http
            .put(url)
            .send()
            .await
            .map_err(|_| AuditError::AnchorWitnessUnavailable)?;
        let status = resp.status();
        if status.is_success() || status == reqwest::StatusCode::CONFLICT {
            Ok(())
        } else {
            Err(AuditError::AnchorWitnessUnavailable)
        }
    }
}

#[async_trait]
impl Witness for ObjectLockWitness {
    async fn publish(&self, signed: &SignedCheckpoint) -> Result<(), AuditError> {
        // One immutable object per checkpoint, keyed so the newest sorts first.
        let key = witness_key(signed.checkpoint.created_at().timestamp_millis());
        let body = serde_json::to_vec(signed).map_err(|_| AuditError::AnchorWitnessUnavailable)?;
        let url: Url = self
            .bucket
            .put_object(Some(&self.credentials), &key)
            .sign(self.presign_ttl);
        let resp = self
            .http
            .put(url)
            .header(CONTENT_TYPE, "application/json")
            .body(body)
            .send()
            .await
            .map_err(|_| AuditError::AnchorWitnessUnavailable)?;
        if resp.status().is_success() {
            Ok(())
        } else {
            Err(AuditError::AnchorWitnessUnavailable)
        }
    }

    async fn latest(&self) -> Result<Option<SignedCheckpoint>, AuditError> {
        let mut list: ListObjectsV2 = self.bucket.list_objects_v2(Some(&self.credentials));
        list.with_prefix(WITNESS_PREFIX);
        // Keys are inverted timestamps (see `witness_key`): the newest is the
        // smallest key, so it is the first entry of the first ascending page —
        // one key suffices and the result is correct no matter how many checkpoints
        // have accumulated.
        list.with_max_keys(1);
        let url = list.sign(self.presign_ttl);
        let resp = self
            .http
            .get(url)
            .send()
            .await
            .map_err(|_| AuditError::AnchorWitnessUnavailable)?;
        if !resp.status().is_success() {
            return Err(AuditError::AnchorWitnessUnavailable);
        }
        let body = resp.text().await.map_err(|_| AuditError::AnchorWitnessUnavailable)?;
        let parsed =
            ListObjectsV2::parse_response(&body).map_err(|_| AuditError::AnchorWitnessUnavailable)?;

        // The smallest key under the prefix is the newest checkpoint.
        let Some(latest_key) = parsed.contents.into_iter().map(|c| c.key).min() else {
            return Ok(None);
        };

        let get_url = self
            .bucket
            .get_object(Some(&self.credentials), &latest_key)
            .sign(self.presign_ttl);
        let obj = self
            .http
            .get(get_url)
            .send()
            .await
            .map_err(|_| AuditError::AnchorWitnessUnavailable)?;
        if !obj.status().is_success() {
            return Err(AuditError::AnchorWitnessUnavailable);
        }
        let bytes = obj.bytes().await.map_err(|_| AuditError::AnchorWitnessUnavailable)?;
        let signed: SignedCheckpoint =
            serde_json::from_slice(&bytes).map_err(|_| AuditError::AnchorWitnessUnavailable)?;
        Ok(Some(signed))
    }
}

/// The production [`CheckpointAnchor`]: sign the root in KMS's domain, anchor it to
/// the witness, and (optionally) keep a Postgres convenience pointer. Reads come
/// from the witness — the authority a DB operator cannot rewrite.
pub struct WitnessCheckpointAnchor {
    signer: Arc<dyn KmsSigner>,
    signing_key_id: String,
    signing_algorithm: String,
    /// The algorithms a witness entry may have been signed with (#485): the
    /// current one, plus those it replaced. An entry recorded under another is
    /// a divergence.
    accepted_algorithms: Vec<String>,
    witness: Arc<dyn Witness>,
    /// Optional convenience index; never the authority. A failure here is logged,
    /// not fatal — the witness copy is what matters.
    pg_index: Option<PgCheckpointAnchor>,
}

impl WitnessCheckpointAnchor {
    pub fn new(
        signer: Arc<dyn KmsSigner>,
        signing_key_id: String,
        signing_algorithm: String,
        witness: Arc<dyn Witness>,
        pg_index: Option<PgCheckpointAnchor>,
    ) -> Self {
        Self {
            signer,
            accepted_algorithms: vec![signing_algorithm.clone()],
            signing_key_id,
            signing_algorithm,
            witness,
            pg_index,
        }
    }

    /// Also accepts entries signed with these algorithms (a rotation, #485):
    /// signing keeps the configured one.
    pub fn accepting(mut self, algorithms: impl IntoIterator<Item = String>) -> Self {
        for algorithm in algorithms {
            if !self.accepted_algorithms.contains(&algorithm) {
                self.accepted_algorithms.push(algorithm);
            }
        }
        self
    }
}

#[async_trait]
impl CheckpointAnchor for WitnessCheckpointAnchor {
    async fn anchor(&self, checkpoint: &MerkleCheckpoint) -> Result<(), AuditError> {
        let message = canonical_bytes(checkpoint);
        let signature = self.signer.sign(&self.signing_key_id, &message).await?;
        let signed = SignedCheckpoint {
            checkpoint: checkpoint.clone(),
            signature_b64: base64::Engine::encode(
                &base64::engine::general_purpose::STANDARD,
                &signature,
            ),
            key_id: self.signing_key_id.clone(),
            algorithm: self.signing_algorithm.clone(),
        };

        self.witness.publish(&signed).await?;

        // Best-effort convenience pointer; the witness is the authority.
        if let Some(pg) = &self.pg_index
            && let Err(error) = pg.anchor(checkpoint).await
        {
            tracing::warn!(%error, "checkpoint witness anchored, but the Postgres convenience pointer failed");
        }
        Ok(())
    }

    async fn latest_anchored(&self) -> Result<Option<MerkleCheckpoint>, AuditError> {
        let Some(signed) = self.witness.latest().await? else {
            return Ok(None);
        };
        let signature = base64::Engine::decode(
            &base64::engine::general_purpose::STANDARD,
            &signed.signature_b64,
        )
        .map_err(|_| AuditError::CheckpointVerificationFailed)?;
        let message = canonical_bytes(&signed.checkpoint);

        // Verify against the anchor's *own configured* signing key, NOT the key id
        // carried in the (operator-controlled) witness entry. Otherwise an operator
        // who can rewrite the witness could re-sign a forged root with a key of their
        // choosing and set `key_id` to match — KMS would happily verify it. Pinning
        // to `self.signing_key_id` means a forged entry must be signed by the genuine
        // key, which the operator cannot access. (`signed.key_id` is retained only as
        // an informational/audit breadcrumb.)
        //
        // A witness entry whose signature does not validate is operator-level
        // tampering, not an availability fault — surface it as divergence.
        //
        // The algorithm is the one the entry records (#485: rotating the signing
        // algorithm must not fail what was signed before), but only one this
        // anchor accepts — an operator cannot pick a weaker one, and the key
        // stays pinned whatever the entry says.
        if !self.accepted_algorithms.contains(&signed.algorithm) {
            tracing::error!(algorithm = %signed.algorithm, "witness entry signed with an algorithm this anchor does not accept");
            return Err(AuditError::CheckpointVerificationFailed);
        }
        if !self
            .signer
            .verify(&self.signing_key_id, &signed.algorithm, &message, &signature)
            .await?
        {
            return Err(AuditError::CheckpointVerificationFailed);
        }
        Ok(Some(signed.checkpoint))
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Mutex;

    use chrono::{TimeZone, Utc};

    use super::*;
    use crate::domain::value_object::{PartitionKey, RecordHash};
    use crate::infrastructure::kms::LocalCheckpointSigner;

    /// An in-memory witness whose stored entry the test can *tamper* — standing in
    /// for an operator who somehow rewrote the external copy, to prove the signature
    /// check catches it.
    #[derive(Default)]
    struct InMemoryWitness {
        latest: Mutex<Option<SignedCheckpoint>>,
    }

    impl InMemoryWitness {
        fn tamper_root(&self) {
            let mut guard = self.latest.lock().unwrap();
            if let Some(signed) = guard.as_mut() {
                signed.checkpoint =
                    MerkleCheckpoint::over(&[head("forged", "deadbeef")], signed.checkpoint.created_at());
            }
        }
    }

    #[async_trait]
    impl Witness for InMemoryWitness {
        async fn publish(&self, signed: &SignedCheckpoint) -> Result<(), AuditError> {
            *self.latest.lock().unwrap() = Some(signed.clone());
            Ok(())
        }
        async fn latest(&self) -> Result<Option<SignedCheckpoint>, AuditError> {
            Ok(self.latest.lock().unwrap().clone())
        }
    }

    fn head(p: &str, h: &str) -> (PartitionKey, RecordHash) {
        (PartitionKey::new(p).unwrap(), RecordHash::digest(h.as_bytes()))
    }

    fn checkpoint() -> MerkleCheckpoint {
        let now = Utc.with_ymd_and_hms(2026, 6, 27, 9, 0, 0).unwrap();
        MerkleCheckpoint::over(&[head("p1", "x"), head("p2", "y")], now)
    }

    fn anchor(witness: Arc<dyn Witness>) -> WitnessCheckpointAnchor {
        WitnessCheckpointAnchor::new(
            Arc::new(LocalCheckpointSigner::new([5u8; 32])),
            "local-signing-key".to_owned(),
            "HMAC_SHA_256".to_owned(),
            witness,
            None,
        )
    }

    #[tokio::test]
    async fn signed_checkpoint_round_trips_through_the_witness() {
        let witness = Arc::new(InMemoryWitness::default());
        let a = anchor(Arc::clone(&witness) as Arc<dyn Witness>);
        let cp = checkpoint();
        a.anchor(&cp).await.unwrap();

        let read_back = a.latest_anchored().await.unwrap().unwrap();
        assert_eq!(read_back.root(), cp.root());
    }

    #[tokio::test]
    async fn nothing_anchored_yet_is_none() {
        let a = anchor(Arc::new(InMemoryWitness::default()));
        assert!(a.latest_anchored().await.unwrap().is_none());
    }

    /// The core #483 guarantee: a witness entry whose checkpoint was rewritten (its
    /// signature no longer matches) is reported as `CheckpointVerificationFailed`,
    /// not silently trusted.
    #[tokio::test]
    async fn a_tampered_witness_entry_fails_signature_verification() {
        let witness = Arc::new(InMemoryWitness::default());
        let a = anchor(Arc::clone(&witness) as Arc<dyn Witness>);
        a.anchor(&checkpoint()).await.unwrap();

        witness.tamper_root();

        let err = a.latest_anchored().await.unwrap_err();
        assert!(matches!(err, AuditError::CheckpointVerificationFailed));
    }

    /// A signer that models KMS faithfully: a signature is valid only under the key
    /// that produced it. `sign`/`verify` use the per-key sentinel `sig-for-<key_id>`,
    /// so a signature minted under one key never verifies under another.
    struct KeyBoundSigner;

    fn sig_for(key_id: &str) -> Vec<u8> {
        format!("sig-for-{key_id}").into_bytes()
    }

    #[async_trait]
    impl KmsSigner for KeyBoundSigner {
        async fn sign(&self, key_id: &str, _message: &[u8]) -> Result<Vec<u8>, AuditError> {
            Ok(sig_for(key_id))
        }
        async fn verify(
            &self,
            key_id: &str,
            _algorithm: &str,
            _message: &[u8],
            signature: &[u8],
        ) -> Result<bool, AuditError> {
            Ok(signature == sig_for(key_id))
        }
    }

    /// Issue #483 hardening: verification must pin to the anchor's *configured*
    /// signing key, never the `key_id` carried in the witness entry. An operator who
    /// rewrites the witness and re-signs with a key they control (setting `key_id` to
    /// match their signature) must still be rejected.
    #[tokio::test]
    async fn a_forged_entry_signed_under_an_attacker_key_id_is_rejected() {
        let witness = Arc::new(InMemoryWitness::default());
        // The anchor is configured to trust only the genuine key.
        let a = WitnessCheckpointAnchor::new(
            Arc::new(KeyBoundSigner),
            "genuine-key".to_owned(),
            "ECDSA_SHA_256".to_owned(),
            Arc::clone(&witness) as Arc<dyn Witness>,
            None,
        );

        // The operator publishes a forged checkpoint signed with their OWN key and
        // stamps `key_id` to match — a signature that genuinely verifies under
        // "attacker-key" but not under "genuine-key".
        witness
            .publish(&SignedCheckpoint {
                checkpoint: checkpoint(),
                signature_b64: base64::Engine::encode(
                    &base64::engine::general_purpose::STANDARD,
                    sig_for("attacker-key"),
                ),
                key_id: "attacker-key".to_owned(),
                algorithm: "ECDSA_SHA_256".to_owned(),
            })
            .await
            .unwrap();

        // Verifying against `signed.key_id` ("attacker-key") would ACCEPT it; pinning
        // to the configured "genuine-key" rejects it.
        let err = a.latest_anchored().await.unwrap_err();
        assert!(matches!(err, AuditError::CheckpointVerificationFailed));
    }

    /// A signer that is *unreachable* — `verify` returns an availability error rather
    /// than a true/false answer (e.g. a KMS outage).
    struct UnavailableSigner;

    #[async_trait]
    impl KmsSigner for UnavailableSigner {
        async fn sign(&self, _key_id: &str, _message: &[u8]) -> Result<Vec<u8>, AuditError> {
            Err(AuditError::AnchorWitnessUnavailable)
        }
        async fn verify(
            &self,
            _key_id: &str,
            _algorithm: &str,
            _message: &[u8],
            _signature: &[u8],
        ) -> Result<bool, AuditError> {
            Err(AuditError::AnchorWitnessUnavailable)
        }
    }

    /// Issue #483 hardening: a signer *outage* during verification must propagate as
    /// `AnchorWitnessUnavailable` ("couldn't check", retryable), never collapse into
    /// `CheckpointVerificationFailed` (which the verifier escalates as tampering).
    #[tokio::test]
    async fn a_signer_outage_does_not_masquerade_as_tampering() {
        let witness = Arc::new(InMemoryWitness::default());
        // Seed a well-formed entry so we reach the verify step.
        anchor(Arc::clone(&witness) as Arc<dyn Witness>)
            .anchor(&checkpoint())
            .await
            .unwrap();

        // The entry's own algorithm (HMAC_SHA_256), so the check reaches the signer.
        let a = WitnessCheckpointAnchor::new(
            Arc::new(UnavailableSigner),
            "genuine-key".to_owned(),
            "HMAC_SHA_256".to_owned(),
            Arc::clone(&witness) as Arc<dyn Witness>,
            None,
        );

        let err = a.latest_anchored().await.unwrap_err();
        assert!(
            matches!(err, AuditError::AnchorWitnessUnavailable),
            "a verify outage must be 'couldn't check', not a divergence/tamper signal"
        );
    }

    /// A signer that records the algorithm each verification is asked for.
    #[derive(Default)]
    struct RecordingSigner {
        verified_with: Mutex<Vec<String>>,
    }

    #[async_trait]
    impl KmsSigner for RecordingSigner {
        async fn sign(&self, key_id: &str, _message: &[u8]) -> Result<Vec<u8>, AuditError> {
            Ok(sig_for(key_id))
        }
        async fn verify(
            &self,
            key_id: &str,
            algorithm: &str,
            _message: &[u8],
            signature: &[u8],
        ) -> Result<bool, AuditError> {
            self.verified_with.lock().unwrap().push(algorithm.to_owned());
            Ok(signature == sig_for(key_id))
        }
    }

    /// #485: an entry is verified with the algorithm it records — a rotated
    /// signing algorithm does not fail what was signed before — but only one
    /// the anchor accepts.
    #[tokio::test]
    async fn a_rotated_signing_algorithm_verifies_entries_signed_before_only_when_accepted() {
        let witness = Arc::new(InMemoryWitness::default());
        let signer = Arc::new(RecordingSigner::default());
        let anchor_with = |algorithm: &str| {
            WitnessCheckpointAnchor::new(
                Arc::clone(&signer) as Arc<dyn KmsSigner>,
                "kms-key".to_owned(),
                algorithm.to_owned(),
                Arc::clone(&witness) as Arc<dyn Witness>,
                None,
            )
        };
        anchor_with("RSASSA_PSS_SHA_256").anchor(&checkpoint()).await.unwrap();

        // Rotated, the old algorithm not listed: the old entry is refused.
        let err = anchor_with("ECDSA_SHA_256").latest_anchored().await.unwrap_err();
        assert!(matches!(err, AuditError::CheckpointVerificationFailed));

        // Listed: it verifies, with the algorithm it was signed with.
        let rotated = anchor_with("ECDSA_SHA_256").accepting(["RSASSA_PSS_SHA_256".to_owned()]);
        assert!(rotated.latest_anchored().await.unwrap().is_some());
        assert_eq!(signer.verified_with.lock().unwrap().as_slice(), ["RSASSA_PSS_SHA_256"]);
    }
}
