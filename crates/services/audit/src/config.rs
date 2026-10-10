//! Environment-sourced configuration, resolved once at boot and threaded into the
//! composition root ([`crate::app`]). Each backend's connection config comes from
//! its own `from_env`; audit-specific knobs are read here.

use std::time::Duration;

use base64::Engine as _;
use postgres_storage::PostgresConfig;
use sha2::{Digest, Sha256};
use transport::kafka::config::KafkaClientConfig;

use crate::infrastructure::{KmsConfig, ObjectLockConfig};

const DEFAULT_RECORD_TIMEOUT_MS: u64 = 2_000;
const DEFAULT_CHECKPOINT_INTERVAL_S: u64 = 300;
const DEFAULT_PRESIGN_TTL_S: u64 = 900;
const DEFAULT_OBJECT_TIMEOUT_MS: u64 = 5_000;
const DEFAULT_KMS_TIMEOUT_MS: u64 = 5_000;
const DEFAULT_KMS_SIGNING_ALGORITHM: &str = "ECDSA_SHA_256";

/// Fully-resolved audit configuration shared by both binaries (the server uses the
/// ledger/archive/vault/anchor + the sync deadline; the worker additionally uses
/// Kafka + the checkpoint cadence).
pub struct AuditConfig {
    pub postgres: PostgresConfig,
    pub kafka: KafkaClientConfig,
    pub object_lock: ObjectLockConfig,
    /// The synchronous `RecordPrivileged` durable-commit deadline. On elapse the
    /// lane fails CLOSED (`AUD-4004`) and the caller must deny the action.
    pub record_timeout: Duration,
    /// How often the worker snapshots the partition heads into an anchored
    /// Merkle checkpoint.
    pub checkpoint_interval: Duration,
    /// The local-dev key-encryption key (32 bytes) that wraps the per-subject DEKs
    /// when [`kms`](Self::kms) is **not** configured. Lives in the environment — the
    /// fallback custody. Production sets [`kms`](Self::kms) instead, so the raw KEK
    /// never enters audit's env or memory (issue #482).
    pub kek: [u8; 32],
    /// When set, KMS holds the trust-domain operations: DEK wrap/unwrap (#482) and
    /// checkpoint signing (#483). `None` selects the local fallbacks (env KEK +
    /// HMAC signer). Enabled by `AUDIT_KMS_ENDPOINT`.
    pub kms: Option<KmsConfig>,
    /// When set, the signed Merkle checkpoint is anchored to this independent WORM
    /// witness (issue #483); `None` keeps the Postgres-only anchor (local/dev).
    /// Enabled by `AUDIT_WITNESS_ENDPOINT`.
    pub witness: Option<ObjectLockConfig>,
    /// `AUDIT_WITNESS_ALLOW_SHARED_PRINCIPAL=true` lets the witness share the
    /// archive's endpoint and credentials or bucket — an acknowledged deferral
    /// (local/dev, or a single-account deployment until the cross-account witness
    /// exists, #485); otherwise the process refuses to start that way.
    pub witness_allow_shared: bool,
    /// The local HMAC checkpoint-signing key, used when [`kms`](Self::kms) is not
    /// set. Keeps the signed-checkpoint path exercised in dev without a real KMS
    /// asymmetric key (not operator-proof — see the README).
    pub checkpoint_signing_key: [u8; 32],
    /// Caller verification for the privileged gRPC surface. `Some` when
    /// `AUDIT_JWKS_URL` is set (the ES256 edge-token JWKS, i.e. `auth`'s);
    /// `None` selects the fail-closed deny-all gate — the surface never opens
    /// by omission.
    pub authz: Option<auth_context::AuthContextConfig>,
}

impl AuditConfig {
    pub fn from_env() -> Self {
        Self {
            postgres: PostgresConfig::from_env(),
            kafka: KafkaClientConfig::from_env(),
            object_lock: object_lock_from_env(),
            record_timeout: Duration::from_millis(env_u64(
                "AUDIT_RECORD_TIMEOUT_MS",
                DEFAULT_RECORD_TIMEOUT_MS,
            )),
            checkpoint_interval: Duration::from_secs(env_u64(
                "AUDIT_CHECKPOINT_INTERVAL_S",
                DEFAULT_CHECKPOINT_INTERVAL_S,
            )),
            kek: kek_from_env(),
            kms: kms_from_env(),
            witness: witness_from_env(),
            witness_allow_shared: std::env::var("AUDIT_WITNESS_ALLOW_SHARED_PRINCIPAL").is_ok_and(|v| v == "true"),
            checkpoint_signing_key: signing_key_from_env(),
            authz: authz_from_env(),
        }
    }
}

impl AuditConfig {
    /// The witness must be a trust domain of its own (#485, the premise of #483):
    /// see [`witness_independence`].
    pub fn witness_independence(&self) -> Result<(), String> {
        witness_independence(self.witness.as_ref(), &self.object_lock, self.witness_allow_shared)
    }
}

/// A witness on the archive's endpoint with the archive's credentials (or its
/// bucket) can be rewritten by whoever holds those, defeating the anchor (#485).
/// Refused unless `allow_shared` (an acknowledged deferral).
pub fn witness_independence(
    witness: Option<&ObjectLockConfig>,
    archive: &ObjectLockConfig,
    allow_shared: bool,
) -> Result<(), String> {
    let Some(witness) = witness else { return Ok(()) };
    let same_endpoint = witness.endpoint.trim_end_matches('/') == archive.endpoint.trim_end_matches('/');
    let shared = same_endpoint && (witness.access_key == archive.access_key || witness.bucket == archive.bucket);
    if shared && !allow_shared {
        return Err(format!(
            "the checkpoint witness shares the WORM archive's endpoint ({}) and its credentials or bucket: \
             give AUDIT_WITNESS_* a separate account (or acknowledge the deferral with AUDIT_WITNESS_ALLOW_SHARED_PRINCIPAL=true)",
            witness.endpoint
        ));
    }
    Ok(())
}

/// Resolve caller verification — `Some` only when `AUDIT_JWKS_URL` is set.
/// Issuer/audience validation are individually opt-in (recommended on: set
/// `AUDIT_TOKEN_ISSUER` / `AUDIT_TOKEN_AUDIENCE` to the values `auth` mints).
fn authz_from_env() -> Option<auth_context::AuthContextConfig> {
    let jwks_url = std::env::var("AUDIT_JWKS_URL").ok()?;
    let mut cfg = auth_context::AuthContextConfig {
        jwks_url,
        ..auth_context::AuthContextConfig::default()
    };
    cfg.expected_issuer = std::env::var("AUDIT_TOKEN_ISSUER").ok();
    cfg.expected_audience = std::env::var("AUDIT_TOKEN_AUDIENCE").ok();
    cfg.fetch_timeout = Duration::from_millis(env_u64("AUDIT_JWKS_TIMEOUT_MS", 10_000));
    Some(cfg)
}

/// Resolve the KMS config — `Some` only when `AUDIT_KMS_ENDPOINT` is set (production
/// hands KEK custody + checkpoint signing to KMS); otherwise `None` (local
/// fallbacks). Credentials default to the standard AWS env vars.
fn kms_from_env() -> Option<KmsConfig> {
    let endpoint = std::env::var("AUDIT_KMS_ENDPOINT").ok()?;
    Some(KmsConfig {
        endpoint,
        region: env_str("AUDIT_KMS_REGION", "us-east-1"),
        access_key: env_first(&["AUDIT_KMS_ACCESS_KEY", "AWS_ACCESS_KEY_ID"], "test"),
        secret_key: env_first(&["AUDIT_KMS_SECRET_KEY", "AWS_SECRET_ACCESS_KEY"], "test"),
        dek_key_id: env_str("AUDIT_KMS_DEK_KEY_ID", "alias/audit-dek"),
        signing_key_id: env_str("AUDIT_KMS_SIGNING_KEY_ID", "alias/audit-checkpoint"),
        signing_algorithm: env_str("AUDIT_KMS_SIGNING_ALGORITHM", DEFAULT_KMS_SIGNING_ALGORITHM),
        accepted_signing_algorithms: env_str("AUDIT_KMS_ACCEPTED_SIGNING_ALGORITHMS", "")
            .split(',')
            .map(str::trim)
            .filter(|a| !a.is_empty())
            .map(str::to_owned)
            .collect(),
        request_timeout: Duration::from_millis(env_u64("AUDIT_KMS_TIMEOUT_MS", DEFAULT_KMS_TIMEOUT_MS)),
    })
}

/// Resolve the external-witness bucket — `Some` only when `AUDIT_WITNESS_ENDPOINT`
/// is set. A *separate* bucket/account from the WORM archive, so its trust domain
/// is independent of the ledger's.
fn witness_from_env() -> Option<ObjectLockConfig> {
    let endpoint = std::env::var("AUDIT_WITNESS_ENDPOINT").ok()?;
    Some(ObjectLockConfig {
        endpoint,
        region: env_str("AUDIT_WITNESS_REGION", "us-east-1"),
        bucket: env_str("AUDIT_WITNESS_BUCKET", "audit-witness"),
        access_key: env_str("AUDIT_WITNESS_ACCESS_KEY", "minioadmin"),
        secret_key: env_str("AUDIT_WITNESS_SECRET_KEY", "minioadmin"),
        presign_ttl: Duration::from_secs(env_u64("AUDIT_WITNESS_PRESIGN_TTL_S", DEFAULT_PRESIGN_TTL_S)),
        request_timeout: Duration::from_millis(env_u64("AUDIT_WITNESS_TIMEOUT_MS", DEFAULT_OBJECT_TIMEOUT_MS)),
    })
}

/// The local HMAC checkpoint-signing key from `AUDIT_CHECKPOINT_SIGNING_KEY_BASE64`
/// (base64 of 32 bytes); a deterministic dev key otherwise.
fn signing_key_from_env() -> [u8; 32] {
    if let Ok(encoded) = std::env::var("AUDIT_CHECKPOINT_SIGNING_KEY_BASE64")
        && let Ok(bytes) = base64::engine::general_purpose::STANDARD.decode(encoded.trim())
        && let Ok(key) = <[u8; 32]>::try_from(bytes.as_slice())
    {
        return key;
    }
    Sha256::digest(b"audit-dev-checkpoint-signing-key-do-not-use-in-prod").into()
}

/// Resolve the 32-byte KEK from `AUDIT_KEK_BASE64` (base64 of exactly 32 bytes).
/// Absent or malformed → a deterministic **dev** key derived from a fixed phrase
/// (sha256), so local/test runs work; this MUST be overridden in production.
fn kek_from_env() -> [u8; 32] {
    if let Ok(encoded) = std::env::var("AUDIT_KEK_BASE64")
        && let Ok(bytes) = base64::engine::general_purpose::STANDARD.decode(encoded.trim())
        && let Ok(key) = <[u8; 32]>::try_from(bytes.as_slice())
    {
        return key;
    }
    Sha256::digest(b"audit-dev-kek-do-not-use-in-prod").into()
}

fn object_lock_from_env() -> ObjectLockConfig {
    ObjectLockConfig {
        endpoint: env_str("AUDIT_OBJECT_STORE_ENDPOINT", "http://localhost:9000"),
        region: env_str("AUDIT_OBJECT_STORE_REGION", "us-east-1"),
        bucket: env_str("AUDIT_OBJECT_STORE_BUCKET", "audit-archive"),
        access_key: env_str("AUDIT_OBJECT_STORE_ACCESS_KEY", "minioadmin"),
        secret_key: env_str("AUDIT_OBJECT_STORE_SECRET_KEY", "minioadmin"),
        presign_ttl: Duration::from_secs(env_u64("AUDIT_OBJECT_PRESIGN_TTL_S", DEFAULT_PRESIGN_TTL_S)),
        request_timeout: Duration::from_millis(env_u64(
            "AUDIT_OBJECT_TIMEOUT_MS",
            DEFAULT_OBJECT_TIMEOUT_MS,
        )),
    }
}

fn env_u64(key: &str, default: u64) -> u64 {
    std::env::var(key).ok().and_then(|v| v.parse().ok()).unwrap_or(default)
}

fn env_str(key: &str, default: &str) -> String {
    std::env::var(key).unwrap_or_else(|_| default.to_owned())
}

/// First set var among `keys`, else `default`.
fn env_first(keys: &[&str], default: &str) -> String {
    keys.iter()
        .find_map(|k| std::env::var(k).ok())
        .unwrap_or_else(|| default.to_owned())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn store(endpoint: &str, bucket: &str, access_key: &str) -> ObjectLockConfig {
        ObjectLockConfig { endpoint: endpoint.to_owned(), bucket: bucket.to_owned(), access_key: access_key.to_owned(), ..object_lock_from_env() }
    }

    /// #485: a witness on the archive's endpoint with its credentials (the old
    /// defaults) or its bucket is refused; a separate account, or the local
    /// escape hatch, is not.
    #[test]
    fn the_witness_must_be_its_own_trust_domain() {
        let archive = store("http://localhost:9000", "audit-archive", "minioadmin");
        let shared = store("http://localhost:9000/", "audit-witness", "minioadmin");
        assert!(witness_independence(Some(&shared), &archive, false).is_err());
        assert!(witness_independence(Some(&shared), &archive, true).is_ok());
        let same_bucket = store("http://localhost:9000", "audit-archive", "other-key");
        assert!(witness_independence(Some(&same_bucket), &archive, false).is_err());
        let separate = store("https://witness.example", "audit-witness", "minioadmin");
        assert!(witness_independence(Some(&separate), &archive, false).is_ok());
        assert!(witness_independence(None, &archive, false).is_ok());
    }
}
