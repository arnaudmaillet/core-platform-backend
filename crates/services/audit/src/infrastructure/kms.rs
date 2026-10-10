//! KMS as a **separate trust domain** — the two operations the audit plane offloads
//! so that no raw key material ever lives in its own environment or database:
//!
//! * [`KmsCipher`] — wrap/unwrap the per-subject **DEK** (issue #482). The DEK is
//!   encrypted under a KMS key the ledger DB role cannot assume; audit can ask KMS
//!   to encrypt/decrypt but can never export the key. Crypto-shred stays "delete
//!   the wrapped-DEK row" — once gone, not even KMS can recover the DEK.
//! * [`KmsSigner`] — sign/verify the Merkle **checkpoint root** (issue #483) with a
//!   KMS asymmetric key in that same separate domain, so an operator who controls
//!   the ledger DB cannot forge a checkpoint the verifier will accept.
//!
//! [`AwsKms`] is the real adapter: header-authenticated SigV4 (see [`super::sigv4`])
//! JSON POSTs to the KMS `TrentService` API, over `reqwest`. It is endpoint-driven,
//! so the *same* adapter talks to AWS KMS in production and to LocalStack KMS in a
//! live test — no SDK, mirroring how the object store signs its own S3 requests.
//!
//! The local-dev fallbacks live elsewhere: the env-KEK [`LocalKek`](super::subject_cipher)
//! for wrapping and the HMAC [`LocalCheckpointSigner`] here for signing. Production
//! selects [`AwsKms`] by config at the composition root.

use std::time::Duration;

use async_trait::async_trait;
use base64::Engine as _;
use chrono::Utc;
use serde::Deserialize;
use serde_json::json;
use url::Url;

use crate::error::AuditError;
use crate::infrastructure::sigv4::{self, SigV4Credentials, SignedHeader};

/// Wrap/unwrap a DEK in KMS. The audit plane mints a random DEK, asks KMS to
/// `encrypt` it (the wrapped blob is stored in `subject_keys`), and asks KMS to
/// `decrypt` it back on first use. The raw KEK never leaves KMS.
#[async_trait]
pub trait KmsCipher: Send + Sync + 'static {
    /// Encrypt (wrap) a DEK under `key_id`. Returns the opaque KMS ciphertext blob.
    async fn encrypt(&self, key_id: &str, plaintext: &[u8]) -> Result<Vec<u8>, AuditError>;
    /// Decrypt (unwrap) a previously wrapped DEK. KMS recovers `key_id` from the
    /// ciphertext blob itself.
    async fn decrypt(&self, ciphertext: &[u8]) -> Result<Vec<u8>, AuditError>;
}

/// Sign/verify a checkpoint root in KMS's trust domain. The signing key is
/// asymmetric and held under a principal distinct from the ledger DB role.
#[async_trait]
pub trait KmsSigner: Send + Sync + 'static {
    /// Sign `message` (the canonical checkpoint bytes) under `key_id`.
    async fn sign(&self, key_id: &str, message: &[u8]) -> Result<Vec<u8>, AuditError>;
    /// Verify a signature over `message`, made under `key_id` with `algorithm`
    /// (the one recorded with the signature, #485: a rotated signing algorithm
    /// must not fail the checkpoints signed before). `Ok(false)` is a *valid
    /// answer* meaning the signature does not match — an operator-level tampering
    /// signal — not a transport fault (those are `Err`).
    async fn verify(
        &self,
        key_id: &str,
        algorithm: &str,
        message: &[u8],
        signature: &[u8],
    ) -> Result<bool, AuditError>;
}

/// Connection + key settings for the real KMS adapter, resolved from env at the
/// composition root. `dek_key_id` backs DEK wrap/unwrap (#482); `signing_key_id`
/// backs checkpoint signing (#483). Either may be unused if only one workstream is
/// enabled.
#[derive(Debug, Clone)]
pub struct KmsConfig {
    pub endpoint: String,
    pub region: String,
    pub access_key: String,
    pub secret_key: String,
    pub dek_key_id: String,
    pub signing_key_id: String,
    /// Asymmetric signing algorithm (KMS `SigningAlgorithm`); e.g.
    /// `ECDSA_SHA_256`.
    pub signing_algorithm: String,
    /// Algorithms checkpoints signed earlier may carry (a rotation, #485), from
    /// `AUDIT_KMS_ACCEPTED_SIGNING_ALGORITHMS`; `signing_algorithm` is always
    /// accepted.
    pub accepted_signing_algorithms: Vec<String>,
    pub request_timeout: Duration,
}

/// The real KMS adapter: SigV4-signed JSON POSTs to the KMS endpoint.
pub struct AwsKms {
    http: reqwest::Client,
    endpoint: Url,
    host: String,
    creds: SigV4Credentials,
    signing_algorithm: String,
}

impl AwsKms {
    pub fn new(config: &KmsConfig) -> Result<Self, AuditError> {
        let endpoint = Url::parse(&config.endpoint).map_err(|_| AuditError::KeyVaultUnavailable)?;
        // The signed `host` MUST equal the `Host` header the client transmits. hyper
        // sends `host:port` whenever the port is non-default for the scheme; `Url`
        // normalizes default ports away, so `port()` is exactly that condition. A
        // bare `host_str()` would drop the port and break SigV4 against any custom
        // endpoint (LocalStack, a MinIO/KMS proxy) while silently passing on AWS:443.
        let host_str = endpoint.host_str().ok_or(AuditError::KeyVaultUnavailable)?;
        let host = match endpoint.port() {
            Some(port) => format!("{host_str}:{port}"),
            None => host_str.to_owned(),
        };
        let http = reqwest::Client::builder()
            .timeout(config.request_timeout)
            .build()
            .map_err(|_| AuditError::KeyVaultUnavailable)?;
        Ok(Self {
            http,
            endpoint,
            host,
            creds: SigV4Credentials {
                access_key: config.access_key.clone(),
                secret_key: config.secret_key.clone(),
                region: config.region.clone(),
                service: "kms".to_owned(),
            },
            signing_algorithm: config.signing_algorithm.clone(),
        })
    }

    /// Issue one signed `TrentService.<target>` call and return the parsed JSON
    /// body on success. A non-2xx response is classified as [`KmsFault`] so the
    /// caller can tell an *invalid signature* (HTTP 400 `KMSInvalidSignatureException`
    /// — a valid "no") apart from an *availability* fault (everything else).
    async fn call(
        &self,
        target: &str,
        body: serde_json::Value,
    ) -> Result<serde_json::Value, KmsFault> {
        let payload = serde_json::to_vec(&body).map_err(|_| KmsFault::Unavailable)?;
        let amz_target = format!("TrentService.{target}");
        // The timestamp is both a signed header and the date in the string-to-sign;
        // derive it once from `now` so the two are identical.
        let now = Utc::now();
        let amz_date = now.format("%Y%m%dT%H%M%SZ").to_string();

        // Canonical headers MUST be lowercase and sorted by name. AWS requires every
        // `x-amz-*` header on the request (here `x-amz-date` and `x-amz-target`) to
        // be signed, alongside `host`.
        let headers = [
            SignedHeader {
                name: "content-type",
                value: "application/x-amz-json-1.1",
            },
            SignedHeader {
                name: "host",
                value: &self.host,
            },
            SignedHeader {
                name: "x-amz-date",
                value: &amz_date,
            },
            SignedHeader {
                name: "x-amz-target",
                value: &amz_target,
            },
        ];
        let signed = sigv4::sign(
            &self.creds,
            "POST",
            self.endpoint.path(),
            "",
            &headers,
            &payload,
            now,
        );

        let resp = self
            .http
            .post(self.endpoint.clone())
            .header("content-type", "application/x-amz-json-1.1")
            .header("x-amz-target", amz_target)
            .header("x-amz-date", signed.amz_date)
            .header("authorization", signed.authorization)
            .body(payload)
            .send()
            .await
            .map_err(|_| KmsFault::Unavailable)?;

        let status = resp.status();
        if status.is_success() {
            return resp.json().await.map_err(|_| KmsFault::Unavailable);
        }
        // Classify the error: a 400 whose `__type` is the invalid-signature
        // exception is a definite "signature does not match"; anything else
        // (throttle, 5xx, auth, transport) is an availability fault.
        let kind = resp.text().await.unwrap_or_default();
        if status == reqwest::StatusCode::BAD_REQUEST
            && kind.contains("KMSInvalidSignatureException")
        {
            Err(KmsFault::InvalidSignature)
        } else {
            Err(KmsFault::Unavailable)
        }
    }
}

/// How a KMS call failed: a definite invalid-signature answer vs. an availability
/// fault. Keeping these distinct is what stops a KMS outage from being reported as
/// operator-level tampering.
enum KmsFault {
    InvalidSignature,
    Unavailable,
}

fn b64(bytes: &[u8]) -> String {
    base64::engine::general_purpose::STANDARD.encode(bytes)
}

fn unb64(s: &str, on_err: fn() -> AuditError) -> Result<Vec<u8>, AuditError> {
    base64::engine::general_purpose::STANDARD
        .decode(s)
        .map_err(|_| on_err())
}

#[derive(Deserialize)]
struct CiphertextResp {
    #[serde(rename = "CiphertextBlob")]
    ciphertext_blob: String,
}

#[derive(Deserialize)]
struct PlaintextResp {
    #[serde(rename = "Plaintext")]
    plaintext: String,
}

#[derive(Deserialize)]
struct SignResp {
    #[serde(rename = "Signature")]
    signature: String,
}

#[derive(Deserialize)]
struct VerifyResp {
    #[serde(rename = "SignatureValid")]
    signature_valid: bool,
}

#[async_trait]
impl KmsCipher for AwsKms {
    async fn encrypt(&self, key_id: &str, plaintext: &[u8]) -> Result<Vec<u8>, AuditError> {
        let resp = self
            .call("Encrypt", json!({ "KeyId": key_id, "Plaintext": b64(plaintext) }))
            .await
            .map_err(|_| AuditError::KeyVaultUnavailable)?;
        let parsed: CiphertextResp =
            serde_json::from_value(resp).map_err(|_| AuditError::KeyVaultUnavailable)?;
        unb64(&parsed.ciphertext_blob, || AuditError::KeyVaultUnavailable)
    }

    async fn decrypt(&self, ciphertext: &[u8]) -> Result<Vec<u8>, AuditError> {
        let resp = self
            .call("Decrypt", json!({ "CiphertextBlob": b64(ciphertext) }))
            .await
            .map_err(|_| AuditError::KeyVaultUnavailable)?;
        let parsed: PlaintextResp =
            serde_json::from_value(resp).map_err(|_| AuditError::KeyVaultUnavailable)?;
        unb64(&parsed.plaintext, || AuditError::KeyVaultUnavailable)
    }
}

#[async_trait]
impl KmsSigner for AwsKms {
    async fn sign(&self, key_id: &str, message: &[u8]) -> Result<Vec<u8>, AuditError> {
        let resp = self
            .call(
                "Sign",
                json!({
                    "KeyId": key_id,
                    "Message": b64(message),
                    "MessageType": "RAW",
                    "SigningAlgorithm": self.signing_algorithm,
                }),
            )
            .await
            .map_err(|_| AuditError::AnchorWitnessUnavailable)?;
        let parsed: SignResp =
            serde_json::from_value(resp).map_err(|_| AuditError::AnchorWitnessUnavailable)?;
        unb64(&parsed.signature, || AuditError::AnchorWitnessUnavailable)
    }

    async fn verify(
        &self,
        key_id: &str,
        algorithm: &str,
        message: &[u8],
        signature: &[u8],
    ) -> Result<bool, AuditError> {
        let payload = json!({
            "KeyId": key_id,
            "Message": b64(message),
            "MessageType": "RAW",
            "Signature": b64(signature),
            "SigningAlgorithm": algorithm,
        });
        match self.call("Verify", payload).await {
            Ok(resp) => {
                let parsed: VerifyResp = serde_json::from_value(resp)
                    .map_err(|_| AuditError::AnchorWitnessUnavailable)?;
                Ok(parsed.signature_valid)
            }
            // A *definite* invalid-signature answer is "no", not a fault — only this
            // case becomes Ok(false). A transport/availability fault propagates as
            // Err, so "we couldn't check" is never misreported as "the signature is
            // bad" (which the verifier would escalate as operator-level tampering).
            Err(KmsFault::InvalidSignature) => Ok(false),
            Err(KmsFault::Unavailable) => Err(AuditError::AnchorWitnessUnavailable),
        }
    }
}

/// The local-dev / CI checkpoint signer: HMAC-SHA256 under a key from the audit
/// environment. Symmetric, so it is *not* operator-proof (the key sits beside the
/// service) — but it keeps the signed-checkpoint code path exercised end-to-end
/// without provisioning a real KMS asymmetric key. Production swaps in [`AwsKms`].
pub struct LocalCheckpointSigner {
    key: [u8; 32],
}

impl LocalCheckpointSigner {
    pub fn new(key: [u8; 32]) -> Self {
        Self { key }
    }
}

#[async_trait]
impl KmsSigner for LocalCheckpointSigner {
    async fn sign(&self, _key_id: &str, message: &[u8]) -> Result<Vec<u8>, AuditError> {
        Ok(sigv4::hmac_sha256(&self.key, message).to_vec())
    }

    async fn verify(
        &self,
        _key_id: &str,
        _algorithm: &str,
        message: &[u8],
        signature: &[u8],
    ) -> Result<bool, AuditError> {
        let expected = sigv4::hmac_sha256(&self.key, message);
        // Constant-time-ish compare (length + byte fold) — these are MACs, not
        // user input, but no reason to leak via early return.
        if signature.len() != expected.len() {
            return Ok(false);
        }
        let mut diff = 0u8;
        for (a, b) in signature.iter().zip(expected.iter()) {
            diff |= a ^ b;
        }
        Ok(diff == 0)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn local_signer_round_trips() {
        let signer = LocalCheckpointSigner::new([7u8; 32]);
        let sig = signer.sign("local", b"checkpoint-root").await.unwrap();
        assert!(signer.verify("local", "HMAC_SHA_256", b"checkpoint-root", &sig).await.unwrap());
    }

    #[tokio::test]
    async fn local_signer_rejects_a_tampered_message() {
        let signer = LocalCheckpointSigner::new([7u8; 32]);
        let sig = signer.sign("local", b"checkpoint-root").await.unwrap();
        // A forged root (operator rewrote the witness) fails verification.
        assert!(!signer.verify("local", "HMAC_SHA_256", b"TAMPERED-root", &sig).await.unwrap());
    }

    #[tokio::test]
    async fn local_signer_rejects_a_tampered_signature() {
        let signer = LocalCheckpointSigner::new([7u8; 32]);
        let mut sig = signer.sign("local", b"root").await.unwrap();
        sig[0] ^= 0xff;
        assert!(!signer.verify("local", "HMAC_SHA_256", b"root", &sig).await.unwrap());
    }

    #[tokio::test]
    async fn a_different_key_does_not_verify() {
        let signer = LocalCheckpointSigner::new([1u8; 32]);
        let other = LocalCheckpointSigner::new([2u8; 32]);
        let sig = signer.sign("local", b"root").await.unwrap();
        assert!(!other.verify("local", "HMAC_SHA_256", b"root", &sig).await.unwrap());
    }
}
