//! App Attest in front of `StartGuestSession` (guest mode B5b).
//!
//! A guest session needs no credential, so per-IP limits alone let anyone with
//! proxies mint sessions. With App Attest the app proves each install is a
//! genuine copy of **our** app on a real Apple device: it asks for a one-time
//! challenge (`StartDeviceAttestation`), attests a fresh Secure Enclave key for
//! it, and presents the attestation with `StartGuestSession`. Guest sessions are
//! then also counted per attested key — not per device: an app can rotate its
//! key (Apple throttles attestations per device), so this is a speed bump; a
//! once-per-device rule would need assertions on one key per install.
//!
//! Rolled out by `AUTH_APP_ATTEST_MODE`: `off` (nothing checked), `observe`
//! (checked and logged, never refused — to measure before enforcing), `enforce`
//! (a missing or invalid attestation is refused).

use std::sync::Arc;

use async_trait::async_trait;
use base64::Engine;
use chrono::{DateTime, Duration, Utc};
use rand::RngCore;
use sha2::{Digest, Sha256};

use crate::application::port::FederatedNonceStore;
use crate::error::AuthError;

/// How long a challenge waits for its attestation.
pub const ATTEST_CHALLENGE_TTL_SECS: i64 = 300;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AttestMode {
    Off,
    Observe,
    Enforce,
}

impl AttestMode {
    pub fn parse(s: &str) -> Option<Self> {
        match s.trim().to_ascii_lowercase().as_str() {
            "" | "off" => Some(Self::Off),
            "observe" => Some(Self::Observe),
            "enforce" => Some(Self::Enforce),
            _ => None,
        }
    }
}

/// What the app presents with `StartGuestSession`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AttestationProof {
    pub key_id:      String,
    /// base64 of the attestation object.
    pub attestation: String,
    pub challenge:   String,
}

/// A device whose key Apple attested for one of our apps.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AttestedDevice {
    pub key_id:      String,
    pub environment: String,
}

/// Verifies an App Attest attestation (the adapter checks the certificate
/// chain to Apple's root, the nonce, the app id and the environment).
pub trait DeviceAttestationVerifier: Send + Sync + 'static {
    /// `Err` carries why it was refused (logged, never returned to the client).
    fn verify(&self, proof: &AttestationProof, now: DateTime<Utc>) -> Result<AttestedDevice, String>;
}

/// Guest sessions per attested key per UTC day.
#[async_trait]
pub trait DeviceQuota: Send + Sync + 'static {
    /// Counts one guest session for `key_id`; `false` past `daily`.
    async fn admit(&self, key_id: &str, daily: u32) -> Result<bool, AuthError>;
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StartedDeviceAttestation {
    pub challenge:       String,
    pub expires_in_secs: i64,
}

fn challenge_hash(challenge: &str) -> String {
    Sha256::digest(challenge.trim().as_bytes()).iter().map(|b| format!("{b:02x}")).collect()
}

pub struct GuestAttestation {
    mode:       AttestMode,
    verifier:   Arc<dyn DeviceAttestationVerifier>,
    challenges: Arc<dyn FederatedNonceStore>,
    quota:      Arc<dyn DeviceQuota>,
    per_device_daily: u32,
}

impl GuestAttestation {
    pub fn new(
        mode: AttestMode,
        verifier: Arc<dyn DeviceAttestationVerifier>,
        challenges: Arc<dyn FederatedNonceStore>,
        quota: Arc<dyn DeviceQuota>,
        per_device_daily: u32,
    ) -> Self {
        Self { mode, verifier, challenges, quota, per_device_daily }
    }

    pub fn mode(&self) -> AttestMode {
        self.mode
    }

    /// A single-use challenge (32 random bytes, base64url), stored as its hash.
    pub async fn start(&self) -> Result<StartedDeviceAttestation, AuthError> {
        let mut bytes = [0u8; 32];
        rand::rng().fill_bytes(&mut bytes);
        let challenge = base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(bytes);
        self.challenges.issue(&challenge_hash(&challenge), Duration::seconds(ATTEST_CHALLENGE_TTL_SECS)).await?;
        Ok(StartedDeviceAttestation { challenge, expires_in_secs: ATTEST_CHALLENGE_TTL_SECS })
    }

    /// The attested device behind a guest session request, per the mode:
    /// `None` when off, or (observing) when missing or invalid.
    pub async fn check(
        &self,
        proof: Option<AttestationProof>,
        now: DateTime<Utc>,
    ) -> Result<Option<AttestedDevice>, AuthError> {
        if self.mode == AttestMode::Off {
            return Ok(None);
        }
        let Some(mut proof) = proof else {
            return self.refuse(AuthError::DeviceAttestationRequired, "no attestation");
        };
        // One spelling of the challenge for the lookup and the nonce check.
        proof.challenge = proof.challenge.trim().to_owned();
        // Single use: a consumed challenge can't vouch for a second session.
        if !self.challenges.consume(&challenge_hash(&proof.challenge)).await? {
            return self.refuse(
                AuthError::DeviceAttestationInvalid { reason: "unknown or used challenge".into() },
                "unknown or used challenge",
            );
        }
        let device = match self.verifier.verify(&proof, now) {
            Ok(device) => device,
            Err(reason) => {
                let error = AuthError::DeviceAttestationInvalid { reason: reason.clone() };
                return self.refuse(error, &reason);
            }
        };
        if !self.quota.admit(&device.key_id, self.per_device_daily).await? {
            return self.refuse(AuthError::DeviceGuestQuotaExceeded, "guest sessions per device exceeded");
        }
        Ok(Some(device))
    }

    fn refuse(&self, error: AuthError, why: &str) -> Result<Option<AttestedDevice>, AuthError> {
        match self.mode {
            AttestMode::Enforce => {
                tracing::info!(reason = why, "guest session refused: device attestation");
                Err(error)
            }
            _ => {
                tracing::warn!(reason = why, "device attestation would refuse this guest session (observe mode)");
                Ok(None)
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Mutex;

    use super::*;
    use crate::application::fakes::InMemoryNonceStore;

    /// Accepts the attestation "ok" for key "k".
    struct Verifier;

    impl DeviceAttestationVerifier for Verifier {
        fn verify(&self, proof: &AttestationProof, _: DateTime<Utc>) -> Result<AttestedDevice, String> {
            (proof.attestation == "ok")
                .then(|| AttestedDevice { key_id: proof.key_id.clone(), environment: "production".into() })
                .ok_or_else(|| "bad attestation".into())
        }
    }

    #[derive(Default)]
    struct Quota(Mutex<u32>);

    #[async_trait]
    impl DeviceQuota for Quota {
        async fn admit(&self, _: &str, daily: u32) -> Result<bool, AuthError> {
            let mut n = self.0.lock().unwrap();
            *n += 1;
            Ok(*n <= daily)
        }
    }

    fn attestation(mode: AttestMode) -> GuestAttestation {
        GuestAttestation::new(
            mode,
            Arc::new(Verifier),
            Arc::new(InMemoryNonceStore::default()),
            Arc::new(Quota::default()),
            2,
        )
    }

    fn proof(challenge: &str, attestation: &str) -> Option<AttestationProof> {
        Some(AttestationProof { key_id: "k".into(), attestation: attestation.into(), challenge: challenge.into() })
    }

    #[tokio::test]
    async fn enforced_a_genuine_device_passes_within_its_quota_once_per_challenge() {
        let a = attestation(AttestMode::Enforce);
        let now = Utc::now();
        let c1 = a.start().await.unwrap().challenge;
        assert_eq!(a.check(proof(&c1, "ok"), now).await.unwrap().unwrap().key_id, "k");
        // The challenge is spent.
        assert!(matches!(a.check(proof(&c1, "ok"), now).await, Err(AuthError::DeviceAttestationInvalid { .. })));
        // A server-unknown challenge, a bad attestation, none at all.
        assert!(matches!(a.check(proof("made-up", "ok"), now).await, Err(AuthError::DeviceAttestationInvalid { .. })));
        let c2 = a.start().await.unwrap().challenge;
        assert!(matches!(a.check(proof(&c2, "forged"), now).await, Err(AuthError::DeviceAttestationInvalid { .. })));
        assert!(matches!(a.check(None, now).await, Err(AuthError::DeviceAttestationRequired)));
        // The quota per device (2 a day).
        let c3 = a.start().await.unwrap().challenge;
        assert!(a.check(proof(&c3, "ok"), now).await.unwrap().is_some());
        let c4 = a.start().await.unwrap().challenge;
        assert!(matches!(a.check(proof(&c4, "ok"), now).await, Err(AuthError::DeviceGuestQuotaExceeded)));
    }

    #[tokio::test]
    async fn observed_nothing_is_refused_and_off_nothing_is_checked() {
        let observe = attestation(AttestMode::Observe);
        let now = Utc::now();
        assert_eq!(observe.check(None, now).await.unwrap(), None);
        assert_eq!(observe.check(proof("made-up", "ok"), now).await.unwrap(), None);
        let c = observe.start().await.unwrap().challenge;
        assert!(observe.check(proof(&c, "ok"), now).await.unwrap().is_some(), "a valid one is still recognised");

        let off = attestation(AttestMode::Off);
        assert_eq!(off.check(proof("made-up", "forged"), now).await.unwrap(), None);
    }

    #[test]
    fn modes_parse() {
        assert_eq!(AttestMode::parse(""), Some(AttestMode::Off));
        assert_eq!(AttestMode::parse("Observe"), Some(AttestMode::Observe));
        assert_eq!(AttestMode::parse("enforce"), Some(AttestMode::Enforce));
        assert_eq!(AttestMode::parse("on"), None);
    }
}
