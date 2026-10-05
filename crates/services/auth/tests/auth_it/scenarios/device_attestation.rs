//! App Attest plumbing against a live Redis: challenges are single use (their
//! own namespace, apart from sign-in nonces) and guest sessions are counted per
//! attested device per day.

use std::sync::Arc;

use auth::application::command::{AttestMode, AttestationProof, AttestedDevice, DeviceAttestationVerifier, GuestAttestation};
use auth::application::command::DeviceQuota;
use auth::application::port::FederatedNonceStore;
use auth::error::AuthError;
use auth::infrastructure::cache::{RedisDeviceQuota, RedisNonceStore};

use crate::auth_it::harness::Harness;

/// Accepts any attestation (the chain checks have their own unit tests).
struct Genuine;

impl DeviceAttestationVerifier for Genuine {
    fn verify(&self, proof: &AttestationProof, _: chrono::DateTime<chrono::Utc>) -> Result<AttestedDevice, String> {
        Ok(AttestedDevice { key_id: proof.key_id.clone(), environment: "production".into() })
    }
}

#[tokio::test]
async fn attest_challenges_are_single_use_and_devices_have_a_daily_quota() {
    let h = Harness::start().await;
    let key = format!("key-{}", uuid::Uuid::now_v7());
    let attestation = GuestAttestation::new(
        AttestMode::Enforce,
        Arc::new(Genuine),
        Arc::new(RedisNonceStore::with_namespace(h.redis.clone(), "attest")),
        Arc::new(RedisDeviceQuota::new(h.redis.clone())),
        2,
    );
    let proof = |challenge: String| {
        Some(AttestationProof { key_id: key.clone(), attestation: "att".into(), challenge })
    };
    let now = chrono::Utc::now();

    let c1 = attestation.start().await.unwrap().challenge;
    assert!(attestation.check(proof(c1.clone()), now).await.unwrap().is_some());
    assert!(matches!(attestation.check(proof(c1), now).await, Err(AuthError::DeviceAttestationInvalid { .. })), "single use");

    let c2 = attestation.start().await.unwrap().challenge;
    assert!(attestation.check(proof(c2), now).await.unwrap().is_some());
    let c3 = attestation.start().await.unwrap().challenge;
    assert!(matches!(attestation.check(proof(c3), now).await, Err(AuthError::DeviceGuestQuotaExceeded)), "2 a day");

    // Separate namespaces: an attest challenge is no sign-in nonce.
    let attest = RedisNonceStore::with_namespace(h.redis.clone(), "attest");
    attest.issue("same-hash", chrono::Duration::seconds(60)).await.unwrap();
    assert!(!RedisNonceStore::new(h.redis.clone()).consume("same-hash").await.unwrap());
    assert!(attest.consume("same-hash").await.unwrap());

    // The quota is per device.
    let other = RedisDeviceQuota::new(h.redis.clone());
    assert!(other.admit(&format!("other-{key}"), 2).await.unwrap());
}
