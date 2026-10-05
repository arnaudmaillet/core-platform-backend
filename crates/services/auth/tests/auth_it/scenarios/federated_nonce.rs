//! Server-issued sign-in nonces against a live Redis: issued once, redeemed
//! once, expiring with their TTL, and stored only as their hash.

use std::sync::Arc;

use auth::application::command::{FederatedNonces, FEDERATED_NONCE_TTL_SECS};
use auth::application::port::FederatedNonceStore;
use auth::error::AuthError;
use auth::infrastructure::cache::RedisNonceStore;

use crate::auth_it::harness::Harness;

#[tokio::test]
async fn a_nonce_is_redeemed_once_and_expires() {
    use fred::interfaces::KeysInterface;

    let h = Harness::start().await;
    let nonces = FederatedNonces::new(Arc::new(RedisNonceStore::new(h.redis.clone())), true);

    let started = nonces.start().await.unwrap();
    // Stored by hash: the raw nonce never appears in a key name.
    use sha2::Digest;
    let hash: String = sha2::Sha256::digest(started.nonce.as_bytes()).iter().map(|b| format!("{b:02x}")).collect();
    let stored: i64 = h.redis.exists(format!("auth:{{fnonce:{hash}}}")).await.unwrap();
    assert_eq!(stored, 1);
    let raw: i64 = h.redis.exists(format!("auth:{{fnonce:{}}}", started.nonce)).await.unwrap();
    assert_eq!(raw, 0);

    nonces.redeem(&started.nonce).await.unwrap();
    assert!(matches!(nonces.redeem(&started.nonce).await, Err(AuthError::IdTokenRejected { .. })), "single use");
    assert!(matches!(nonces.redeem("never-issued").await, Err(AuthError::IdTokenRejected { .. })));

    // The TTL is the store's: a one-second nonce is gone after it.
    let store = RedisNonceStore::new(h.redis.clone());
    store.issue("short-lived-hash", chrono::Duration::seconds(1)).await.unwrap();
    tokio::time::sleep(std::time::Duration::from_millis(2_100)).await;
    assert!(!store.consume("short-lived-hash").await.unwrap(), "expired");
    assert_eq!(started.expires_in_secs, FEDERATED_NONCE_TTL_SECS);
}
