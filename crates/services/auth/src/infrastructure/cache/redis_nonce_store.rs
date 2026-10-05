//! Server-issued sign-in nonces in Redis: `auth:{fnonce:<sha256 hex>}` with the
//! nonce's TTL. Redeeming is a `DEL` — exactly one caller sees it removed.

use async_trait::async_trait;
use chrono::Duration;
use fred::interfaces::KeysInterface;
use fred::types::{Expiration, SetOptions};
use redis_storage::{RedisClient, RedisStorageError};

use crate::application::port::FederatedNonceStore;
use crate::error::AuthError;

fn key(namespace: &str, nonce_hash: &str) -> String {
    format!("auth:{{{namespace}:{nonce_hash}}}")
}

fn cache_err(e: fred::error::Error) -> AuthError {
    AuthError::Cache(RedisStorageError::from(e))
}

#[derive(Clone)]
pub struct RedisNonceStore {
    client: RedisClient,
    namespace: &'static str,
}

impl RedisNonceStore {
    /// Sign-in nonces (`auth:{fnonce:<hash>}`).
    pub fn new(client: RedisClient) -> Self {
        Self { client, namespace: "fnonce" }
    }

    /// Single-use values of another kind (e.g. App Attest challenges:
    /// `auth:{attest:<hash>}`).
    pub fn with_namespace(client: RedisClient, namespace: &'static str) -> Self {
        Self { client, namespace }
    }
}

#[async_trait]
impl FederatedNonceStore for RedisNonceStore {
    async fn issue(&self, nonce_hash: &str, ttl: Duration) -> Result<(), AuthError> {
        let _: Option<String> = self
            .client
            .set(key(self.namespace, nonce_hash), "1", Some(Expiration::EX(ttl.num_seconds().max(1))), Some(SetOptions::NX), false)
            .await
            .map_err(cache_err)?;
        Ok(())
    }

    async fn consume(&self, nonce_hash: &str) -> Result<bool, AuthError> {
        let removed: i64 = self.client.del(key(self.namespace, nonce_hash)).await.map_err(cache_err)?;
        Ok(removed == 1)
    }
}
