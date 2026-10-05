//! Server-issued sign-in nonces in Redis: `auth:{fnonce:<sha256 hex>}` with the
//! nonce's TTL. Redeeming is a `DEL` — exactly one caller sees it removed.

use async_trait::async_trait;
use chrono::Duration;
use fred::interfaces::KeysInterface;
use fred::types::{Expiration, SetOptions};
use redis_storage::{RedisClient, RedisStorageError};

use crate::application::port::FederatedNonceStore;
use crate::error::AuthError;

fn key(nonce_hash: &str) -> String {
    format!("auth:{{fnonce:{nonce_hash}}}")
}

fn cache_err(e: fred::error::Error) -> AuthError {
    AuthError::Cache(RedisStorageError::from(e))
}

#[derive(Clone)]
pub struct RedisNonceStore {
    client: RedisClient,
}

impl RedisNonceStore {
    pub fn new(client: RedisClient) -> Self {
        Self { client }
    }
}

#[async_trait]
impl FederatedNonceStore for RedisNonceStore {
    async fn issue(&self, nonce_hash: &str, ttl: Duration) -> Result<(), AuthError> {
        let _: Option<String> = self
            .client
            .set(key(nonce_hash), "1", Some(Expiration::EX(ttl.num_seconds().max(1))), Some(SetOptions::NX), false)
            .await
            .map_err(cache_err)?;
        Ok(())
    }

    async fn consume(&self, nonce_hash: &str) -> Result<bool, AuthError> {
        let removed: i64 = self.client.del(key(nonce_hash)).await.map_err(cache_err)?;
        Ok(removed == 1)
    }
}
