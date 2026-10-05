use async_trait::async_trait;
use chrono::Duration;

use crate::error::AuthError;

/// Server-issued sign-in nonces (Redis), stored by their SHA-256 hex only.
#[async_trait]
pub trait FederatedNonceStore: Send + Sync + 'static {
    async fn issue(&self, nonce_hash: &str, ttl: Duration) -> Result<(), AuthError>;

    /// Removes the nonce; `true` if it was issued and not yet used or expired.
    async fn consume(&self, nonce_hash: &str) -> Result<bool, AuthError>;
}
