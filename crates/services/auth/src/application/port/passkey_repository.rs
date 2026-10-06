use async_trait::async_trait;
use chrono::{DateTime, Utc};

use crate::domain::value_object::AccountId;
use crate::error::AuthError;

/// A registered passkey (#808), as stored.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StoredPasskey {
    pub credential_id:   Vec<u8>,
    /// SEC1 uncompressed P-256 point.
    pub public_key:      Vec<u8>,
    pub sign_count:      u32,
    /// The holder's name for it (e.g. "iPhone").
    pub name:            String,
    pub aaguid:          uuid::Uuid,
    pub backup_eligible: bool,
    pub backed_up:       bool,
    pub created_at:      DateTime<Utc>,
    pub last_used_at:    Option<DateTime<Utc>>,
}

/// An account's passkeys (Postgres, on the account's shard).
#[async_trait]
pub trait PasskeyRepository: Send + Sync + 'static {
    /// Oldest first.
    async fn list(&self, account_id: &AccountId) -> Result<Vec<StoredPasskey>, AuthError>;

    /// Adds a passkey unless the account already holds `max`
    /// ([`AuthError::PasskeyLimitReached`]) or this credential
    /// ([`AuthError::PasskeyAlreadyRegistered`]). Atomic with the count.
    async fn add(&self, account_id: &AccountId, passkey: &StoredPasskey, max: usize) -> Result<(), AuthError>;

    /// Removes one; `false` when the account has no such passkey.
    async fn remove(&self, account_id: &AccountId, credential_id: &[u8]) -> Result<bool, AuthError>;
}
