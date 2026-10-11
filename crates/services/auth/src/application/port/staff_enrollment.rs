use async_trait::async_trait;
use chrono::{DateTime, Utc};
use uuid::Uuid;

use crate::domain::value_object::AccountId;
use crate::error::AuthError;

/// A staff passkey enrollment grant (#880), as kept: the grant itself is a
/// secret handed to its holder once; only its SHA-256 is stored.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StaffGrant {
    pub grant_id:   Uuid,
    pub account_id: AccountId,
    /// `None`: the one-off bootstrap.
    pub issued_by:  Option<AccountId>,
    pub issued_at:  DateTime<Utc>,
    pub expires_at: DateTime<Utc>,
}

/// Staff enrollment grants (Postgres, on the account's shard).
#[async_trait]
pub trait StaffEnrollmentGrants: Send + Sync + 'static {
    async fn issue(&self, grant: &StaffGrant, grant_hash: &str) -> Result<(), AuthError>;

    /// Uses the grant up: `Some(grant_id)` when it is `account_id`'s, not
    /// expired at `now` and not used before; `None` otherwise (single use,
    /// under races too).
    async fn redeem(&self, account_id: &AccountId, grant_hash: &str, now: DateTime<Utc>) -> Result<Option<Uuid>, AuthError>;
}
