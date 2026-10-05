use async_trait::async_trait;

use crate::domain::value_object::AccountId;
use crate::error::AuthError;

/// What an erasure deleted.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct ErasedAccount {
    /// The account's sessions and, before sign-up, its guest's.
    pub sessions: u64,
    /// Identity links (an email / phone code identity's subject is the address).
    pub links:    u64,
    /// Guest records that became this account.
    pub guests:   u64,
}

/// Hard-deletes everything auth holds about an account (Postgres). Idempotent:
/// erasing an erased account deletes nothing.
#[async_trait]
pub trait AccountEraser: Send + Sync + 'static {
    async fn erase(&self, account_id: &AccountId) -> Result<ErasedAccount, AuthError>;
}
