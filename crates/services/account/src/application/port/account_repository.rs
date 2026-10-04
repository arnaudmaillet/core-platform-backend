use async_trait::async_trait;

use crate::domain::aggregate::Account;
use crate::domain::value_object::{AccountId, AccountStatus, EmailAddress, IdentityId, PhoneNumber};
use crate::error::AccountError;

/// Persistence port for the Account aggregate.
///
/// This trait is the only contract the application layer holds against the
/// storage layer. All concrete implementations live in
/// `infrastructure::persistence` and are injected at the composition root.
///
/// # Optimistic locking
///
/// `save` must enforce the aggregate's `version` counter: it should issue an
/// `UPDATE ... WHERE version = $old_version` and return
/// [`AccountError::OptimisticLockConflict`] when zero rows are affected.
#[async_trait]
pub trait AccountRepository: Send + Sync + 'static {
    /// Upserts the account aggregate state.
    ///
    /// New accounts (version == 0) are inserted; existing accounts are updated
    /// with optimistic-lock protection on the version column.
    async fn save(&self, account: &Account) -> Result<(), AccountError>;

    /// Returns the account with `id`, or `None` if it does not exist.
    async fn find_by_id(&self, id: &AccountId) -> Result<Option<Account>, AccountError>;

    /// Looks up an account by its IdP subject claim (`identity_id`).
    ///
    /// Used during the authentication flow to resolve an authenticated
    /// principal to its internal account record.
    async fn find_by_identity_id(
        &self,
        identity_id: &IdentityId,
    ) -> Result<Option<Account>, AccountError>;

    /// Looks up the account holding `email` (stored lower-cased).
    async fn find_by_email(&self, email: &EmailAddress) -> Result<Option<Account>, AccountError>;

    /// Returns a paginated slice of accounts with `status`.
    async fn list_by_status(
        &self,
        status: &AccountStatus,
        limit: i64,
        offset: i64,
    ) -> Result<Vec<Account>, AccountError>;

    /// Looks up the account holding `phone` (E.164).
    async fn find_by_phone(&self, phone: &PhoneNumber) -> Result<Option<Account>, AccountError>;

    /// Returns `true` if an account with the given email already exists.
    async fn exists_by_email(&self, email: &EmailAddress) -> Result<bool, AccountError>;

    /// Returns `true` if an account with the given identity ID already exists.
    async fn exists_by_identity_id(
        &self,
        identity_id: &IdentityId,
    ) -> Result<bool, AccountError>;

    /// Counts accounts currently in `status`.
    async fn count_by_status(&self, status: &AccountStatus) -> Result<i64, AccountError>;

    /// Accounts whose requested erasure is due (`deletion_scheduled_at <= now`)
    /// and not yet anonymized, oldest first — the GDPR janitor's batch.
    async fn list_due_for_anonymization(
        &self,
        now: chrono::DateTime<chrono::Utc>,
        limit: i64,
    ) -> Result<Vec<AccountId>, AccountError>;
}
