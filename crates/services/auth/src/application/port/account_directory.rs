use async_trait::async_trait;

use crate::domain::value_object::{AccountId, IdpSubject, Permission};
use crate::error::AuthError;

/// Whether an account may currently establish or keep a session.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AccountActivation {
    Active,
    /// The holder deactivated the account themselves. A login (the holder
    /// proving their credential again) resumes it; a refresh does not.
    Deactivated,
    /// Suspended / deleted / pending — `reason` carries the SoR's status.
    Inactive { reason: String },
}

impl AccountActivation {
    pub fn is_active(&self) -> bool {
        matches!(self, Self::Active)
    }
}

/// A point-in-time view of an account from the `account` service (the SoR).
#[derive(Debug, Clone)]
pub struct AccountSnapshot {
    pub activation: AccountActivation,
    /// Normalized RBAC grants — `account` is authoritative for these, so they are
    /// re-read on every login and refresh (a role change takes effect at the next
    /// token mint, not at the next full sign-in).
    pub permissions: Vec<Permission>,
}

/// Outbound port to the `account` service (gRPC adapter in Phase 4).
///
/// Auth reads identity here and writes one thing only: resuming an account its
/// holder deactivated, when they sign back in. Provisioning of the account
/// record on first federated login is the `account` service's idempotent
/// responsibility — auth only asks for the resulting internal id.
#[async_trait]
pub trait AccountDirectory: Send + Sync + 'static {
    /// Resolves the internal account id for an IdP subject, provisioning the
    /// account record on first sight (idempotent in the `account` service).
    async fn resolve_or_provision(&self, subject: &IdpSubject) -> Result<AccountId, AuthError>;

    /// Fetches the account's activation state and current permissions. Fails with
    /// [`AuthError::AccountDirectoryUnavailable`] if the SoR is unreachable.
    async fn lookup(&self, account_id: &AccountId) -> Result<AccountSnapshot, AuthError>;

    /// Returns a self-deactivated account to Active (idempotent if it already
    /// is). Fails with [`AuthError::AccountNotActive`] if the account is in any
    /// other state by now — e.g. suspended in the meantime.
    async fn resume_deactivated(&self, account_id: &AccountId) -> Result<(), AuthError>;
}
