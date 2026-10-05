use async_trait::async_trait;

use crate::domain::value_object::{AccountId, AgeBracket, IdpSubject, Permission};
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
    /// From the account's date of birth, today; `None` when none is on file.
    pub age_bracket: Option<AgeBracket>,
    /// Two-step sign-in is on (#649): a sign-in needs a second factor.
    pub mfa_enrolled: bool,
}

/// An account's addresses, as `account` holds them.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ContactDetails {
    pub email: Option<String>,
    pub phone: Option<String>,
}

/// The consent given at sign-up.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SignUpConsent {
    pub policy_version:  String,
    pub data_processing: bool,
    pub marketing:       bool,
    pub analytics:       bool,
}

/// An account to create at sign-up.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NewAccount {
    pub subject:        IdpSubject,
    /// `None` for a phone-only account.
    pub email:          Option<String>,
    /// The IdP (or a code) vouches for the address: the account is active at once.
    pub email_verified: bool,
    /// E.164, for an account signed up with its phone number.
    pub phone:          Option<String>,
    /// A code proved the number: the account is active at once.
    pub phone_verified: bool,
    /// ISO 8601; the `account` service enforces the minimum age.
    pub date_of_birth:  String,
    /// ISO 3166-1 alpha-2 (the home country), if known.
    pub country:        Option<String>,
    pub consent:        SignUpConsent,
}

/// The account holding an email address.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EmailHolder {
    pub account_id:  AccountId,
    /// Its `identity_id` (`issuer#subject`).
    pub identity_id: String,
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

    /// Creates the account for a sign-up and returns its id: idempotent for the
    /// same subject (a retried sign-up finishes the steps a failed one left).
    /// Fails with [`AuthError::AgeBelowMinimum`] under the minimum age (nothing
    /// is created) or [`AuthError::EmailAlreadyRegistered`].
    async fn provision(&self, account: &NewAccount) -> Result<AccountId, AuthError>;

    /// The account holding `email`, if any.
    async fn find_by_email(&self, email: &str) -> Result<Option<EmailHolder>, AuthError>;

    /// The account holding `phone` (E.164), if any.
    async fn find_by_phone(&self, phone: &str) -> Result<Option<EmailHolder>, AuthError>;

    /// The account's email and phone now (#651: the old address is told of a
    /// change).
    async fn contact(&self, account_id: &AccountId) -> Result<ContactDetails, AuthError>;

    /// Replaces the account's email (`Email`) or phone (`Sms`) with
    /// `destination`, which its holder just proved. Another account's address:
    /// [`AuthError::EmailAlreadyRegistered`] / [`AuthError::PhoneAlreadyRegistered`].
    async fn change_contact(
        &self,
        account_id: &AccountId,
        channel: super::VerificationChannel,
        destination: &str,
    ) -> Result<(), AuthError>;

    /// The account's two-step material (#649), for checking a code.
    async fn mfa_secret(&self, account_id: &AccountId) -> Result<super::MfaSecret, AuthError>;

    /// Spends the backup code hashed `code_hash`: `true` when one matched
    /// (now spent), `false` when none did (wrong, spent, or MFA off).
    async fn consume_recovery_code(&self, account_id: &AccountId, code_hash: &str) -> Result<bool, AuthError>;

    /// Turns two-step sign-in on with `sealed_seed` and the backup codes'
    /// hashes. [`AuthError::MfaAlreadyEnabled`] when it already is.
    async fn enroll_mfa(&self, account_id: &AccountId, sealed_seed: &[u8], code_hashes: &[String]) -> Result<(), AuthError>;

    /// Turns two-step sign-in off. [`AuthError::MfaNotEnabled`] when it is.
    async fn revoke_mfa(&self, account_id: &AccountId) -> Result<(), AuthError>;

    /// Replaces the backup codes. [`AuthError::MfaNotEnabled`] when it is off.
    async fn replace_recovery_codes(&self, account_id: &AccountId, code_hashes: &[String]) -> Result<(), AuthError>;
}
