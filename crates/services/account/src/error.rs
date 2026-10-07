use error::{AppError, Severity};
use http::StatusCode;
use thiserror::Error;

/// Canonical domain and application error type for the account microservice.
///
/// ## Code catalogue
///
/// | Code     | Variant                    | HTTP | Severity | Retryable |
/// |----------|----------------------------|------|----------|-----------|
/// | ACC-1001 | AccountNotFound            | 404  | Low      | No        |
/// | ACC-1002 | IdentityAlreadyRegistered  | 409  | Low      | No        |
/// | ACC-1003 | EmailAlreadyRegistered     | 409  | Low      | No        |
/// | ACC-1004 | PhoneAlreadyRegistered     | 409  | Low      | No        |
/// | ACC-2001 | AccountNotActive           | 422  | Medium   | No        |
/// | ACC-2002 | InvalidStatusTransition    | 422  | Medium   | No        |
/// | ACC-2003 | EmailAlreadyVerified       | 409  | Low      | No        |
/// | ACC-2004 | AgeBelowMinimum            | 422  | Low      | No        |
/// | ACC-2005 | DateOfBirthAlreadySet      | 409  | Low      | No        |
/// | ACC-4001 | ConcurrentModification     | 409  | High     | **Yes**   |
/// | ACC-5001 | MfaAlreadyEnrolled         | 409  | Low      | No        |
/// | ACC-5002 | MfaNotEnrolled             | 422  | Low      | No        |
/// | ACC-5003 | RecoveryCodeInvalid        | 422  | Low      | No        |
/// | ACC-6001 | InvalidKycTransition       | 422  | Medium   | No        |
/// | ACC-7001 | GdprDeletionAlreadyReq.    | 409  | Low      | No        |
/// | ACC-7002 | AccountAlreadyAnonymized   | 422  | Low      | No        |
/// | ACC-7003 | NoPendingGdprDeletion      | 422  | Low      | No        |
/// | ACC-7004 | GdprGracePeriodOver        | 422  | Low      | No        |
/// | ACC-7005 | DataExportUnavailable      | 503  | Medium   | **Yes**   |
/// | ACC-7006 | DirectoryUnavailable       | 503  | Medium   | **Yes**   |
/// | ACC-7007 | ContactLookupQuotaExceeded | 429  | Low      | No        |
/// | ACC-8001 | RoleAlreadyAssigned        | 409  | Low      | No        |
/// | ACC-8002 | RoleNotAssigned            | 422  | Low      | No        |
/// | ACC-9001 | DomainViolation            | 422  | Medium   | No        |
/// | ACC-9002 | InvalidAccountId           | 422  | Low      | No        |
/// | ACC-9003 | InvalidIdentityId          | 422  | Low      | No        |
/// | ACC-9004 | InvalidEmail               | 422  | Low      | No        |
/// | ACC-9005 | InvalidPhone               | 422  | Low      | No        |
/// | ACC-9006 | InvalidCountryCode         | 422  | Low      | No        |
/// | ACC-9007 | InvalidAccountStatus       | 422  | Low      | No        |
/// | ACC-9008 | InvalidKycStatus           | 422  | Low      | No        |
/// | ACC-9009 | InvalidAccountRole         | 422  | Low      | No        |
/// | DB-*     | Storage (delegated)        | var  | var      | var       |
/// | VAL-*    | Validation (delegated)     | 422  | Low      | No        |
#[derive(Debug, Error)]
#[non_exhaustive]
pub enum AccountError {
    // ── Infrastructure delegates ───────────────────────────────────────────────

    #[error(transparent)]
    Storage(#[from] postgres_storage::StorageError),

    #[error(transparent)]
    Validation(#[from] validation::ValidationError),

    // ── Identity & uniqueness (ACC-1xxx) ──────────────────────────────────────

    #[error("account not found: {id}")]
    AccountNotFound { id: String },

    #[error("identity '{identity_id}' is already registered to an existing account")]
    IdentityAlreadyRegistered { identity_id: String },

    #[error("email '{email}' is already registered to an existing account")]
    EmailAlreadyRegistered { email: String },

    #[error("this phone number is already registered to an existing account")]
    PhoneAlreadyRegistered,

    // ── Lifecycle state machine (ACC-2xxx) ────────────────────────────────────

    #[error("operation requires an active account; current status: '{current}'")]
    AccountNotActive { current: String },

    #[error("status transition from '{from}' to '{to}' is not permitted")]
    InvalidStatusTransition { from: String, to: String },

    #[error("the email address for this account is already verified")]
    EmailAlreadyVerified,

    // ── Optimistic concurrency (ACC-4xxx) ─────────────────────────────────────

    // ── Family supervision (ACC-3xxx, #670) ───────────────────────────────────
    /// No live invite with this code (unknown, used or expired).
    #[error("this invite is not valid")]
    SupervisionInviteInvalid,

    /// The account's age does not fit this side: a supervisor is a known
    /// adult, a teen 13–17.
    #[error("this account cannot be the {role} of a supervision")]
    SupervisionRoleNotAllowed { role: String },

    /// The teen already has the most supervisors.
    #[error("this teen already has the most supervisors")]
    SupervisorLimitReached,

    /// These two accounts are already paired.
    #[error("these accounts are already paired")]
    AlreadySupervised,

    /// No such supervision for this account.
    #[error("supervision not found")]
    SupervisionNotFound,

    /// An account cannot supervise itself.
    #[error("an account cannot supervise itself")]
    SelfSupervision,

    #[error("concurrent modification detected; reload the account and retry")]
    ConcurrentModification,

    // ── MFA (ACC-5xxx) ────────────────────────────────────────────────────────

    #[error("MFA is already enrolled for this account")]
    MfaAlreadyEnrolled,

    #[error("MFA is not enrolled for this account")]
    MfaNotEnrolled,

    /// No unused backup code has that hash (wrong, or already spent).
    #[error("no unused backup code matches")]
    RecoveryCodeInvalid,

    // ── KYC (ACC-6xxx) ────────────────────────────────────────────────────────

    #[error("KYC status transition from '{from}' to '{to}' is not permitted")]
    InvalidKycTransition { from: String, to: String },

    /// Under the minimum age (13; 16 where the country of residence says so).
    #[error("the account holder is under the minimum age of {minimum}")]
    AgeBelowMinimum { minimum: u32 },

    /// A date of birth is on file already: only support may change it.
    #[error("a date of birth is already on file")]
    DateOfBirthAlreadySet,

    // ── GDPR / compliance (ACC-7xxx) ──────────────────────────────────────────

    #[error("a GDPR deletion request has already been submitted for this account")]
    GdprDeletionAlreadyRequested,

    #[error("this account has already been anonymized")]
    AccountAlreadyAnonymized,

    #[error("no deletion is pending for this account")]
    NoPendingGdprDeletion,

    /// The erasure's grace period has ended: it can no longer be cancelled
    /// (the janitor anonymizes the account).
    #[error("the deletion grace period is over")]
    GdprGracePeriodOver,

    /// A GDPR data export could not be built or stored now (a source service
    /// or the export store is unreachable); the export pass retries.
    #[error("the data export is unavailable right now: {reason}")]
    DataExportUnavailable { reason: String },

    /// Contact matching (#661) could not read the matched accounts' profiles
    /// or the blocks between them (profile / social-graph unreachable).
    #[error("contact matching is unavailable right now: {reason}")]
    DirectoryUnavailable { reason: String },

    /// The account looked up its daily budget of contact hashes (#661).
    #[error("the daily contact lookup budget of {limit} hashes is spent")]
    ContactLookupQuotaExceeded { limit: i64 },

    // ── Roles (ACC-8xxx) ──────────────────────────────────────────────────────

    #[error("role '{0}' is already assigned to this account")]
    RoleAlreadyAssigned(String),

    #[error("role '{0}' is not assigned to this account")]
    RoleNotAssigned(String),

    // ── Domain invariants & parse errors (ACC-9xxx) ───────────────────────────

    #[error("domain invariant violated on '{field}': {message}")]
    DomainViolation { field: String, message: String },

    #[error("invalid account ID: '{0}'")]
    InvalidAccountId(String),

    #[error("invalid identity ID: {0}")]
    InvalidIdentityId(String),

    #[error("invalid email address: {0}")]
    InvalidEmail(String),

    #[error("invalid phone number: {0}")]
    InvalidPhone(String),

    #[error("invalid country code: {0}")]
    InvalidCountryCode(String),

    #[error("unknown account status: '{0}'")]
    InvalidAccountStatus(String),

    #[error("unknown KYC status: '{0}'")]
    InvalidKycStatus(String),

    #[error("unknown account role: '{0}'")]
    InvalidAccountRole(String),

    #[error("failed to publish account event: {0}")]
    EventPublishFailed(String),
}

impl AppError for AccountError {
    fn error_code(&self) -> &'static str {
        match self {
            AccountError::Storage(e)    => e.error_code(),
            AccountError::Validation(e) => e.error_code(),

            AccountError::AccountNotFound { .. }           => "ACC-1001",
            AccountError::IdentityAlreadyRegistered { .. } => "ACC-1002",
            AccountError::EmailAlreadyRegistered { .. }    => "ACC-1003",
            AccountError::PhoneAlreadyRegistered           => "ACC-1004",

            AccountError::AccountNotActive { .. }          => "ACC-2001",
            AccountError::InvalidStatusTransition { .. }   => "ACC-2002",
            AccountError::EmailAlreadyVerified             => "ACC-2003",
            AccountError::AgeBelowMinimum { .. }           => "ACC-2004",
            AccountError::DateOfBirthAlreadySet            => "ACC-2005",

            AccountError::SupervisionInviteInvalid         => "ACC-3001",
            AccountError::SupervisionRoleNotAllowed { .. } => "ACC-3002",
            AccountError::SupervisorLimitReached           => "ACC-3003",
            AccountError::AlreadySupervised                => "ACC-3004",
            AccountError::SupervisionNotFound              => "ACC-3005",
            AccountError::SelfSupervision                  => "ACC-3006",

            AccountError::ConcurrentModification           => "ACC-4001",

            AccountError::MfaAlreadyEnrolled               => "ACC-5001",
            AccountError::MfaNotEnrolled                   => "ACC-5002",
            AccountError::RecoveryCodeInvalid              => "ACC-5003",

            AccountError::InvalidKycTransition { .. }      => "ACC-6001",

            AccountError::GdprDeletionAlreadyRequested     => "ACC-7001",
            AccountError::AccountAlreadyAnonymized         => "ACC-7002",
            AccountError::NoPendingGdprDeletion            => "ACC-7003",
            AccountError::GdprGracePeriodOver              => "ACC-7004",
            AccountError::DataExportUnavailable { .. }     => "ACC-7005",
            AccountError::DirectoryUnavailable { .. }      => "ACC-7006",
            AccountError::ContactLookupQuotaExceeded { .. } => "ACC-7007",

            AccountError::RoleAlreadyAssigned(_)           => "ACC-8001",
            AccountError::RoleNotAssigned(_)               => "ACC-8002",

            AccountError::DomainViolation { .. }           => "ACC-9001",
            AccountError::InvalidAccountId(_)              => "ACC-9002",
            AccountError::InvalidIdentityId(_)             => "ACC-9003",
            AccountError::InvalidEmail(_)                  => "ACC-9004",
            AccountError::InvalidPhone(_)                  => "ACC-9005",
            AccountError::InvalidCountryCode(_)            => "ACC-9006",
            AccountError::InvalidAccountStatus(_)          => "ACC-9007",
            AccountError::InvalidKycStatus(_)              => "ACC-9008",
            AccountError::InvalidAccountRole(_)            => "ACC-9009",
            AccountError::EventPublishFailed(_)            => "ACC-9010",
        }
    }

    fn http_status(&self) -> StatusCode {
        match self {
            AccountError::Storage(e)    => e.http_status(),
            AccountError::Validation(e) => e.http_status(),

            AccountError::AccountNotFound { .. }
            | AccountError::SupervisionInviteInvalid
            | AccountError::SupervisionNotFound => StatusCode::NOT_FOUND,

            AccountError::IdentityAlreadyRegistered { .. }
            | AccountError::EmailAlreadyRegistered { .. }
            | AccountError::PhoneAlreadyRegistered
            | AccountError::EmailAlreadyVerified
            | AccountError::ConcurrentModification
            | AccountError::MfaAlreadyEnrolled
            | AccountError::GdprDeletionAlreadyRequested
            | AccountError::DateOfBirthAlreadySet
            | AccountError::RoleAlreadyAssigned(_)
            | AccountError::SupervisorLimitReached
            | AccountError::AlreadySupervised => StatusCode::CONFLICT,

            AccountError::EventPublishFailed(_) => StatusCode::INTERNAL_SERVER_ERROR,

            AccountError::DataExportUnavailable { .. }
            | AccountError::DirectoryUnavailable { .. } => StatusCode::SERVICE_UNAVAILABLE,
            AccountError::ContactLookupQuotaExceeded { .. } => StatusCode::TOO_MANY_REQUESTS,

            _ => StatusCode::UNPROCESSABLE_ENTITY,
        }
    }

    fn severity(&self) -> Severity {
        match self {
            AccountError::Storage(e)    => e.severity(),
            AccountError::Validation(e) => e.severity(),

            AccountError::ConcurrentModification => Severity::High,

            AccountError::AccountNotActive { .. }
            | AccountError::InvalidStatusTransition { .. }
            | AccountError::InvalidKycTransition { .. }
            | AccountError::DomainViolation { .. } => Severity::Medium,

            _ => Severity::Low,
        }
    }

    fn is_retryable(&self) -> bool {
        match self {
            AccountError::Storage(e)             => e.is_retryable(),
            AccountError::ConcurrentModification => true,
            AccountError::DataExportUnavailable { .. } | AccountError::DirectoryUnavailable { .. } => true,
            _                                    => false,
        }
    }

    fn category(&self) -> &'static str {
        match self {
            AccountError::Storage(e)    => e.category(),
            AccountError::Validation(e) => e.category(),
            _                           => "ACC",
        }
    }

    fn user_facing_message(&self) -> &'static str {
        match self {
            AccountError::Storage(e)    => e.user_facing_message(),
            AccountError::Validation(e) => e.user_facing_message(),

            AccountError::AccountNotFound { .. }           => "The requested account does not exist.",
            AccountError::IdentityAlreadyRegistered { .. } => "This identity is already associated with an account.",
            AccountError::EmailAlreadyRegistered { .. }    => "This email address is already registered.",
            AccountError::PhoneAlreadyRegistered           => "This phone number is already registered.",
            AccountError::AccountNotActive { .. }          => "This operation is not permitted for the account's current status.",
            AccountError::InvalidStatusTransition { .. }   => "This status transition is not permitted.",
            AccountError::EmailAlreadyVerified             => "The email address for this account is already verified.",
            AccountError::ConcurrentModification           => "The account was modified concurrently. Please retry.",
            AccountError::MfaAlreadyEnrolled               => "Multi-factor authentication is already set up.",
            AccountError::MfaNotEnrolled                   => "Multi-factor authentication is not configured.",
            AccountError::RecoveryCodeInvalid              => "That backup code is not valid.",
            AccountError::InvalidKycTransition { .. }      => "This KYC status transition is not permitted.",
            AccountError::GdprDeletionAlreadyRequested     => "A deletion request has already been submitted.",
            AccountError::AgeBelowMinimum { .. }           => "You are not old enough to use this service.",
            AccountError::DateOfBirthAlreadySet            => "Your date of birth is already on file; contact support to change it.",
            AccountError::AccountAlreadyAnonymized         => "This account has already been anonymized.",
            AccountError::NoPendingGdprDeletion            => "No deletion is pending for this account.",
            AccountError::GdprGracePeriodOver              => "This account's deletion can no longer be cancelled.",
            AccountError::DataExportUnavailable { .. }     => "Your data export is being prepared; please check again later.",
            AccountError::DirectoryUnavailable { .. }      => "Finding your contacts is unavailable right now; please try again later.",
            AccountError::ContactLookupQuotaExceeded { .. } => "You have looked up many contacts today; please try again tomorrow.",
            AccountError::SupervisionInviteInvalid => "This code is not valid or has expired.",
            AccountError::SupervisionRoleNotAllowed { .. } => "Supervision pairs an adult with a teen aged 13 to 17.",
            AccountError::SupervisorLimitReached => "This teen already has two supervisors.",
            AccountError::AlreadySupervised => "These accounts are already paired.",
            AccountError::SupervisionNotFound => "This supervision was not found.",
            AccountError::SelfSupervision => "You cannot supervise your own account.",
            AccountError::RoleAlreadyAssigned(_)           => "This role is already assigned to the account.",
            AccountError::RoleNotAssigned(_)               => "This role is not assigned to the account.",
            _                                              => "A domain constraint was violated.",
        }
    }
}
