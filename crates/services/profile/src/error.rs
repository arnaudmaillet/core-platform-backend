use error::{AppError, Severity};
use http::StatusCode;
use thiserror::Error;

/// Canonical domain and application error type for the profile microservice.
///
/// ## Code catalogue
///
/// | Code     | Variant                  | HTTP | Severity | Retryable |
/// |----------|--------------------------|------|----------|-----------|
/// | PRF-1001 | ProfileNotFound          | 404  | Low      | No        |
/// | PRF-1002 | HandleAlreadyTaken       | 409  | Low      | No        |
/// | PRF-1003 | HandleReserved           | 409  | Low      | No        |
/// | PRF-2001 | ProfileNotActive         | 422  | Medium   | No        |
/// | PRF-2002 | InvalidStatusTransition  | 422  | Medium   | No        |
/// | PRF-4001 | ConcurrentModification   | 409  | High     | **Yes**   |
/// | PRF-5001 | ProfileAlreadyVerified   | 409  | Low      | No        |
/// | PRF-5002 | VerificationPending      | 409  | Low      | No        |
/// | PRF-5003 | NoPendingVerification    | 409  | Low      | No        |
/// | PRF-5004 | VerificationDocumentInvalid | 422 | Low     | No        |
/// | PRF-5005 | MediaUnavailable         | 503  | Medium   | **Yes**   |
/// | PRF-9001 | DomainViolation          | 422  | Medium   | No        |
/// | PRF-9002 | InvalidProfileId         | 422  | Low      | No        |
/// | PRF-9003 | InvalidHandle            | 422  | Low      | No        |
/// | PRF-9004 | InvalidDisplayName       | 422  | Low      | No        |
/// | PRF-9005 | InvalidBio               | 422  | Low      | No        |
/// | PRF-9006 | InvalidUrl               | 422  | Low      | No        |
/// | PRF-9007 | InvalidLocale            | 422  | Low      | No        |
/// | PRF-9008 | InvalidProfileKind       | 422  | Low      | No        |
/// | PRF-9009 | InvalidProfileStatus     | 422  | Low      | No        |
/// | PRF-9010 | TooManyCustomLinks       | 422  | Low      | No        |
/// | SDB-*    | Storage (delegated)      | var  | var      | var       |
/// | RDB-*    | Cache (delegated)        | 500  | var      | var       |
/// | VAL-*    | Validation (delegated)   | 422  | Low      | No        |
#[derive(Debug, Error)]
#[non_exhaustive]
pub enum ProfileError {
    // ── Infrastructure delegates ───────────────────────────────────────────────

    #[error(transparent)]
    Storage(#[from] scylla_storage::ScyllaStorageError),

    #[error(transparent)]
    Cache(#[from] redis_storage::RedisStorageError),

    #[error(transparent)]
    Validation(#[from] validation::ValidationError),

    // ── Existence (PRF-1xxx) ──────────────────────────────────────────────────

    #[error("profile not found: {id}")]
    ProfileNotFound { id: String },

    #[error("handle '{handle}' is already taken by another profile")]
    HandleAlreadyTaken { handle: String },

    #[error("handle '{handle}' is currently reserved and cannot be claimed for 30 days")]
    HandleReserved { handle: String },

    // ── Lifecycle state machine (PRF-2xxx) ────────────────────────────────────

    #[error("operation requires an active profile; current status: '{current}'")]
    ProfileNotActive { current: String },

    #[error("status transition from '{from}' to '{to}' is not permitted")]
    InvalidStatusTransition { from: String, to: String },

    // ── Optimistic concurrency (PRF-4xxx) ─────────────────────────────────────

    #[error("concurrent modification detected; reload the profile and retry")]
    ConcurrentModification,

    // ── Verification (PRF-5xxx) ───────────────────────────────────────────────

    #[error("this profile is already verified")]
    ProfileAlreadyVerified,

    #[error("a verification request is already pending for this profile")]
    VerificationPending,

    #[error("no verification request is pending for this profile")]
    NoPendingVerification,

    /// A private document named by a verification request is not one of the
    /// requester's ready private documents (#777).
    #[error("'{id}' is not one of your private documents")]
    VerificationDocumentInvalid { id: String },

    /// media could not check a private document (#777).
    #[error("media is unavailable: {reason}")]
    MediaUnavailable { reason: String },

    // ── Domain invariants & parse errors (PRF-9xxx) ───────────────────────────

    #[error("domain invariant violated on '{field}': {message}")]
    DomainViolation { field: String, message: String },

    #[error("invalid profile ID: '{0}'")]
    InvalidProfileId(String),

    #[error("invalid handle: '{0}'")]
    InvalidHandle(String),

    #[error("invalid display name: '{0}'")]
    InvalidDisplayName(String),

    #[error("invalid bio: '{0}'")]
    InvalidBio(String),

    #[error("invalid URL: '{0}'")]
    InvalidUrl(String),

    #[error("invalid locale tag: '{0}'")]
    InvalidLocale(String),

    #[error("unknown profile kind: '{0}'")]
    InvalidProfileKind(String),

    #[error("unknown profile status: '{0}'")]
    InvalidProfileStatus(String),

    #[error("too many custom links: maximum is 5, got {count}")]
    TooManyCustomLinks { count: usize },
}

impl AppError for ProfileError {
    fn error_code(&self) -> &'static str {
        match self {
            ProfileError::Storage(e)    => e.error_code(),
            ProfileError::Cache(e)      => e.error_code(),
            ProfileError::Validation(e) => e.error_code(),

            ProfileError::ProfileNotFound { .. }    => "PRF-1001",
            ProfileError::HandleAlreadyTaken { .. } => "PRF-1002",
            ProfileError::HandleReserved { .. }     => "PRF-1003",

            ProfileError::ProfileNotActive { .. }       => "PRF-2001",
            ProfileError::InvalidStatusTransition { .. } => "PRF-2002",

            ProfileError::ConcurrentModification => "PRF-4001",

            ProfileError::ProfileAlreadyVerified => "PRF-5001",
            ProfileError::VerificationPending    => "PRF-5002",
            ProfileError::NoPendingVerification  => "PRF-5003",
            ProfileError::VerificationDocumentInvalid { .. } => "PRF-5004",
            ProfileError::MediaUnavailable { .. } => "PRF-5005",

            ProfileError::DomainViolation { .. }  => "PRF-9001",
            ProfileError::InvalidProfileId(_)     => "PRF-9002",
            ProfileError::InvalidHandle(_)        => "PRF-9003",
            ProfileError::InvalidDisplayName(_)   => "PRF-9004",
            ProfileError::InvalidBio(_)           => "PRF-9005",
            ProfileError::InvalidUrl(_)           => "PRF-9006",
            ProfileError::InvalidLocale(_)        => "PRF-9007",
            ProfileError::InvalidProfileKind(_)   => "PRF-9008",
            ProfileError::InvalidProfileStatus(_) => "PRF-9009",
            ProfileError::TooManyCustomLinks { .. } => "PRF-9010",
        }
    }

    fn http_status(&self) -> StatusCode {
        match self {
            ProfileError::Storage(e)    => e.http_status(),
            ProfileError::Cache(e)      => e.http_status(),
            ProfileError::Validation(e) => e.http_status(),

            ProfileError::ProfileNotFound { .. } => StatusCode::NOT_FOUND,

            ProfileError::HandleAlreadyTaken { .. }
            | ProfileError::HandleReserved { .. }
            | ProfileError::ConcurrentModification
            | ProfileError::ProfileAlreadyVerified
            | ProfileError::VerificationPending
            | ProfileError::NoPendingVerification => StatusCode::CONFLICT,

            ProfileError::MediaUnavailable { .. } => StatusCode::SERVICE_UNAVAILABLE,

            _ => StatusCode::UNPROCESSABLE_ENTITY,
        }
    }

    fn severity(&self) -> Severity {
        match self {
            ProfileError::Storage(e)    => e.severity(),
            ProfileError::Cache(e)      => e.severity(),
            ProfileError::Validation(e) => e.severity(),

            ProfileError::ConcurrentModification => Severity::High,

            ProfileError::ProfileNotActive { .. }
            | ProfileError::InvalidStatusTransition { .. }
            | ProfileError::DomainViolation { .. } => Severity::Medium,

            _ => Severity::Low,
        }
    }

    fn is_retryable(&self) -> bool {
        match self {
            ProfileError::Storage(e)             => e.is_retryable(),
            ProfileError::Cache(e)               => e.is_retryable(),
            ProfileError::ConcurrentModification => true,
            ProfileError::MediaUnavailable { .. } => true,
            _                                    => false,
        }
    }

    fn category(&self) -> &'static str {
        match self {
            ProfileError::Storage(e)    => e.category(),
            ProfileError::Cache(e)      => e.category(),
            ProfileError::Validation(e) => e.category(),
            _                           => "PRF",
        }
    }

    fn user_facing_message(&self) -> &'static str {
        match self {
            ProfileError::Storage(e)    => e.user_facing_message(),
            ProfileError::Cache(e)      => e.user_facing_message(),
            ProfileError::Validation(e) => e.user_facing_message(),

            ProfileError::ProfileNotFound { .. }        => "The requested profile does not exist.",
            ProfileError::HandleAlreadyTaken { .. }     => "This handle is already taken.",
            ProfileError::HandleReserved { .. }         => "This handle is temporarily reserved.",
            ProfileError::ProfileNotActive { .. }       => "This operation requires an active profile.",
            ProfileError::InvalidStatusTransition { .. } => "This status transition is not permitted.",
            ProfileError::ConcurrentModification        => "The profile was modified concurrently. Please retry.",
            ProfileError::ProfileAlreadyVerified        => "This profile is already verified.",
            ProfileError::VerificationPending           => "A verification request is already under review.",
            ProfileError::NoPendingVerification         => "There is no verification request under review.",
            ProfileError::VerificationDocumentInvalid { .. } => "A document is not one of your uploaded private documents.",
            ProfileError::MediaUnavailable { .. }       => "Your documents cannot be checked right now. Please retry.",
            ProfileError::TooManyCustomLinks { .. }     => "You may have at most 5 custom links.",
            _                                           => "A domain constraint was violated.",
        }
    }
}
