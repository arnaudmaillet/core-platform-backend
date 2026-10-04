use error::{AppError, Severity};
use http::StatusCode;
use thiserror::Error;

/// Canonical domain and application error type for the social-graph microservice.
///
/// ## Error code catalogue
///
/// | Code     | Variant               | HTTP | Severity | Retryable |
/// |----------|-----------------------|------|----------|-----------|
/// | SGR-1001 | AlreadyFollowing      | 409  | Low      | No        |
/// | SGR-1002 | NotFollowing          | 422  | Low      | No        |
/// | SGR-1005 | AlreadyRequested      | 409  | Low      | No        |
/// | SGR-1006 | NoFollowRequest       | 422  | Low      | No        |
/// | SGR-1003 | AlreadyBlocked        | 409  | Low      | No        |
/// | SGR-1004 | NotBlocked            | 422  | Low      | No        |
/// | SGR-2001 | SelfInteraction       | 422  | Low      | No        |
/// | SGR-2002 | BlockGateDenied       | 422  | Medium   | No        |
/// | SGR-9001 | DomainViolation       | 422  | Medium   | No        |
/// | SGR-9002 | InvalidProfileId      | 422  | Low      | No        |
/// | SDB-*    | Storage (delegated)   | var  | var      | var       |
/// | RDB-*    | Cache (delegated)     | 500  | var      | var       |
/// | VAL-*    | Validation (delegated)| 422  | Low      | No        |
#[derive(Debug, Error)]
#[non_exhaustive]
pub enum SocialGraphError {
    // ── Infrastructure delegates ───────────────────────────────────────────────

    #[error(transparent)]
    Storage(#[from] scylla_storage::ScyllaStorageError),

    #[error(transparent)]
    Cache(#[from] redis_storage::RedisStorageError),

    #[error(transparent)]
    Validation(#[from] validation::ValidationError),

    // ── Follow state (SGR-1xxx) ───────────────────────────────────────────────

    #[error("profile '{actor_id}' already follows '{target_id}'")]
    AlreadyFollowing { actor_id: String, target_id: String },

    #[error("profile '{actor_id}' does not follow '{target_id}'")]
    NotFollowing { actor_id: String, target_id: String },

    #[error("profile '{actor_id}' has already asked to follow '{target_id}'")]
    AlreadyRequested { actor_id: String, target_id: String },

    #[error("profile '{actor_id}' has no pending request to follow '{target_id}'")]
    NoFollowRequest { actor_id: String, target_id: String },

    // ── Block state (SGR-1xxx continued) ─────────────────────────────────────

    #[error("profile '{actor_id}' has already blocked '{target_id}'")]
    AlreadyBlocked { actor_id: String, target_id: String },

    #[error("profile '{actor_id}' has not blocked '{target_id}'")]
    NotBlocked { actor_id: String, target_id: String },

    // ── Graph invariants (SGR-2xxx) ───────────────────────────────────────────

    #[error("a profile cannot follow or block itself")]
    SelfInteraction,

    #[error("follow rejected: a block relationship exists between '{actor_id}' and '{target_id}'")]
    BlockGateDenied { actor_id: String, target_id: String },

    // ── Domain parse errors (SGR-9xxx) ────────────────────────────────────────

    #[error("domain invariant violated on '{field}': {message}")]
    DomainViolation { field: String, message: String },

    #[error("invalid profile ID: '{0}'")]
    InvalidProfileId(String),
}

impl AppError for SocialGraphError {
    fn error_code(&self) -> &'static str {
        match self {
            SocialGraphError::Storage(e)    => e.error_code(),
            SocialGraphError::Cache(e)      => e.error_code(),
            SocialGraphError::Validation(e) => e.error_code(),

            SocialGraphError::AlreadyFollowing { .. } => "SGR-1001",
            SocialGraphError::NotFollowing { .. }     => "SGR-1002",
            SocialGraphError::AlreadyRequested { .. } => "SGR-1005",
            SocialGraphError::NoFollowRequest { .. }  => "SGR-1006",
            SocialGraphError::AlreadyBlocked { .. }   => "SGR-1003",
            SocialGraphError::NotBlocked { .. }       => "SGR-1004",

            SocialGraphError::SelfInteraction           => "SGR-2001",
            SocialGraphError::BlockGateDenied { .. }    => "SGR-2002",

            SocialGraphError::DomainViolation { .. } => "SGR-9001",
            SocialGraphError::InvalidProfileId(_)    => "SGR-9002",
        }
    }

    fn http_status(&self) -> StatusCode {
        match self {
            SocialGraphError::Storage(e)    => e.http_status(),
            SocialGraphError::Cache(e)      => e.http_status(),
            SocialGraphError::Validation(e) => e.http_status(),

            SocialGraphError::AlreadyFollowing { .. }
            | SocialGraphError::AlreadyRequested { .. }
            | SocialGraphError::AlreadyBlocked { .. } => StatusCode::CONFLICT,

            SocialGraphError::NotFollowing { .. }
            | SocialGraphError::NoFollowRequest { .. }
            | SocialGraphError::NotBlocked { .. }
            | SocialGraphError::SelfInteraction
            | SocialGraphError::BlockGateDenied { .. }
            | SocialGraphError::DomainViolation { .. }
            | SocialGraphError::InvalidProfileId(_) => StatusCode::UNPROCESSABLE_ENTITY,
        }
    }

    fn severity(&self) -> Severity {
        match self {
            SocialGraphError::Storage(e)    => e.severity(),
            SocialGraphError::Cache(e)      => e.severity(),
            SocialGraphError::Validation(e) => e.severity(),

            SocialGraphError::BlockGateDenied { .. }
            | SocialGraphError::DomainViolation { .. } => Severity::Medium,

            _ => Severity::Low,
        }
    }

    fn is_retryable(&self) -> bool {
        match self {
            SocialGraphError::Storage(e) => e.is_retryable(),
            SocialGraphError::Cache(e)   => e.is_retryable(),
            _                            => false,
        }
    }

    fn category(&self) -> &'static str {
        match self {
            SocialGraphError::Storage(e)    => e.category(),
            SocialGraphError::Cache(e)      => e.category(),
            SocialGraphError::Validation(e) => e.category(),
            _                               => "SGR",
        }
    }

    fn user_facing_message(&self) -> &'static str {
        match self {
            SocialGraphError::Storage(e)    => e.user_facing_message(),
            SocialGraphError::Cache(e)      => e.user_facing_message(),
            SocialGraphError::Validation(e) => e.user_facing_message(),

            SocialGraphError::AlreadyFollowing { .. } => "You are already following this profile.",
            SocialGraphError::NotFollowing { .. }     => "You are not following this profile.",
            SocialGraphError::AlreadyRequested { .. } => "You have already asked to follow this profile.",
            SocialGraphError::NoFollowRequest { .. }  => "There is no pending follow request.",
            SocialGraphError::AlreadyBlocked { .. }   => "You have already blocked this profile.",
            SocialGraphError::NotBlocked { .. }       => "You have not blocked this profile.",
            SocialGraphError::SelfInteraction          => "A profile cannot follow or block itself.",
            SocialGraphError::BlockGateDenied { .. }   => "A block relationship prevents this follow.",
            SocialGraphError::DomainViolation { .. }   => "A domain constraint was violated.",
            SocialGraphError::InvalidProfileId(_)      => "The provided profile ID is not valid.",
        }
    }
}
