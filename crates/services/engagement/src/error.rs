use error::{AppError, Severity};
use http::StatusCode;
use thiserror::Error;

#[derive(Debug, Error)]
#[non_exhaustive]
pub enum EngagementError {
    #[error(transparent)]
    Scylla(#[from] scylla_storage::ScyllaStorageError),

    #[error(transparent)]
    Redis(#[from] redis_storage::RedisStorageError),

    #[error(transparent)]
    Validation(#[from] validation::ValidationError),

    // ── ENG-1xxx: Reaction state violations ───────────────────────────────────
    #[error("reaction not found for post {post_id} / profile {profile_id}")]
    ReactionNotFound { post_id: String, profile_id: String },

    // ── ENG-2xxx: Reaction kind / weight validation ────────────────────────────
    #[error("unknown reaction kind: '{kind}'")]
    UnknownReactionKind { kind: String },

    #[error("reaction weight for kind '{kind}' must be positive (got {weight})")]
    InvalidReactionWeight { kind: String, weight: i64 },

    // ── ENG-3xxx: Kafka / event publish errors ────────────────────────────────
    #[error("failed to publish engagement event to Kafka: {message}")]
    EventPublishFailed { message: String },

    // ── ENG-5xxx: Worker / background task errors ─────────────────────────────
    #[error("Lua script returned an unexpected value")]
    ScriptReturnInvalid,

    #[error("counter flush failed for post {post_id}: {message}")]
    CounterFlushFailed { post_id: String, message: String },

    /// The durable ledger (ScyllaDB) is not wired: this instance runs without
    /// the write-behind path.
    #[error("the reaction ledger is unavailable on this instance")]
    LedgerUnavailable,

    // ── ENG-9xxx: ID parsing / domain violations ──────────────────────────────
    #[error("invalid post ID: '{0}'")]
    InvalidPostId(String),

    #[error("invalid profile ID: '{0}'")]
    InvalidProfileId(String),

    #[error("domain violation on field '{field}': {message}")]
    DomainViolation { field: String, message: String },
}

impl AppError for EngagementError {
    fn error_code(&self) -> &'static str {
        match self {
            Self::Scylla(e)     => e.error_code(),
            Self::Redis(e)      => e.error_code(),
            Self::Validation(e) => e.error_code(),

            Self::ReactionNotFound { .. }     => "ENG-1001",

            Self::UnknownReactionKind { .. }  => "ENG-2001",
            Self::InvalidReactionWeight { .. } => "ENG-2002",

            Self::EventPublishFailed { .. }   => "ENG-3001",

            Self::ScriptReturnInvalid         => "ENG-5001",
            Self::CounterFlushFailed { .. }   => "ENG-5002",
            Self::LedgerUnavailable           => "ENG-5003",

            Self::InvalidPostId(_)            => "ENG-9001",
            Self::InvalidProfileId(_)         => "ENG-9002",
            Self::DomainViolation { .. }      => "ENG-9003",
        }
    }

    fn http_status(&self) -> StatusCode {
        match self {
            Self::Scylla(e)     => e.http_status(),
            Self::Redis(e)      => e.http_status(),
            Self::Validation(e) => e.http_status(),

            Self::ReactionNotFound { .. } => StatusCode::NOT_FOUND,

            Self::UnknownReactionKind { .. }
            | Self::InvalidReactionWeight { .. }
            | Self::InvalidPostId(_)
            | Self::InvalidProfileId(_)
            | Self::DomainViolation { .. } => StatusCode::UNPROCESSABLE_ENTITY,

            Self::EventPublishFailed { .. }
            | Self::ScriptReturnInvalid
            | Self::CounterFlushFailed { .. } => StatusCode::INTERNAL_SERVER_ERROR,

            Self::LedgerUnavailable => StatusCode::SERVICE_UNAVAILABLE,
        }
    }

    fn severity(&self) -> Severity {
        match self {
            Self::Scylla(e) => e.severity(),
            Self::Redis(e)  => e.severity(),

            Self::EventPublishFailed { .. }
            | Self::ScriptReturnInvalid
            | Self::CounterFlushFailed { .. }
            | Self::LedgerUnavailable => Severity::High,

            Self::Validation(e) => e.severity(),

            Self::UnknownReactionKind { .. }
            | Self::InvalidReactionWeight { .. }
            | Self::DomainViolation { .. } => Severity::Medium,

            Self::ReactionNotFound { .. }
            | Self::InvalidPostId(_)
            | Self::InvalidProfileId(_) => Severity::Low,
        }
    }

    fn is_retryable(&self) -> bool {
        match self {
            Self::Scylla(e) => e.is_retryable(),
            Self::Redis(e)  => e.is_retryable(),
            _               => false,
        }
    }

    fn category(&self) -> &'static str {
        match self {
            Self::Scylla(e)     => e.category(),
            Self::Redis(e)      => e.category(),
            Self::Validation(e) => e.category(),
            _                   => "ENG",
        }
    }

    fn user_facing_message(&self) -> &'static str {
        match self {
            Self::Scylla(_)
            | Self::Redis(_)
            | Self::EventPublishFailed { .. }
            | Self::ScriptReturnInvalid
            | Self::CounterFlushFailed { .. }
            | Self::LedgerUnavailable =>
                "An internal error occurred. Please try again later.",

            Self::ReactionNotFound { .. } =>
                "No active reaction was found for this post.",

            Self::UnknownReactionKind { .. }
            | Self::InvalidReactionWeight { .. } =>
                "The reaction type provided is not supported.",

            Self::InvalidPostId(_)    => "The provided post ID is not valid.",
            Self::InvalidProfileId(_) => "The provided profile ID is not valid.",
            Self::DomainViolation { .. } =>
                "The request contains an invalid domain value.",

            Self::Validation(e) => e.user_facing_message(),
        }
    }
}

/// Classifies failures for the Kafka consumer runner: transient storage/cache
/// faults are retried with backoff, data/invariant errors are dead-lettered.
impl transport::kafka::consumer::ClassifyError for EngagementError {
    fn is_retryable(&self) -> bool {
        <Self as AppError>::is_retryable(self)
    }
}
