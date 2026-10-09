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

    // ENG-1001, ENG-2001/2002, ENG-3001 and ENG-9002 were the weighted
    // reactions' (removed with #665: a like is a point); never reused.

    // ── ENG-5xxx: Worker / background task errors ─────────────────────────────
    #[error("Lua script returned an unexpected value")]
    ScriptReturnInvalid,

    #[error("counter flush failed for post {post_id}: {message}")]
    CounterFlushFailed { post_id: String, message: String },

    /// The durable ledger (ScyllaDB) is not wired: this instance runs without
    /// the write-behind path.
    #[error("the likes ledger is unavailable on this instance")]
    LedgerUnavailable,

    // ── ENG-6xxx: Peers ───────────────────────────────────────────────────────
    /// post could not say whose post it is or whether its author hides like
    /// counts (#809): likes are withheld (fail closed), never shown.
    #[error("post is unavailable: {message}")]
    PostUnavailable { message: String },

    /// social-graph could not say whether the reader may see a profile
    /// (#829): its Likes tab is withheld (fail closed).
    #[error("social-graph is unavailable: {message}")]
    SocialGraphUnavailable { message: String },

    // ── ENG-9xxx: ID parsing / domain violations ──────────────────────────────
    #[error("invalid post ID: '{0}'")]
    InvalidPostId(String),

    #[error("domain violation on field '{field}': {message}")]
    DomainViolation { field: String, message: String },

    /// Not a post or comment a like can land on (#665).
    #[error("invalid like target: '{value}'")]
    InvalidLikeTarget { value: String },
}

impl AppError for EngagementError {
    fn error_code(&self) -> &'static str {
        match self {
            Self::Scylla(e)     => e.error_code(),
            Self::Redis(e)      => e.error_code(),
            Self::Validation(e) => e.error_code(),

            Self::ScriptReturnInvalid         => "ENG-5001",
            Self::CounterFlushFailed { .. }   => "ENG-5002",
            Self::LedgerUnavailable           => "ENG-5003",

            Self::PostUnavailable { .. }      => "ENG-6001",
            Self::SocialGraphUnavailable { .. } => "ENG-6002",

            Self::InvalidPostId(_)            => "ENG-9001",
            Self::DomainViolation { .. }      => "ENG-9003",
            Self::InvalidLikeTarget { .. }    => "ENG-9004",
        }
    }

    fn http_status(&self) -> StatusCode {
        match self {
            Self::Scylla(e)     => e.http_status(),
            Self::Redis(e)      => e.http_status(),
            Self::Validation(e) => e.http_status(),

            Self::InvalidPostId(_)
            | Self::InvalidLikeTarget { .. }
            | Self::DomainViolation { .. } => StatusCode::UNPROCESSABLE_ENTITY,

            Self::ScriptReturnInvalid
            | Self::CounterFlushFailed { .. } => StatusCode::INTERNAL_SERVER_ERROR,

            Self::LedgerUnavailable | Self::PostUnavailable { .. } | Self::SocialGraphUnavailable { .. } => {
                StatusCode::SERVICE_UNAVAILABLE
            }
        }
    }

    fn severity(&self) -> Severity {
        match self {
            Self::Scylla(e) => e.severity(),
            Self::Redis(e)  => e.severity(),

            Self::ScriptReturnInvalid
            | Self::CounterFlushFailed { .. }
            | Self::LedgerUnavailable => Severity::High,

            Self::Validation(e) => e.severity(),

            Self::DomainViolation { .. } => Severity::Medium,

            Self::InvalidPostId(_)
            | Self::InvalidLikeTarget { .. } => Severity::Low,

            Self::PostUnavailable { .. } | Self::SocialGraphUnavailable { .. } => Severity::Medium,
        }
    }

    fn is_retryable(&self) -> bool {
        match self {
            Self::Scylla(e) => e.is_retryable(),
            Self::Redis(e)  => e.is_retryable(),
            Self::PostUnavailable { .. } | Self::SocialGraphUnavailable { .. } => true,
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
            | Self::ScriptReturnInvalid
            | Self::CounterFlushFailed { .. }
            | Self::LedgerUnavailable
            | Self::PostUnavailable { .. }
            | Self::SocialGraphUnavailable { .. } =>
                "An internal error occurred. Please try again later.",

            Self::InvalidPostId(_)    => "The provided post ID is not valid.",
            Self::InvalidLikeTarget { .. } => "Only posts and comments can be liked.",
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
