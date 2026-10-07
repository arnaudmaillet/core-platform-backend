use error::{AppError, Severity};
use http::StatusCode;
use thiserror::Error;

#[derive(Debug, Error)]
#[non_exhaustive]
pub enum NotificationError {
    #[error(transparent)]
    Scylla(#[from] scylla_storage::ScyllaStorageError),

    #[error(transparent)]
    Redis(#[from] redis_storage::RedisStorageError),

    #[error(transparent)]
    Validation(#[from] validation::ValidationError),

    // ── NTF-1xxx: Notification lifecycle ──────────────────────────────────────
    #[error("notification {notification_id} not found for profile {profile_id}")]
    NotificationNotFound { notification_id: String, profile_id: String },

    #[error("notification {notification_id} is already marked as read")]
    AlreadyRead { notification_id: String },

    #[error("notification suppressed: sender {sender_id} is blocked by target {target_id}")]
    SenderBlocked { sender_id: String, target_id: String },

    #[error("self-notification suppressed: sender and target are the same profile ({profile_id})")]
    SelfNotification { profile_id: String },

    // ── NTF-2xxx: Domain validation ───────────────────────────────────────────
    #[error("unknown notification kind: '{kind}'")]
    UnknownNotificationKind { kind: String },

    #[error("unknown subject kind: '{kind}'")]
    UnknownSubjectKind { kind: String },

    #[error("invalid page token: '{token}'")]
    InvalidPageToken { token: String },

    /// The push token is registered from another device, for another account:
    /// a token only moves between accounts on the device it belongs to.
    #[error("this push token belongs to another device")]
    PushTokenOnAnotherDevice,

    // ── NTF-3xxx: Kafka / event errors ────────────────────────────────────────
    #[error("failed to publish notification event to Kafka: {message}")]
    EventPublishFailed { message: String },

    /// A push did not reach the push network (#654); logged, never surfaced.
    #[error("push delivery failed: {reason}")]
    PushDeliveryFailed { reason: String },

    // ── NTF-4xxx: gRPC streaming errors ──────────────────────────────────────
    #[error("stream registry send failed for profile {profile_id}: channel closed")]
    StreamSendFailed { profile_id: String },

    // ── NTF-5xxx: Worker / collapse pipeline errors ───────────────────────────
    #[error("collapse flush failed for window '{window_key}': {message}")]
    CollapseFlushFailed { window_key: String, message: String },

    #[error("Lua script returned an unexpected value for key '{key}'")]
    ScriptReturnInvalid { key: String },

    // ── NTF-6xxx: Cache errors ─────────────────────────────────────────────────
    #[error("post author cache miss for post {post_id}: reaction notification suppressed")]
    PostAuthorCacheMiss { post_id: String },

    #[error("comment author cache miss for comment {comment_id}: reply notification suppressed")]
    CommentAuthorCacheMiss { comment_id: String },

    // ── NTF-9xxx: ID parsing / domain violations ──────────────────────────────
    #[error("invalid notification ID: '{0}'")]
    InvalidNotificationId(String),

    #[error("invalid profile ID: '{0}'")]
    InvalidProfileId(String),

    #[error("invalid subject ID: '{0}'")]
    InvalidSubjectId(String),

    #[error("domain violation on field '{field}': {message}")]
    DomainViolation { field: String, message: String },
}

impl AppError for NotificationError {
    fn error_code(&self) -> &'static str {
        match self {
            Self::Scylla(e)     => e.error_code(),
            Self::Redis(e)      => e.error_code(),
            Self::Validation(e) => e.error_code(),

            Self::NotificationNotFound { .. } => "NTF-1001",
            Self::AlreadyRead { .. }          => "NTF-1002",
            Self::SenderBlocked { .. }        => "NTF-1003",
            Self::SelfNotification { .. }     => "NTF-1004",

            Self::UnknownNotificationKind { .. } => "NTF-2001",
            Self::UnknownSubjectKind { .. }      => "NTF-2002",
            Self::InvalidPageToken { .. }        => "NTF-2003",
            Self::PushTokenOnAnotherDevice       => "NTF-2004",

            Self::EventPublishFailed { .. }   => "NTF-3001",
            Self::PushDeliveryFailed { .. }   => "NTF-3002",

            Self::StreamSendFailed { .. }     => "NTF-4001",

            Self::CollapseFlushFailed { .. }  => "NTF-5001",
            Self::ScriptReturnInvalid { .. }  => "NTF-5002",

            Self::PostAuthorCacheMiss { .. }    => "NTF-6001",
            Self::CommentAuthorCacheMiss { .. } => "NTF-6002",

            Self::InvalidNotificationId(_) => "NTF-9001",
            Self::InvalidProfileId(_)      => "NTF-9002",
            Self::InvalidSubjectId(_)      => "NTF-9003",
            Self::DomainViolation { .. }   => "NTF-9004",
        }
    }

    fn http_status(&self) -> StatusCode {
        match self {
            Self::Scylla(e)     => e.http_status(),
            Self::Redis(e)      => e.http_status(),
            Self::Validation(e) => e.http_status(),

            Self::NotificationNotFound { .. } => StatusCode::NOT_FOUND,
            Self::AlreadyRead { .. }
            | Self::PushTokenOnAnotherDevice => StatusCode::CONFLICT,

            Self::SenderBlocked { .. }
            | Self::SelfNotification { .. } => StatusCode::UNPROCESSABLE_ENTITY,

            Self::UnknownNotificationKind { .. }
            | Self::UnknownSubjectKind { .. }
            | Self::InvalidPageToken { .. }
            | Self::InvalidNotificationId(_)
            | Self::InvalidProfileId(_)
            | Self::InvalidSubjectId(_)
            | Self::DomainViolation { .. } => StatusCode::UNPROCESSABLE_ENTITY,

            Self::EventPublishFailed { .. }
            | Self::PushDeliveryFailed { .. }
            | Self::StreamSendFailed { .. }
            | Self::CollapseFlushFailed { .. }
            | Self::ScriptReturnInvalid { .. }
            | Self::PostAuthorCacheMiss { .. }
            | Self::CommentAuthorCacheMiss { .. } => StatusCode::INTERNAL_SERVER_ERROR,
        }
    }

    fn severity(&self) -> Severity {
        match self {
            Self::Scylla(e) => e.severity(),
            Self::Redis(e)  => e.severity(),

            Self::EventPublishFailed { .. }
            | Self::CollapseFlushFailed { .. }
            | Self::ScriptReturnInvalid { .. } => Severity::High,

            Self::StreamSendFailed { .. } | Self::PushDeliveryFailed { .. } => Severity::Medium,

            Self::PostAuthorCacheMiss { .. }
            | Self::CommentAuthorCacheMiss { .. } => Severity::Medium,

            Self::Validation(e) => e.severity(),

            Self::UnknownNotificationKind { .. }
            | Self::UnknownSubjectKind { .. }
            | Self::InvalidPageToken { .. }
            | Self::DomainViolation { .. } => Severity::Medium,

            Self::PushTokenOnAnotherDevice => Severity::Medium,

            Self::NotificationNotFound { .. }
            | Self::AlreadyRead { .. }
            | Self::SenderBlocked { .. }
            | Self::SelfNotification { .. }
            | Self::InvalidNotificationId(_)
            | Self::InvalidProfileId(_)
            | Self::InvalidSubjectId(_) => Severity::Low,
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
            _                   => "NTF",
        }
    }

    fn user_facing_message(&self) -> &'static str {
        match self {
            Self::Scylla(_)
            | Self::Redis(_)
            | Self::EventPublishFailed { .. }
            | Self::PushDeliveryFailed { .. }
            | Self::StreamSendFailed { .. }
            | Self::CollapseFlushFailed { .. }
            | Self::ScriptReturnInvalid { .. }
            | Self::PostAuthorCacheMiss { .. }
            | Self::CommentAuthorCacheMiss { .. } =>
                "An internal error occurred. Please try again later.",

            Self::NotificationNotFound { .. } =>
                "The notification was not found.",

            Self::AlreadyRead { .. } =>
                "This notification is already marked as read.",

            Self::SenderBlocked { .. } | Self::SelfNotification { .. } =>
                "The notification could not be delivered.",

            Self::UnknownNotificationKind { .. } | Self::UnknownSubjectKind { .. } =>
                "The notification type is not supported.",

            Self::InvalidPageToken { .. } =>
                "The pagination token is invalid or expired.",

            Self::PushTokenOnAnotherDevice =>
                "This device cannot receive notifications for this account.",

            Self::InvalidNotificationId(_) => "The notification ID is not valid.",
            Self::InvalidProfileId(_)      => "The profile ID is not valid.",
            Self::InvalidSubjectId(_)      => "The subject ID is not valid.",

            Self::DomainViolation { .. } =>
                "The request contains an invalid value.",

            Self::Validation(e) => e.user_facing_message(),
        }
    }
}

/// Classifies failures for the Kafka consumer runner: transient faults are retried
/// with backoff, permanent ones are dead-lettered. Delegates to the existing
/// [`AppError::is_retryable`] so storage timeouts stay retryable while data /
/// invariant errors are treated as poison.
impl transport::kafka::consumer::ClassifyError for NotificationError {
    fn is_retryable(&self) -> bool {
        <Self as AppError>::is_retryable(self)
    }
}
