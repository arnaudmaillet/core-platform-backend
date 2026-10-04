use error::{AppError, Severity};
use http::StatusCode;
use thiserror::Error;

#[derive(Debug, Error)]
#[non_exhaustive]
pub enum CommentError {
    #[error(transparent)]
    Storage(#[from] scylla_storage::ScyllaStorageError),

    #[error(transparent)]
    Validation(#[from] validation::ValidationError),

    // ── CMT-1xxx: Comment lifecycle violations ────────────────────────────────
    #[error("comment not found: {comment_id}")]
    CommentNotFound { comment_id: String },

    #[error("comment {comment_id} is already deleted")]
    CommentAlreadyDeleted { comment_id: String },

    #[error("caller {caller_id} is not the author of comment {comment_id}")]
    AuthorMismatch { comment_id: String, caller_id: String },

    /// The post is missing, or not one the commenter may read.
    #[error("post not found: {post_id}")]
    PostNotFound { post_id: String },

    /// The post's author does not take comments from this profile (their
    /// interaction settings, or a block).
    #[error("the author of post {post_id} does not take comments from this profile")]
    CommentsRestricted { post_id: String },

    // ── CMT-2xxx: Threading invariant violations ──────────────────────────────
    #[error("replies to replies are not allowed — maximum nesting depth is 1")]
    NestingDepthExceeded,

    #[error("parent comment {parent_id} was not found — cannot create reply")]
    ParentNotFound { parent_id: String },

    #[error("parent comment {parent_id} is deleted — cannot reply to a deleted comment")]
    ParentDeleted { parent_id: String },

    // ── CMT-3xxx: Content invariant violations ────────────────────────────────
    #[error("a comment must have text, a GIF attachment, or both")]
    EmptyContent,

    #[error("GIF metadata is incomplete — gif_id, gif_url, gif_width, and gif_height are all required")]
    IncompleteGifMetadata,

    // ── CMT-4xxx: Kafka / event publish errors ────────────────────────────────
    #[error("failed to publish comment event to Kafka: {message}")]
    EventPublishFailed { message: String },

    // ── CMT-9xxx: ID parsing / generic domain violations ──────────────────────
    #[error("invalid comment ID: '{0}'")]
    InvalidCommentId(String),

    #[error("invalid post ID: '{0}'")]
    InvalidPostId(String),

    #[error("invalid profile ID: '{0}'")]
    InvalidProfileId(String),

    #[error("domain violation on field '{field}': {message}")]
    DomainViolation { field: String, message: String },

    // ── CMT-5xxx: dependencies ────────────────────────────────────────────────
    /// The read check (post lookup or social-graph `CheckAccess`) could not
    /// answer. Reads fail closed rather than risk showing hidden content.
    #[error("audience check unavailable: {reason}")]
    AccessCheckUnavailable { reason: String },
}

impl AppError for CommentError {
    fn error_code(&self) -> &'static str {
        match self {
            Self::Storage(e)    => e.error_code(),
            Self::Validation(e) => e.error_code(),

            Self::CommentNotFound { .. }      => "CMT-1001",
            Self::CommentAlreadyDeleted { .. } => "CMT-1002",
            Self::AuthorMismatch { .. }        => "CMT-1003",
            Self::PostNotFound { .. }          => "CMT-1004",
            Self::CommentsRestricted { .. }    => "CMT-1005",

            Self::NestingDepthExceeded        => "CMT-2001",
            Self::ParentNotFound { .. }        => "CMT-2002",
            Self::ParentDeleted { .. }         => "CMT-2003",

            Self::EmptyContent                 => "CMT-3001",
            Self::IncompleteGifMetadata        => "CMT-3002",

            Self::EventPublishFailed { .. }    => "CMT-4001",

            Self::InvalidCommentId(_)          => "CMT-9001",
            Self::InvalidPostId(_)             => "CMT-9002",
            Self::InvalidProfileId(_)          => "CMT-9003",
            Self::DomainViolation { .. }       => "CMT-9004",
            Self::AccessCheckUnavailable { .. } => "CMT-5001",
        }
    }

    fn http_status(&self) -> StatusCode {
        match self {
            Self::Storage(e)    => e.http_status(),
            Self::Validation(e) => e.http_status(),

            Self::CommentNotFound { .. }
            | Self::ParentNotFound { .. }
            | Self::PostNotFound { .. }        => StatusCode::NOT_FOUND,

            Self::AuthorMismatch { .. }
            | Self::CommentsRestricted { .. }  => StatusCode::FORBIDDEN,

            Self::CommentAlreadyDeleted { .. } => StatusCode::CONFLICT,

            Self::NestingDepthExceeded
            | Self::ParentDeleted { .. }
            | Self::EmptyContent
            | Self::IncompleteGifMetadata
            | Self::InvalidCommentId(_)
            | Self::InvalidPostId(_)
            | Self::InvalidProfileId(_)
            | Self::DomainViolation { .. }     => StatusCode::UNPROCESSABLE_ENTITY,

            Self::EventPublishFailed { .. }    => StatusCode::INTERNAL_SERVER_ERROR,
            Self::AccessCheckUnavailable { .. } => StatusCode::SERVICE_UNAVAILABLE,
        }
    }

    fn severity(&self) -> Severity {
        match self {
            Self::Storage(e)                   => e.severity(),
            Self::Validation(e)                => e.severity(),
            Self::EventPublishFailed { .. }    => Severity::High,
            Self::AuthorMismatch { .. }        => Severity::Medium,
            Self::DomainViolation { .. }       => Severity::Medium,
            Self::AccessCheckUnavailable { .. } => Severity::Medium,
            _                                  => Severity::Low,
        }
    }

    fn is_retryable(&self) -> bool {
        match self {
            Self::Storage(e) => e.is_retryable(),
            Self::AccessCheckUnavailable { .. } => true,
            _                => false,
        }
    }

    fn category(&self) -> &'static str {
        match self {
            Self::Storage(e)    => e.category(),
            Self::Validation(e) => e.category(),
            Self::AuthorMismatch { .. }        => "authorization",
            Self::CommentNotFound { .. }
            | Self::ParentNotFound { .. }      => "not_found",
            Self::CommentAlreadyDeleted { .. } => "lifecycle",
            Self::NestingDepthExceeded
            | Self::ParentDeleted { .. }       => "threading",
            Self::EmptyContent
            | Self::IncompleteGifMetadata      => "content",
            Self::EventPublishFailed { .. }    => "kafka",
            Self::AccessCheckUnavailable { .. } => "dependency",
            _                                  => "CMT",
        }
    }

    fn user_facing_message(&self) -> &'static str {
        match self {
            Self::Storage(_)
            | Self::EventPublishFailed { .. }  =>
                "An internal error occurred. Please try again later.",
            Self::CommentNotFound { .. }       => "The requested comment was not found.",
            Self::CommentAlreadyDeleted { .. } => "This comment has already been deleted.",
            Self::AuthorMismatch { .. }        => "You are not authorised to delete this comment.",
            Self::PostNotFound { .. }          => "This post was not found.",
            Self::CommentsRestricted { .. }    => "The author has limited who can comment on this post.",
            Self::NestingDepthExceeded         => "Replies to replies are not supported.",
            Self::ParentNotFound { .. }        => "The parent comment was not found.",
            Self::ParentDeleted { .. }         => "Cannot reply to a deleted comment.",
            Self::EmptyContent                 => "A comment must contain text, a GIF, or both.",
            Self::IncompleteGifMetadata        =>
                "GIF metadata is incomplete — provide gif_id, gif_url, width, and height.",
            Self::InvalidCommentId(_)          => "The provided comment ID is not valid.",
            Self::InvalidPostId(_)             => "The provided post ID is not valid.",
            Self::InvalidProfileId(_)          => "The provided profile ID is not valid.",
            Self::DomainViolation { .. }       => "A domain constraint was violated.",
            Self::AccessCheckUnavailable { .. } =>
                "These comments are temporarily unavailable. Please try again later.",
            Self::Validation(e)                => e.user_facing_message(),
        }
    }
}
