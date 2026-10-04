use error::{AppError, Severity};
use http::StatusCode;
use thiserror::Error;

#[derive(Debug, Error)]
#[non_exhaustive]
pub enum PostError {
    #[error(transparent)]
    Storage(#[from] scylla_storage::ScyllaStorageError),

    #[error(transparent)]
    Validation(#[from] validation::ValidationError),

    #[error("post not found: {post_id}")]
    PostNotFound { post_id: String },

    #[error("post {post_id} is already published")]
    PostAlreadyPublished { post_id: String },

    #[error("post {post_id} is already deleted")]
    PostAlreadyDeleted { post_id: String },

    #[error("post {post_id} is not in Draft status (current: {current_status})")]
    NotDraft { post_id: String, current_status: String },

    #[error("caller {caller_id} is not the author of post {post_id}")]
    AuthorMismatch { post_id: String, caller_id: String },

    #[error("carousel requires at least 2 items")]
    CarouselTooFewItems,

    #[error("carousel exceeds maximum of 10 items (got {count})")]
    CarouselTooManyItems { count: usize },

    #[error("carousel video at index {index} exceeds 15 s (got {duration:.1} s)")]
    CarouselVideoTooLong { index: usize, duration: f32 },

    #[error("video at index {index} is missing a thumbnail_url")]
    MissingVideoThumbnail { index: usize },

    #[error("unsupported MIME type: {mime_type}")]
    InvalidMimeType { mime_type: String },

    #[error("invalid CDN URL: {url}")]
    InvalidCdnUrl { url: String },

    #[error("attachment at index {index} has invalid dimensions ({width}x{height})")]
    InvalidDimensions { index: usize, width: u32, height: u32 },

    #[error("invalid post ID: {0}")]
    InvalidPostId(String),

    #[error("invalid profile ID: {0}")]
    InvalidProfileId(String),

    #[error("invalid audio ID: {0}")]
    InvalidAudioId(String),

    #[error("attachments JSON corrupted for post {post_id}: {reason}")]
    AttachmentsCorrupted { post_id: String, reason: String },

    #[error("domain violation on field '{field}': {message}")]
    DomainViolation { field: String, message: String },

    /// The audience check (social-graph `CheckAccess`) could not answer. Reads
    /// fail closed rather than risk showing a private or blocked author's post.
    #[error("audience check unavailable: {reason}")]
    AccessCheckUnavailable { reason: String },
}

impl AppError for PostError {
    fn error_code(&self) -> &'static str {
        match self {
            Self::Storage(e)    => e.error_code(),
            Self::Validation(e) => e.error_code(),
            Self::PostNotFound { .. }         => "PST-1001",
            Self::PostAlreadyPublished { .. } => "PST-1002",
            Self::PostAlreadyDeleted { .. }   => "PST-1003",
            Self::NotDraft { .. }             => "PST-1004",
            Self::AuthorMismatch { .. }       => "PST-1005",
            Self::CarouselTooFewItems         => "PST-2001",
            Self::CarouselTooManyItems { .. } => "PST-2002",
            Self::CarouselVideoTooLong { .. } => "PST-2003",
            Self::MissingVideoThumbnail { .. } => "PST-3001",
            Self::InvalidMimeType { .. }      => "PST-3002",
            Self::InvalidCdnUrl { .. }        => "PST-3003",
            Self::InvalidDimensions { .. }    => "PST-3004",
            Self::InvalidPostId(_)            => "PST-9001",
            Self::InvalidProfileId(_)         => "PST-9002",
            Self::AttachmentsCorrupted { .. } => "PST-9003",
            Self::DomainViolation { .. }      => "PST-9004",
            Self::InvalidAudioId(_)           => "PST-9005",
            Self::AccessCheckUnavailable { .. } => "PST-5001",
        }
    }

    fn http_status(&self) -> StatusCode {
        match self {
            Self::Storage(e)    => e.http_status(),
            Self::Validation(e) => e.http_status(),
            Self::PostNotFound { .. }         => StatusCode::NOT_FOUND,
            Self::PostAlreadyPublished { .. }
            | Self::PostAlreadyDeleted { .. } => StatusCode::CONFLICT,
            Self::AuthorMismatch { .. }       => StatusCode::FORBIDDEN,
            Self::NotDraft { .. }
            | Self::CarouselTooFewItems
            | Self::CarouselTooManyItems { .. }
            | Self::CarouselVideoTooLong { .. }
            | Self::MissingVideoThumbnail { .. }
            | Self::InvalidMimeType { .. }
            | Self::InvalidCdnUrl { .. }
            | Self::InvalidDimensions { .. }
            | Self::InvalidPostId(_)
            | Self::InvalidProfileId(_)
            | Self::InvalidAudioId(_)
            | Self::DomainViolation { .. }    => StatusCode::UNPROCESSABLE_ENTITY,
            Self::AttachmentsCorrupted { .. } => StatusCode::INTERNAL_SERVER_ERROR,
            Self::AccessCheckUnavailable { .. } => StatusCode::SERVICE_UNAVAILABLE,
        }
    }

    fn severity(&self) -> Severity {
        match self {
            Self::Storage(e)    => e.severity(),
            Self::Validation(e) => e.severity(),
            Self::AttachmentsCorrupted { .. } => Severity::High,
            Self::AuthorMismatch { .. }
            | Self::DomainViolation { .. }
            | Self::AccessCheckUnavailable { .. } => Severity::Medium,
            _                                 => Severity::Low,
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
            Self::AuthorMismatch { .. }         => "authorization",
            Self::PostNotFound { .. }           => "not_found",
            Self::PostAlreadyPublished { .. }
            | Self::PostAlreadyDeleted { .. }
            | Self::NotDraft { .. }             => "lifecycle",
            Self::CarouselTooFewItems
            | Self::CarouselTooManyItems { .. }
            | Self::CarouselVideoTooLong { .. } => "carousel",
            Self::MissingVideoThumbnail { .. }
            | Self::InvalidMimeType { .. }
            | Self::InvalidCdnUrl { .. }
            | Self::InvalidDimensions { .. }    => "attachment",
            Self::AccessCheckUnavailable { .. } => "dependency",
            _                                   => "PST",
        }
    }

    fn user_facing_message(&self) -> &'static str {
        match self {
            Self::Storage(_)
            | Self::AttachmentsCorrupted { .. } => "An internal error occurred. Please try again later.",
            Self::AccessCheckUnavailable { .. } => "This content is temporarily unavailable. Please try again later.",
            Self::PostNotFound { .. }            => "The requested post was not found.",
            Self::PostAlreadyPublished { .. }    => "This post has already been published.",
            Self::PostAlreadyDeleted { .. }      => "This post has already been deleted.",
            Self::NotDraft { .. }               => "Only draft posts can be published.",
            Self::AuthorMismatch { .. }         => "You are not authorised to modify this post.",
            Self::CarouselTooFewItems           => "A carousel must contain at least 2 items.",
            Self::CarouselTooManyItems { .. }   => "A carousel can contain at most 10 items.",
            Self::CarouselVideoTooLong { .. }   => "A carousel video must not exceed 15 seconds.",
            Self::MissingVideoThumbnail { .. }  => "Video attachments require a thumbnail URL.",
            Self::InvalidMimeType { .. }        => "The provided MIME type is not supported.",
            Self::InvalidCdnUrl { .. }          => "The provided URL is not a valid HTTPS CDN URL.",
            Self::InvalidDimensions { .. }      => "Attachment dimensions must be greater than zero.",
            Self::InvalidPostId(_)              => "The provided post ID is not valid.",
            Self::InvalidProfileId(_)           => "The provided profile ID is not valid.",
            Self::InvalidAudioId(_)             => "The provided audio ID is not valid.",
            Self::DomainViolation { .. }        => "A domain constraint was violated.",
            Self::Validation(e)                 => e.user_facing_message(),
        }
    }
}
