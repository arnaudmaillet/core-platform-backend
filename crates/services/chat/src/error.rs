use error::{AppError, Severity};
use http::StatusCode;
use thiserror::Error;

/// Service-wide error contract for the chat microservice.
///
/// Storage and validation failures are wrapped transparently so their original
/// error codes propagate; every domain- and application-level fault carries a
/// stable `CHT-xxxx` code (see [`AppError::error_code`]). The blocks are grouped:
/// `1xxx` conversation lifecycle, `2xxx` domain validation, `3xxx` events,
/// `4xxx` real-time streaming/routing, `9xxx` identifier parsing.
#[derive(Debug, Error)]
#[non_exhaustive]
pub enum ChatError {
    #[error(transparent)]
    Scylla(#[from] scylla_storage::ScyllaStorageError),

    #[error(transparent)]
    Redis(#[from] redis_storage::RedisStorageError),

    #[error(transparent)]
    Validation(#[from] validation::ValidationError),

    // ── CHT-1xxx: Conversation lifecycle ──────────────────────────────────────
    #[error("conversation {conversation_id} not found")]
    ConversationNotFound { conversation_id: String },

    #[error("conversation {conversation_id} is already public")]
    ConversationAlreadyPublic { conversation_id: String },

    #[error("conversation {conversation_id} is already private")]
    ConversationAlreadyPrivate { conversation_id: String },

    #[error("profile {profile_id} is not authorized to administer conversation {conversation_id}")]
    NotAuthorized { profile_id: String, conversation_id: String },

    #[error("conversation {conversation_id} has reached its member limit of {limit}")]
    MemberLimitExceeded { conversation_id: String, limit: u16 },

    #[error("profile {profile_id} is already a member of conversation {conversation_id}")]
    AlreadyMember { profile_id: String, conversation_id: String },

    #[error("profile {profile_id} is not a member of conversation {conversation_id}")]
    NotAMember { profile_id: String, conversation_id: String },

    #[error("conversation {conversation_id} is not public")]
    ConversationNotPublic { conversation_id: String },

    /// A private conversation the caller may not see (no membership, no
    /// invitation). Its `Display`, HTTP status and user-facing message are
    /// **deliberately identical** to [`ChatError::ConversationNotFound`], so the
    /// edge cannot be used to probe whether a private conversation exists; only
    /// the code (`CHT-1009`, logged server-side) tells the two apart.
    #[error("conversation {conversation_id} not found")]
    ConversationConcealed { conversation_id: String },

    /// A message request already holds its one message (#656). A declined
    /// request, and one whose recipient blocks the sender, answer **exactly
    /// the same**: the sender never learns which.
    #[error("the message request in conversation {conversation_id} awaits an answer")]
    MessageRequestPending { conversation_id: String },

    /// The recipient takes messages from no one (#656). Never the answer to a
    /// block: a blocked sender sees a pending request instead.
    #[error("profile {profile_id} does not accept messages")]
    MessagingNotAllowed { profile_id: String },

    /// Accept or decline with no request from someone else to answer (#656).
    #[error("no message request to answer in conversation {conversation_id}")]
    NoMessageRequest { conversation_id: String },

    // ── CHT-5xxx: Peer services ───────────────────────────────────────────────
    /// social-graph, which decides who may message whom, did not answer:
    /// direct messages and invitations fail closed until it does (#656).
    #[error("interaction check unavailable: {reason}")]
    InteractionCheckUnavailable { reason: String },

    // ── CHT-2xxx: Domain validation ───────────────────────────────────────────
    #[error("unknown conversation kind: '{kind}'")]
    UnknownConversationKind { kind: String },

    #[error("unknown visibility: '{visibility}'")]
    UnknownVisibility { visibility: String },

    #[error("unknown role: '{role}'")]
    UnknownRole { role: String },

    #[error("unknown content type: '{content_type}'")]
    UnknownContentType { content_type: String },

    #[error("message body must not be empty for a text message")]
    EmptyMessage,

    #[error("message body must be at most {max} characters (got {got})")]
    MessageTooLong { max: usize, got: usize },

    #[error("a media message requires a media reference")]
    MediaReferenceMissing,

    #[error("role '{role}' belongs to the audience plane and cannot be a participant")]
    InvalidParticipantRole { role: String },

    #[error("invalid page token: '{token}'")]
    InvalidPageToken { token: String },

    // ── CHT-3xxx: Event / Kafka errors ────────────────────────────────────────
    #[error("failed to publish chat event to Kafka: {message}")]
    EventPublishFailed { message: String },

    // ── CHT-4xxx: Real-time streaming / routing errors ────────────────────────
    #[error("stream registry send failed for conversation {conversation_id}: channel closed")]
    StreamSendFailed { conversation_id: String },

    // ── CHT-9xxx: Identifier parsing / domain violations ──────────────────────
    #[error("invalid conversation ID: '{0}'")]
    InvalidConversationId(String),

    #[error("invalid message ID: '{0}'")]
    InvalidMessageId(String),

    #[error("invalid profile ID: '{0}'")]
    InvalidProfileId(String),

    #[error("domain violation on field '{field}': {message}")]
    DomainViolation { field: String, message: String },
}

impl AppError for ChatError {
    fn error_code(&self) -> &'static str {
        match self {
            Self::Scylla(e)     => e.error_code(),
            Self::Redis(e)      => e.error_code(),
            Self::Validation(e) => e.error_code(),

            Self::ConversationNotFound { .. }       => "CHT-1001",
            Self::ConversationAlreadyPublic { .. }  => "CHT-1002",
            Self::ConversationAlreadyPrivate { .. } => "CHT-1003",
            Self::NotAuthorized { .. }              => "CHT-1004",
            Self::MemberLimitExceeded { .. }        => "CHT-1005",
            Self::AlreadyMember { .. }              => "CHT-1006",
            Self::NotAMember { .. }                 => "CHT-1007",
            Self::ConversationNotPublic { .. }      => "CHT-1008",
            Self::ConversationConcealed { .. }      => "CHT-1009",
            Self::MessageRequestPending { .. }      => "CHT-1010",
            Self::MessagingNotAllowed { .. }        => "CHT-1011",
            Self::NoMessageRequest { .. }           => "CHT-1012",

            Self::InteractionCheckUnavailable { .. } => "CHT-5001",

            Self::UnknownConversationKind { .. } => "CHT-2001",
            Self::UnknownVisibility { .. }       => "CHT-2002",
            Self::UnknownRole { .. }             => "CHT-2003",
            Self::UnknownContentType { .. }      => "CHT-2004",
            Self::EmptyMessage                   => "CHT-2005",
            Self::MessageTooLong { .. }          => "CHT-2006",
            Self::MediaReferenceMissing          => "CHT-2007",
            Self::InvalidParticipantRole { .. }  => "CHT-2008",
            Self::InvalidPageToken { .. }        => "CHT-2009",

            Self::EventPublishFailed { .. } => "CHT-3001",

            Self::StreamSendFailed { .. } => "CHT-4001",

            Self::InvalidConversationId(_) => "CHT-9001",
            Self::InvalidMessageId(_)      => "CHT-9002",
            Self::InvalidProfileId(_)      => "CHT-9003",
            Self::DomainViolation { .. }   => "CHT-9004",
        }
    }

    fn http_status(&self) -> StatusCode {
        match self {
            Self::Scylla(e)     => e.http_status(),
            Self::Redis(e)      => e.http_status(),
            Self::Validation(e) => e.http_status(),

            Self::ConversationNotFound { .. }
            | Self::ConversationConcealed { .. } => StatusCode::NOT_FOUND,

            Self::ConversationAlreadyPublic { .. }
            | Self::ConversationAlreadyPrivate { .. }
            | Self::AlreadyMember { .. } => StatusCode::CONFLICT,

            Self::NotAuthorized { .. } => StatusCode::FORBIDDEN,

            Self::MemberLimitExceeded { .. }
            | Self::ConversationNotPublic { .. } => StatusCode::UNPROCESSABLE_ENTITY,

            Self::NotAMember { .. }
            | Self::MessagingNotAllowed { .. } => StatusCode::FORBIDDEN,

            Self::MessageRequestPending { .. }
            | Self::NoMessageRequest { .. } => StatusCode::UNPROCESSABLE_ENTITY,

            Self::InteractionCheckUnavailable { .. } => StatusCode::SERVICE_UNAVAILABLE,

            Self::UnknownConversationKind { .. }
            | Self::UnknownVisibility { .. }
            | Self::UnknownRole { .. }
            | Self::UnknownContentType { .. }
            | Self::EmptyMessage
            | Self::MessageTooLong { .. }
            | Self::MediaReferenceMissing
            | Self::InvalidParticipantRole { .. }
            | Self::InvalidPageToken { .. }
            | Self::InvalidConversationId(_)
            | Self::InvalidMessageId(_)
            | Self::InvalidProfileId(_)
            | Self::DomainViolation { .. } => StatusCode::UNPROCESSABLE_ENTITY,

            Self::EventPublishFailed { .. }
            | Self::StreamSendFailed { .. } => StatusCode::INTERNAL_SERVER_ERROR,
        }
    }

    fn severity(&self) -> Severity {
        match self {
            Self::Scylla(e) => e.severity(),
            Self::Redis(e)  => e.severity(),

            Self::EventPublishFailed { .. } => Severity::High,

            Self::StreamSendFailed { .. }
            | Self::InteractionCheckUnavailable { .. } => Severity::Medium,

            Self::Validation(e) => e.severity(),

            Self::UnknownConversationKind { .. }
            | Self::UnknownVisibility { .. }
            | Self::UnknownRole { .. }
            | Self::UnknownContentType { .. }
            | Self::EmptyMessage
            | Self::MessageTooLong { .. }
            | Self::MediaReferenceMissing
            | Self::InvalidParticipantRole { .. }
            | Self::InvalidPageToken { .. }
            | Self::MemberLimitExceeded { .. }
            | Self::DomainViolation { .. } => Severity::Medium,

            Self::ConversationNotFound { .. }
            | Self::ConversationConcealed { .. }
            | Self::ConversationAlreadyPublic { .. }
            | Self::ConversationAlreadyPrivate { .. }
            | Self::NotAuthorized { .. }
            | Self::AlreadyMember { .. }
            | Self::NotAMember { .. }
            | Self::ConversationNotPublic { .. }
            | Self::MessageRequestPending { .. }
            | Self::MessagingNotAllowed { .. }
            | Self::NoMessageRequest { .. }
            | Self::InvalidConversationId(_)
            | Self::InvalidMessageId(_)
            | Self::InvalidProfileId(_) => Severity::Low,
        }
    }

    fn is_retryable(&self) -> bool {
        match self {
            Self::Scylla(e) => e.is_retryable(),
            Self::Redis(e)  => e.is_retryable(),
            Self::InteractionCheckUnavailable { .. } => true,
            _               => false,
        }
    }

    fn category(&self) -> &'static str {
        match self {
            Self::Scylla(e)     => e.category(),
            Self::Redis(e)      => e.category(),
            Self::Validation(e) => e.category(),
            _                   => "CHT",
        }
    }

    fn user_facing_message(&self) -> &'static str {
        match self {
            Self::Scylla(_)
            | Self::Redis(_)
            | Self::EventPublishFailed { .. }
            | Self::StreamSendFailed { .. } =>
                "An internal error occurred. Please try again later.",

            Self::ConversationNotFound { .. } | Self::ConversationConcealed { .. } =>
                "The conversation was not found.",

            Self::ConversationAlreadyPublic { .. } =>
                "The conversation is already public.",

            Self::ConversationAlreadyPrivate { .. } =>
                "The conversation is already private.",

            Self::NotAuthorized { .. } =>
                "You are not allowed to perform this action.",

            Self::MemberLimitExceeded { .. } =>
                "The conversation has reached its maximum number of members.",

            Self::AlreadyMember { .. } =>
                "You are already a member of this conversation.",

            Self::NotAMember { .. } =>
                "You are not a member of this conversation.",

            Self::ConversationNotPublic { .. } =>
                "This conversation is not public.",

            Self::MessageRequestPending { .. } =>
                "Your message request is waiting for an answer.",

            Self::MessagingNotAllowed { .. } =>
                "This account doesn't accept messages.",

            Self::NoMessageRequest { .. } =>
                "There is no message request to answer.",

            Self::InteractionCheckUnavailable { .. } =>
                "Messages are temporarily unavailable. Please try again later.",

            Self::UnknownConversationKind { .. }
            | Self::UnknownVisibility { .. }
            | Self::UnknownRole { .. }
            | Self::UnknownContentType { .. } =>
                "The request type is not supported.",

            Self::EmptyMessage =>
                "The message cannot be empty.",

            Self::MessageTooLong { .. } =>
                "The message is too long.",

            Self::MediaReferenceMissing =>
                "The media attachment is missing.",

            Self::InvalidParticipantRole { .. } =>
                "The requested role is not valid for this action.",

            Self::InvalidPageToken { .. } =>
                "The pagination token is invalid or expired.",

            Self::InvalidConversationId(_) => "The conversation ID is not valid.",
            Self::InvalidMessageId(_)      => "The message ID is not valid.",
            Self::InvalidProfileId(_)      => "The profile ID is not valid.",

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
impl transport::kafka::consumer::ClassifyError for ChatError {
    fn is_retryable(&self) -> bool {
        <Self as AppError>::is_retryable(self)
    }
}
