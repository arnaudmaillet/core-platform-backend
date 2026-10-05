use chrono::{DateTime, Utc};

use crate::domain::value_object::{
    ContentType, ConversationId, MessageContent, MessageId, ProfileId,
};
use crate::error::ChatError;

/// A single chat message — an immutable, append-only record in the message log.
///
/// Messages are modelled as their own small aggregate rather than as children of
/// [`Conversation`](super::Conversation): the log is a high-volume stream that
/// must never be hydrated into the conversation aggregate. A message is created
/// once and never mutated, so it buffers no events; the `SendMessage` handler
/// emits the [`MessageSentEvent`](crate::domain::event::MessageSentEvent) after a
/// durable write.
pub struct Message {
    id:              MessageId,
    conversation_id: ConversationId,
    sender_id:       ProfileId,
    content_type:    ContentType,
    content:         MessageContent,
    /// Out-of-band pointer for `Media` messages; large media is never inlined.
    media_ref:       Option<String>,
    /// The message this one replies to, if any (threaded replies).
    reply_to:        Option<MessageId>,
    /// Shown to its sender only (#656): its recipient blocks them, or they
    /// block the recipient. The sender sees it sent, as any other.
    withheld:        bool,
    created_at:      DateTime<Utc>,
}

impl Message {
    /// Creates a new message, enforcing content-type invariants:
    /// - `Text`  — body must be non-empty;
    /// - `Media` — a `media_ref` must be present (caption body may be empty);
    /// - `System`— no constraint (service-controlled).
    pub fn create(
        id:              MessageId,
        conversation_id: ConversationId,
        sender_id:       ProfileId,
        content_type:    ContentType,
        content:         MessageContent,
        media_ref:       Option<String>,
        reply_to:        Option<MessageId>,
    ) -> Result<Self, ChatError> {
        match content_type {
            ContentType::Text => {
                if content.is_empty() {
                    return Err(ChatError::EmptyMessage);
                }
            }
            ContentType::Media => {
                if media_ref.as_ref().map(|r| r.trim().is_empty()).unwrap_or(true) {
                    return Err(ChatError::MediaReferenceMissing);
                }
            }
            ContentType::System => {}
        }

        Ok(Self {
            id,
            conversation_id,
            sender_id,
            content_type,
            content,
            media_ref,
            reply_to,
            withheld: false,
            created_at: Utc::now(),
        })
    }

    /// Withholds the message from everyone but its sender (#656).
    pub fn withhold(&mut self) {
        self.withheld = true;
    }

    /// Reconstitutes a message from a persisted row.
    #[allow(clippy::too_many_arguments)]
    pub fn reconstitute(
        id:              MessageId,
        conversation_id: ConversationId,
        sender_id:       ProfileId,
        content_type:    ContentType,
        content:         MessageContent,
        media_ref:       Option<String>,
        reply_to:        Option<MessageId>,
        created_at:      DateTime<Utc>,
    ) -> Self {
        Self {
            id,
            conversation_id,
            sender_id,
            content_type,
            content,
            media_ref,
            reply_to,
            withheld: false,
            created_at,
        }
    }

    pub fn id(&self)              -> MessageId        { self.id }
    pub fn conversation_id(&self) -> ConversationId   { self.conversation_id }
    pub fn sender_id(&self)       -> ProfileId        { self.sender_id }
    pub fn content_type(&self)    -> ContentType      { self.content_type }
    pub fn content(&self)         -> &MessageContent  { &self.content }
    pub fn media_ref(&self)       -> Option<&str>     { self.media_ref.as_deref() }
    pub fn reply_to(&self)        -> Option<MessageId> { self.reply_to }
    pub fn withheld(&self)        -> bool             { self.withheld }
    pub fn created_at(&self)      -> DateTime<Utc>    { self.created_at }
}
