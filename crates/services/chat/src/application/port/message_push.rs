//! A message's push (#654), asked of the notification service: who gets it and
//! what it shows. chat decides who (its inbox rules and mutes); notification
//! applies each recipient's preferences and sends.

use async_trait::async_trait;
use serde::{Deserialize, Serialize};

use crate::error::ChatError;

/// One message's push, on `chat.message.push` (keyed by conversation).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MessagePush {
    pub message_id:        String,
    pub conversation_id:   String,
    /// `direct`, `group` or `channel`.
    pub conversation_kind: String,
    pub sender_id:         String,
    /// `text` or `media` (system messages are never pushed).
    pub content_type:      String,
    /// The body's first [`PREVIEW_CHARS`](crate::application::port::PREVIEW_CHARS)
    /// characters (empty for media).
    pub preview:           String,
    /// The members it goes to: they would see it in their inbox (not its
    /// sender, not a request's recipient, not someone blocking the sender)
    /// and have not muted the conversation.
    pub recipients:        Vec<String>,
    pub created_at_ms:     i64,
}

#[async_trait]
pub trait MessagePushes: Send + Sync + 'static {
    async fn request(&self, push: &MessagePush) -> Result<(), ChatError>;
}

/// No broker: no push.
pub struct NoMessagePushes;

#[async_trait]
impl MessagePushes for NoMessagePushes {
    async fn request(&self, _push: &MessagePush) -> Result<(), ChatError> {
        Ok(())
    }
}
