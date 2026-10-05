use async_trait::async_trait;
use chrono::{DateTime, Utc};
use uuid::Uuid;

use crate::domain::aggregate::Message;
use crate::domain::value_object::{ContentType, ConversationId};
use crate::error::ChatError;

/// Read projection of a message row, returned by history queries. A flat DTO of
/// scalars so it is cheap to map into the gRPC view and the Redis hot-tail cache.
#[derive(Debug, Clone)]
pub struct MessageSummary {
    pub message_id:   Uuid,
    pub sender_id:    Uuid,
    pub content_type: ContentType,
    pub body:         String,
    pub media_ref:    Option<String>,
    pub reply_to:     Option<Uuid>,
    pub created_at:   DateTime<Utc>,
    /// Shown to its sender only (#656).
    pub withheld:     bool,
}

impl MessageSummary {
    /// May `reader` see it? A withheld message is its sender's alone.
    pub fn visible_to(&self, reader: Uuid) -> bool {
        !self.withheld || self.sender_id == reader
    }

    /// Another member's message as the GDPR export shows it: when, not who
    /// nor what (the sender nil, no content).
    pub fn reduced_to_its_time(self) -> Self {
        Self {
            sender_id: Uuid::nil(),
            body: String::new(),
            media_ref: None,
            reply_to: None,
            ..self
        }
    }
}

/// Persistence port for the time-bucketed message log
/// (`chat.messages_by_conversation`).
#[async_trait]
pub trait MessageRepository: Send + Sync + 'static {
    /// Durably appends a message to its conversation's current time bucket.
    async fn insert(&self, message: &Message) -> Result<(), ChatError>;

    /// Reads one page of history, newest-first, walking time buckets as needed
    /// to fill `limit`.
    ///
    /// - `cursor` is `(created_at_ms, message_id)` from the last row of the
    ///   previous page; `None` starts at the live tail.
    /// - `floor_created_at_ms` is the Audience-Plane visibility floor (the
    ///   public-since watermark in epoch ms): when set, the query adds a
    ///   server-side `created_at >= ?` predicate and the bucket walk stops at the
    ///   watermark bucket. Pass `None` for members (full history).
    ///
    /// Returns `(page, next_cursor)`; `next_cursor` is `None` when the history is
    /// exhausted within the walk bound.
    async fn list_history(
        &self,
        conversation_id:      &ConversationId,
        limit:                i32,
        cursor:               Option<(i64, Uuid)>,
        floor_created_at_ms:  Option<i64>,
    ) -> Result<(Vec<MessageSummary>, Option<(i64, Uuid)>), ChatError>;
}
