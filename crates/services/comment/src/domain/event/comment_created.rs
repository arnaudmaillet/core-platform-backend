use serde::{Deserialize, Serialize};

/// Emitted when a comment is successfully persisted.
///
/// Published to Kafka topic `comment.created` (key: `comment_id`).
/// The engagement service's CommentEventConsumer reads `post_id` from this
/// payload to increment its Redis and ScyllaDB counters atomically.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CommentCreatedEvent {
    pub comment_id:    String,
    pub post_id:       String,
    pub author_id:     String,
    /// None when the comment is top-level.
    pub parent_id:     Option<String>,
    pub created_at_ms: i64,
    /// Announced without notifying anyone: the post's owner restricted the
    /// author (#659), so the comment is seen only by its author and the owner.
    /// Absent from events published before the field existed (`false`).
    #[serde(default)]
    pub quiet:         bool,
}
