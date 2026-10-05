use scylla::DeserializeRow;
use scylla::value::CqlTimestamp;
use uuid::Uuid;

/// ScyllaDB row type for `chat.messages_by_conversation` history reads.
///
/// Column order MUST match the SELECT column list exactly due to
/// `scylla(flavor = "enforce_order")`. `conversation_id` and `bucket` are not
/// selected — the caller already holds them.
#[derive(Debug, DeserializeRow)]
#[scylla(flavor = "enforce_order")]
pub struct MessageRow {
    pub created_at:   CqlTimestamp,
    pub message_id:   Uuid,
    pub sender_id:    Uuid,
    pub content_type: i8,
    pub body:         Option<String>,
    pub media_ref:    Option<String>,
    pub reply_to:     Option<Uuid>,
    pub withheld:     Option<bool>,
}
