use scylla::DeserializeRow;
use scylla::value::CqlTimestamp;
use uuid::Uuid;

/// ScyllaDB row type for `chat.conversations`.
///
/// Column order MUST match the SELECT column list exactly due to
/// `scylla(flavor = "enforce_order")` — ScyllaDB 1.x deserializes by position.
#[derive(Debug, DeserializeRow)]
#[scylla(flavor = "enforce_order")]
pub struct ConversationRow {
    pub conversation_id: Uuid,
    pub kind:            i8,
    pub visibility:      i8,
    pub owner_id:        Uuid,
    pub member_count:    i32,
    pub public_since:    Option<Uuid>,
    pub created_at:      CqlTimestamp,
    pub updated_at:      CqlTimestamp,
    pub peer_id:         Option<Uuid>,
    pub request_state:   Option<i8>,
    pub requester_id:    Option<Uuid>,
    pub declined_at:     Option<CqlTimestamp>,
}
