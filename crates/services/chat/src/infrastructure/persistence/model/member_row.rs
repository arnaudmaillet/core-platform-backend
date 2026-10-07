use scylla::DeserializeRow;
use scylla::value::CqlTimestamp;
use uuid::Uuid;

/// ScyllaDB row type for `chat.members_by_conversation`.
///
/// Column order MUST match the SELECT column list exactly due to
/// `scylla(flavor = "enforce_order")`.
#[derive(Debug, DeserializeRow)]
#[scylla(flavor = "enforce_order")]
pub struct MemberRow {
    pub member_id: Uuid,
    pub role:      i8,
    pub joined_at: CqlTimestamp,
    pub last_read: Option<Uuid>,
    pub muted_until: Option<CqlTimestamp>,
}
