use scylla::value::CqlTimestamp;
use scylla::DeserializeRow;
use uuid::Uuid;

/// Positional deserialization for `post.posts_by_profile`.
///
/// SELECT must emit columns in exactly this order:
/// created_at, post_id, kind, status, moderation_restriction
#[derive(DeserializeRow)]
#[scylla(flavor = "enforce_order")]
pub struct PostProfileRow {
    pub created_at: CqlTimestamp,
    pub post_id:    Uuid,
    pub kind:       i8,
    pub status:     i8,
    /// NULL until moderation first acts on the post (migration 0007).
    pub moderation_restriction: Option<i8>,
    /// NULL for posts written before migration 0014: read as false.
    pub is_repost: Option<bool>,
    pub has_place: Option<bool>,
}
