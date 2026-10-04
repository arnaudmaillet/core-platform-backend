use scylla::value::CqlTimestamp;
use scylla::DeserializeRow;
use uuid::Uuid;

/// Positional deserialization for `comment.comments_by_post`.
///
/// SELECT must emit columns in exactly this order:
/// created_at, comment_id, author_id, status, body,
/// gif_url, gif_width, gif_height, held
///
/// `parent_id` and `post_id` are partition/clustering keys consumed as
/// query parameters — they are not included in the SELECT column list.
#[derive(DeserializeRow)]
#[scylla(flavor = "enforce_order")]
pub struct CommentFeedRow {
    pub created_at: CqlTimestamp,
    pub comment_id: Uuid,
    pub author_id:  Uuid,
    pub status:     i8,
    pub body:       Option<String>,
    pub gif_url:    Option<String>,
    pub gif_width:  Option<i32>,
    pub gif_height: Option<i32>,
    /// NULL ⇒ not held (#669).
    pub held:       Option<bool>,
}
