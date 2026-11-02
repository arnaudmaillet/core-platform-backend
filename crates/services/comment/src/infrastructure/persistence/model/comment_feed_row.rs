use scylla::value::CqlTimestamp;
use scylla::DeserializeRow;
use uuid::Uuid;

/// Positional deserialization for `comment.comments_by_post` and
/// `comment.comments_timeline` (#988), which share these columns.
///
/// SELECT must emit columns in exactly this order:
/// created_at, comment_id, author_id, status, body,
/// gif_url, gif_width, gif_height, held, parent_id
///
/// `post_id` is the partition key, consumed as a query parameter.
///
/// `author_id`, `status` and `parent_id` are NULL only on a timeline row the backfill has
/// not written yet: a tombstone or a review of an older comment updates its
/// row before (#988). Such a row is not listed.
#[derive(DeserializeRow)]
#[scylla(flavor = "enforce_order")]
pub struct CommentFeedRow {
    pub created_at: CqlTimestamp,
    pub comment_id: Uuid,
    pub author_id:  Option<Uuid>,
    pub status:     Option<i8>,
    pub body:       Option<String>,
    pub gif_url:    Option<String>,
    pub gif_width:  Option<i32>,
    pub gif_height: Option<i32>,
    /// NULL ⇒ not held (#669).
    pub held:       Option<bool>,
    /// Nil UUID for a top-level comment.
    pub parent_id:  Option<Uuid>,
}
