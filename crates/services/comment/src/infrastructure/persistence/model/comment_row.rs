use scylla::value::CqlTimestamp;
use scylla::DeserializeRow;
use uuid::Uuid;

/// Positional deserialization for `comment.comments`.
///
/// SELECT must emit columns in exactly this order:
/// comment_id, post_id, author_id, parent_id, status, body,
/// gif_id, gif_url, gif_width, gif_height,
/// created_at, updated_at, deleted_at
#[derive(DeserializeRow)]
#[scylla(flavor = "enforce_order")]
pub struct CommentRow {
    pub comment_id: Uuid,
    pub post_id:    Uuid,
    pub author_id:  Uuid,
    pub parent_id:  Uuid,           // Uuid::nil() for top-level
    pub status:     i8,
    pub body:       Option<String>,
    pub gif_id:     Option<String>,
    pub gif_url:    Option<String>,
    pub gif_width:  Option<i32>,
    pub gif_height: Option<i32>,
    pub created_at: CqlTimestamp,
    pub updated_at: CqlTimestamp,
    pub deleted_at: Option<CqlTimestamp>,
    /// NULL ⇒ not held (#669).
    pub held:       Option<bool>,
}
