use scylla::value::CqlTimestamp;
use scylla::DeserializeRow;
use uuid::Uuid;

/// Positional deserialization for `post.posts`.
///
/// SELECT must emit columns in exactly this order:
/// post_id, profile_id, kind, status, caption, attachments,
/// parent_id, root_id, created_at, updated_at, published_at, deleted_at,
/// audio_id, audio_kind, lat, lng, moderation_restriction, moderation_version
#[derive(DeserializeRow)]
#[scylla(flavor = "enforce_order")]
pub struct PostRow {
    pub post_id:      Uuid,
    pub profile_id:   Uuid,
    pub kind:         i8,
    pub status:       i8,
    pub caption:      String,
    pub attachments:  String,
    pub parent_id:    Option<Uuid>,
    pub root_id:      Option<Uuid>,
    pub created_at:   CqlTimestamp,
    pub updated_at:   CqlTimestamp,
    pub published_at: Option<CqlTimestamp>,
    pub deleted_at:   Option<CqlTimestamp>,
    pub audio_id:     Option<Uuid>,
    pub audio_kind:   Option<i8>,
    /// Client-supplied location. NULL for locationless posts (and any row
    /// written before migration 0006).
    pub lat:          Option<f64>,
    pub lng:          Option<f64>,
    /// NULL until moderation first acts on the post (migration 0007).
    pub moderation_restriction: Option<i8>,
    pub moderation_version:     Option<i64>,
    /// NULL ⇒ the author's default (migration 0011).
    pub allow_remix:            Option<bool>,
    pub allow_sound_reuse:      Option<bool>,
}
