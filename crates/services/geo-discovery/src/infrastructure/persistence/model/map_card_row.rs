use scylla::DeserializeRow;
use scylla::value::CqlTimestamp;
use uuid::Uuid;

/// ScyllaDB row type for `geo_discovery.map_post_cards`.
///
/// Field names must match CQL column names exactly; scylla 1.5 `DeserializeRow`
/// is name-based (not positional). `author_tier` is a `tinyint` (i8); the From
/// impl casts it to u8 before storing in `MapPostCard`.
#[derive(Debug, DeserializeRow)]
pub struct MapCardRow {
    pub post_id:           Uuid,
    pub author_id:         Uuid,
    pub author_handle:     String,
    pub author_avatar_url: String,
    pub thumbnail_url:     String,
    /// Post caption for the Focus-mode read path.
    /// Option<String> because rows written before migration 0005 have NULL here.
    /// NULL maps to an empty caption.
    pub caption:           Option<String>,
    pub h3_index_r7:       i64,
    pub virality_score:    f32,
    pub published_at:      CqlTimestamp,
    pub expires_at:        CqlTimestamp,
    /// tinyint in ScyllaDB; 0=Standard, 1=Premium, 2=VIP.
    /// Option<i8> because rows written before migration 0004 have NULL here.
    /// NULL maps to Standard (0) — correct for any legacy post.
    pub author_tier:       Option<i8>,
    /// Migration 0006: NULL on older rows (shown, never moderated, no coordinates).
    pub suppressed:         Option<i8>,
    pub moderation_version: Option<i64>,
    pub lat:                Option<f64>,
    pub lng:                Option<f64>,
    /// `TTL(author_handle)`: the row's remaining seconds (NULL when no TTL).
    pub ttl_secs:           Option<i32>,
}

impl From<MapCardRow> for crate::domain::entity::MapPostCard {
    fn from(row: MapCardRow) -> Self {
        Self {
            post_id:           row.post_id,
            author_id:         row.author_id,
            author_handle:     row.author_handle,
            author_avatar_url: row.author_avatar_url,
            thumbnail_url:     row.thumbnail_url,
            caption:           row.caption.unwrap_or_default(),
            h3_index_r7:       row.h3_index_r7,
            virality_score:    row.virality_score,
            published_at_ms:   row.published_at.0,
            author_tier:       row.author_tier.unwrap_or(0) as u8,
            lat:               row.lat,
            lng:               row.lng,
        }
    }
}
