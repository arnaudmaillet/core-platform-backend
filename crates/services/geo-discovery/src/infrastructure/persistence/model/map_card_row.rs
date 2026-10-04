use scylla::DeserializeRow;
use scylla::value::CqlTimestamp;
use uuid::Uuid;

/// ScyllaDB row type for `geo_discovery.map_post_cards`.
///
/// Field names must match CQL column names exactly; scylla 1.5 `DeserializeRow`
/// is name-based (not positional). `author_tier` is a `tinyint` (i8); the From
/// impl casts it to u8 before storing in `MapPostCard`.
/// Card columns are `Option` because a row can be a **tombstone**: only the
/// visibility cells, written when a delete / takedown arrives before the
/// post.published that indexes the card (two consumers, no ordering).
#[derive(Debug, DeserializeRow)]
pub struct MapCardRow {
    pub post_id:           Uuid,
    pub author_id:         Option<Uuid>,
    pub author_handle:     Option<String>,
    pub author_avatar_url: Option<String>,
    pub thumbnail_url:     Option<String>,
    /// Post caption for the Focus-mode read path. NULL on rows written before
    /// migration 0005 → empty caption.
    pub caption:           Option<String>,
    pub h3_index_r7:       Option<i64>,
    pub virality_score:    Option<f32>,
    pub published_at:      Option<CqlTimestamp>,
    pub expires_at:        Option<CqlTimestamp>,
    /// tinyint in ScyllaDB; 0=Standard, 1=Premium, 2=VIP. NULL (pre-0004) → 0.
    pub author_tier:       Option<i8>,
    /// Migration 0006: NULL on older rows (shown, never moderated, no coordinates).
    pub suppressed:         Option<i8>,
    pub moderation_version: Option<i64>,
    pub lat:                Option<f64>,
    pub lng:                Option<f64>,
    /// `TTL(author_handle)`: the card's remaining seconds (NULL for a tombstone).
    pub ttl_secs:           Option<i32>,
}

impl MapCardRow {
    /// The card, or `None` for a tombstone (no card cells).
    pub fn card(&self) -> Option<crate::domain::entity::MapPostCard> {
        Some(crate::domain::entity::MapPostCard {
            post_id:           self.post_id,
            author_id:         self.author_id?,
            author_handle:     self.author_handle.clone()?,
            author_avatar_url: self.author_avatar_url.clone().unwrap_or_default(),
            thumbnail_url:     self.thumbnail_url.clone().unwrap_or_default(),
            caption:           self.caption.clone().unwrap_or_default(),
            h3_index_r7:       self.h3_index_r7?,
            virality_score:    self.virality_score.unwrap_or(0.0),
            published_at_ms:   self.published_at?.0,
            author_tier:       self.author_tier.unwrap_or(0) as u8,
            lat:               self.lat,
            lng:               self.lng,
        })
    }
}
