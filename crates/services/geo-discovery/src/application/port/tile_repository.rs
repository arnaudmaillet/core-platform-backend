use async_trait::async_trait;
use uuid::Uuid;

use crate::domain::entity::MapPostCard;
use crate::domain::value_object::{H3Index, H3Resolution, MapVisibility, PostId, RetentionTtl};
use crate::error::GeoDiscoveryError;

/// Port: ScyllaDB durable persistence layer.
///
/// Provides authoritative long-term storage for both the spatial tile index
/// and the card projection. Redis is the hot path; this repository provides:
///   1. Cold-start recovery (populate Redis after service restart).
///   2. Cache-miss fallback (serve cards not in Redis due to score threshold).
///   3. Virality score updates (written here on every score event).
#[async_trait]
pub trait TileRepository: Send + Sync {
    /// Writes one row to `posts_by_tile` with a per-row TTL.
    ///
    /// Called three times per published post (once per resolution).
    /// The INSERT is idempotent (last-write-wins for all columns in the PK).
    async fn insert_tile_entry(
        &self,
        h3_index:     H3Index,
        resolution:   H3Resolution,
        post_id:      &PostId,
        published_at_ms: i64,
        ttl:          RetentionTtl,
    ) -> Result<(), GeoDiscoveryError>;

    /// Writes or replaces the full card row in `map_post_cards`.
    async fn upsert_card(
        &self,
        card: &MapPostCard,
        ttl:  RetentionTtl,
    ) -> Result<(), GeoDiscoveryError>;

    /// Updates only the `virality_score` column for an existing card.
    async fn update_card_score(
        &self,
        post_id: &PostId,
        score:   f32,
    ) -> Result<(), GeoDiscoveryError>;

    /// Reads a card by post ID. Returns `None` if the row has expired or
    /// does not exist.
    async fn get_card(
        &self,
        post_id: &PostId,
    ) -> Result<Option<MapPostCard>, GeoDiscoveryError>;

    /// Returns all post IDs in the given tile partition (cold-start recovery).
    ///
    /// Ordered by `published_at DESC`. Limited to `limit` rows to bound memory.
    /// The row's visibility, its card (`None` for a tombstone: visibility cells
    /// only) and the card's remaining TTL in seconds; `None` when no row
    /// exists. `get_card` hides suppressed cards and tombstones; this does not.
    async fn get_card_with_visibility(
        &self,
        post_id: &PostId,
    ) -> Result<Option<(Option<MapPostCard>, MapVisibility, Option<i32>)>, GeoDiscoveryError>;

    /// Records the card's visibility; `ttl` is the row's remaining life.
    async fn set_visibility(
        &self,
        post_id:    &PostId,
        visibility: MapVisibility,
        ttl:        RetentionTtl,
    ) -> Result<(), GeoDiscoveryError>;

    async fn list_tile_post_ids(
        &self,
        h3_index:   H3Index,
        resolution: H3Resolution,
        limit:      i32,
    ) -> Result<Vec<Uuid>, GeoDiscoveryError>;
}
