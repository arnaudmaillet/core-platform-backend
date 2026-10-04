use std::sync::Arc;

use cqrs::{Command, CommandHandler, Envelope};
use validate_core::{FieldViolation, Validate};

use crate::application::port::{CardStore, PinStore, SpatialIndex, TileRepository};
use crate::domain::entity::{MapPostCard, RadarPin};
use crate::domain::value_object::{
    AuthorId, GeoCoordinate, H3Index, H3Resolution, PostId, RetentionTtl, Suppression,
    ViralityScore,
};
use crate::error::GeoDiscoveryError;

/// Indexes a newly published post into the spatial index and card store.
///
/// Triggered by the `PostIndexerWorker` on every `post.published` Kafka event.
///
/// Write order (chosen for graceful degradation):
///   1. ScyllaDB — durable source of truth. Always written first.
///   2. Redis spatial index — one ZADD+cap per resolution.
///   3. Redis card cache — conditional on score ≥ card_cache_threshold.
pub struct IndexPostCommand {
    pub post_id:           String,
    pub author_id:         String,
    pub author_handle:     String,
    pub author_avatar_url: String,
    pub thumbnail_url:     String,
    /// Post caption, denormalized from `post.published`. Stored on the card for
    /// the Focus-mode read path. Empty when the post has no caption.
    pub caption:           String,
    pub lat:               f64,
    pub lng:               f64,
    pub virality_score:    f64,
    pub published_at_ms:   i64,
    /// Seconds. None → service default (172 800 s).
    pub retention_secs:    Option<u64>,
    /// Author tier at publish time. 0=Standard, 1=Premium, 2=VIP.
    /// Sourced from the post.published Kafka event (denormalized by services/post).
    pub author_tier:       u8,
}

impl Command for IndexPostCommand {}

impl Validate for IndexPostCommand {
    fn validate(&self) -> Result<(), Vec<FieldViolation>> {
        let mut v = Vec::new();
        if self.post_id.trim().is_empty() {
            v.push(FieldViolation::new("post_id", "GEO-VAL-001", "post_id must not be empty"));
        }
        if self.author_id.trim().is_empty() {
            v.push(FieldViolation::new("author_id", "GEO-VAL-002", "author_id must not be empty"));
        }
        if !self.lat.is_finite() || self.lat < -90.0 || self.lat > 90.0 {
            v.push(FieldViolation::new("lat", "GEO-VAL-003", "lat must be in [-90, 90]"));
        }
        if !self.lng.is_finite() || self.lng < -180.0 || self.lng > 180.0 {
            v.push(FieldViolation::new("lng", "GEO-VAL-004", "lng must be in [-180, 180]"));
        }
        if !self.virality_score.is_finite() || self.virality_score < 0.0 {
            v.push(FieldViolation::new("virality_score", "GEO-VAL-005", "virality_score must be finite and non-negative"));
        }
        if v.is_empty() { Ok(()) } else { Err(v) }
    }
}

pub struct IndexPostHandler<SI, CS, TR, PS> {
    pub spatial_index:       Arc<SI>,
    pub card_store:          Arc<CS>,
    pub tile_repository:     Arc<TR>,
    pub pin_store:           Arc<PS>,
    pub card_cache_threshold: f64,
}

impl<SI, CS, TR, PS> CommandHandler<IndexPostCommand> for IndexPostHandler<SI, CS, TR, PS>
where
    SI: SpatialIndex + 'static,
    CS: CardStore + 'static,
    TR: TileRepository + 'static,
    PS: PinStore + 'static,
{
    type Error = GeoDiscoveryError;

    async fn handle(&self, envelope: Envelope<IndexPostCommand>) -> Result<(), GeoDiscoveryError> {
        let cmd = &envelope.payload;

        let post_id   = PostId::try_from(cmd.post_id.as_str())?;
        let author_id = AuthorId::try_from(cmd.author_id.as_str())?;
        let coord     = GeoCoordinate::new(cmd.lat, cmd.lng)?;
        let score     = ViralityScore::new(cmd.virality_score)?;
        let ttl       = cmd.retention_secs
            .map(RetentionTtl::from_secs)
            .unwrap_or_else(RetentionTtl::default_ttl);

        let idx_r5 = H3Index::encode(&coord, H3Resolution::R5);
        let idx_r7 = H3Index::encode(&coord, H3Resolution::R7);
        let idx_r9 = H3Index::encode(&coord, H3Resolution::R9);

        let card = MapPostCard {
            post_id:           post_id.as_uuid(),
            author_id:         author_id.as_uuid(),
            author_handle:     cmd.author_handle.clone(),
            author_avatar_url: cmd.author_avatar_url.clone(),
            thumbnail_url:     cmd.thumbnail_url.clone(),
            caption:           cmd.caption.clone(),
            h3_index_r7:       idx_r7.as_i64(),
            virality_score:    score.as_f32(),
            published_at_ms:   cmd.published_at_ms,
            author_tier:       cmd.author_tier,
            lat:               Some(cmd.lat),
            lng:               Some(cmd.lng),
        };

        // A redelivered post.published must not bring back a post the map
        // already suppressed (deleted, or moderated): keep it off Redis.
        let suppressed = self
            .tile_repository
            .get_card_with_visibility(&post_id)
            .await?
            .is_some_and(|(_, visibility, _)| visibility.suppression != Suppression::None);

        // ── 1. ScyllaDB (durable, always first) ───────────────────────────────
        let (r5, r7, r9, card_res) = tokio::join!(
            self.tile_repository.insert_tile_entry(idx_r5, H3Resolution::R5, &post_id, cmd.published_at_ms, ttl),
            self.tile_repository.insert_tile_entry(idx_r7, H3Resolution::R7, &post_id, cmd.published_at_ms, ttl),
            self.tile_repository.insert_tile_entry(idx_r9, H3Resolution::R9, &post_id, cmd.published_at_ms, ttl),
            self.tile_repository.upsert_card(&card, ttl),
        );
        r5?; r7?; r9?; card_res?;
        if suppressed {
            tracing::debug!(post_id = %post_id, "post.published for a suppressed map post — Redis left untouched");
            return Ok(());
        }

        // ── 2. Redis spatial index (ZADDs with Top-K cap) ─────────────────────
        let (si5, si7, si9) = tokio::join!(
            self.spatial_index.upsert(idx_r5, H3Resolution::R5, &post_id, score),
            self.spatial_index.upsert(idx_r7, H3Resolution::R7, &post_id, score),
            self.spatial_index.upsert(idx_r9, H3Resolution::R9, &post_id, score),
        );
        if let Err(e) = si5.and(si7).and(si9) {
            tracing::warn!(post_id = %post_id, error = %e, "spatial index write failed — ScyllaDB is durable");
        }

        // ── 3. Redis pin projection (Radar path — ALWAYS) ─────────────────────
        // Every indexed post needs a pin: the Radar pan query is Redis-only with
        // no ScyllaDB fallback, so a missing pin means the marker never renders.
        let pin = RadarPin {
            post_id:       post_id.as_uuid(),
            lat:           cmd.lat,
            lng:           cmd.lng,
            thumbnail_url: cmd.thumbnail_url.clone(),
            author_id:     Some(author_id.as_uuid()),
        };
        if let Err(e) = self.pin_store.set(&pin, ttl).await {
            tracing::warn!(post_id = %post_id, error = %e, "pin store write failed — Radar will miss this post until reindex");
        }

        // ── 4. Redis card cache (Focus path — conditional on score threshold) ──
        if score.exceeds_threshold(self.card_cache_threshold)
            && let Err(e) = self.card_store.set(&card, ttl).await {
                tracing::warn!(post_id = %post_id, error = %e, "card cache write failed — ScyllaDB is durable");
            }

        tracing::debug!(
            post_id  = %post_id,
            lat      = cmd.lat,
            lng      = cmd.lng,
            h3_r5    = idx_r5.as_u64(),
            h3_r7    = idx_r7.as_u64(),
            h3_r9    = idx_r9.as_u64(),
            score    = score.as_f64(),
            ttl_secs = ttl.as_redis_ex(),
            "post indexed"
        );

        Ok(())
    }
}
