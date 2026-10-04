use std::sync::Arc;

use cqrs::{Command, CommandHandler, Envelope};
use validate_core::{FieldViolation, Validate};

use crate::application::port::{CardStore, PinStore, SpatialIndex, TileRepository};
use crate::domain::entity::RadarPin;
use crate::domain::value_object::{
    GeoCoordinate, H3Index, H3Resolution, PostId, RetentionTtl, Suppression,
    ViralityScore, VisibilityChange,
};
use crate::error::GeoDiscoveryError;

/// Takes a post off the map (deleted, or moderation removed / limited it) or
/// puts it back (a newer moderation reversal). Issued by the visibility worker
/// from `post.deleted` and `moderation.v1.events`, never by a client.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ApplyMapVisibilityCommand {
    pub post_id: String,
    pub change:  VisibilityChange,
}

impl Command for ApplyMapVisibilityCommand {}

impl Validate for ApplyMapVisibilityCommand {
    fn validate(&self) -> Result<(), Vec<FieldViolation>> {
        if self.post_id.trim().is_empty() {
            return Err(vec![FieldViolation::new("post_id", "GEO-VAL-001", "post_id must not be empty")]);
        }
        Ok(())
    }
}

/// How long a tombstone (a visibility written before its card exists) lives:
/// the longest retention a card can have, so the card it guards never outlives it.
const TOMBSTONE_TTL_SECS: u64 = RetentionTtl::MAX_SECS;

pub struct ApplyMapVisibilityHandler<SI, CS, TR, PS> {
    pub spatial_index:   Arc<SI>,
    pub card_store:      Arc<CS>,
    pub tile_repository: Arc<TR>,
    pub pin_store:       Arc<PS>,
}

impl<SI, CS, TR, PS> CommandHandler<ApplyMapVisibilityCommand> for ApplyMapVisibilityHandler<SI, CS, TR, PS>
where
    SI: SpatialIndex + 'static,
    CS: CardStore + 'static,
    TR: TileRepository + 'static,
    PS: PinStore + 'static,
{
    type Error = GeoDiscoveryError;

    /// ScyllaDB records the state (durable, so the cold read path filters it);
    /// Redis follows: a hidden post loses its pin (the Radar path drops a
    /// missing pin) and its cached card; a restored one gets its pin and its
    /// spatial-index entries back. When the card does not exist yet (the index
    /// consumer lags behind this one), a **tombstone** keeps the decision so
    /// the later post.published cannot surface the post. Idempotent: a
    /// redelivery re-applies the same writes.
    async fn handle(&self, envelope: Envelope<ApplyMapVisibilityCommand>) -> Result<(), GeoDiscoveryError> {
        let cmd     = &envelope.payload;
        let post_id = PostId::try_from(cmd.post_id.as_str())?;

        // No row at all reads as a card-less, shown, never-moderated post.
        let (card, held, ttl_secs) =
            self.tile_repository.get_card_with_visibility(&post_id).await?.unwrap_or_default();
        let Some(next) = held.apply(cmd.change) else {
            return Ok(()); // stale, or already permanently deleted
        };

        let Some(card) = card else {
            // No card (yet): record the decision as a tombstone. post.published
            // will find it and keep the post off Redis.
            let ttl = RetentionTtl::from_secs(TOMBSTONE_TTL_SECS);
            return self.tile_repository.set_visibility(&post_id, next, ttl).await;
        };

        // Writes carry the card's remaining life, so they expire with it.
        let Some(remaining) = ttl_secs.filter(|s| *s > 0) else {
            return Ok(()); // expiring now
        };
        let ttl = RetentionTtl::from_secs(remaining as u64);
        self.tile_repository.set_visibility(&post_id, next, ttl).await?;

        if next.suppression != Suppression::None {
            self.pin_store.del(&post_id).await?;
            self.card_store.del(&post_id).await?;
            return Ok(());
        }

        // Restore: the pin and the spatial-index entries (gone if the post was
        // suppressed before it was ever indexed in Redis), from the card's
        // stored coordinates. A card without them ages out unseen.
        let (Some(lat), Some(lng)) = (card.lat, card.lng) else {
            return Ok(());
        };
        let coord = GeoCoordinate::new(lat, lng)?;
        let score = ViralityScore::from(card.virality_score);
        for res in [H3Resolution::R5, H3Resolution::R7, H3Resolution::R9] {
            self.spatial_index.upsert(H3Index::encode(&coord, res), res, &post_id, score).await?;
        }
        let pin = RadarPin {
            post_id:       card.post_id,
            lat,
            lng,
            thumbnail_url: card.thumbnail_url.clone(),
            author_id:     Some(card.author_id),
        };
        self.pin_store.set(&pin, ttl).await
    }
}
