use std::sync::Arc;

use cqrs::{Command, CommandHandler, Envelope};
use validate_core::{FieldViolation, Validate};

use crate::application::port::{CardStore, PinStore, TileRepository};
use crate::domain::entity::RadarPin;
use crate::domain::value_object::{PostId, RetentionTtl, Suppression, VisibilityChange};
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

pub struct ApplyMapVisibilityHandler<CS, TR, PS> {
    pub card_store:      Arc<CS>,
    pub tile_repository: Arc<TR>,
    pub pin_store:       Arc<PS>,
}

impl<CS, TR, PS> CommandHandler<ApplyMapVisibilityCommand> for ApplyMapVisibilityHandler<CS, TR, PS>
where
    CS: CardStore + 'static,
    TR: TileRepository + 'static,
    PS: PinStore + 'static,
{
    type Error = GeoDiscoveryError;

    /// ScyllaDB records the state (durable, so the cold read path filters it);
    /// Redis follows: a hidden post loses its pin (the Radar path drops a
    /// missing pin) and its cached card; a restored one gets its pin back.
    /// Idempotent: a post the map never indexed (no location, or expired) is a
    /// no-op, and a redelivery re-applies the same writes.
    async fn handle(&self, envelope: Envelope<ApplyMapVisibilityCommand>) -> Result<(), GeoDiscoveryError> {
        let cmd     = &envelope.payload;
        let post_id = PostId::try_from(cmd.post_id.as_str())?;

        let Some((card, held, ttl_secs)) = self.tile_repository.get_card_with_visibility(&post_id).await?
        else {
            return Ok(());
        };
        let Some(next) = held.apply(cmd.change) else {
            return Ok(()); // stale, or already permanently deleted
        };
        // Writes carry the row's remaining life, so they expire with it.
        let Some(remaining) = ttl_secs.filter(|s| *s > 0) else {
            return Ok(()); // expiring now
        };
        let ttl = RetentionTtl::from_secs(remaining as u64);

        self.tile_repository.set_visibility(&post_id, next, ttl).await?;

        if next.suppression == Suppression::None {
            // Restore the pin when the card kept the coordinates to rebuild it.
            if let (Some(lat), Some(lng)) = (card.lat, card.lng) {
                let pin = RadarPin {
                    post_id:       card.post_id,
                    lat,
                    lng,
                    thumbnail_url: card.thumbnail_url.clone(),
                    author_id:     Some(card.author_id),
                };
                self.pin_store.set(&pin, ttl).await?;
            }
        } else {
            self.pin_store.del(&post_id).await?;
            self.card_store.del(&post_id).await?;
        }
        Ok(())
    }
}
