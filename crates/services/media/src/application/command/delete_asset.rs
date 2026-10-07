use std::sync::Arc;

use chrono::{DateTime, Utc};
use cqrs::Envelope;

use crate::application::command::asset_objects;
use crate::application::port::{AssetRepository, CdnGateway, DeliveryCache, EventPublisher, ObjectStore};
use crate::domain::value_object::{AssetId, OwnerId, StorageKey};
use crate::error::MediaError;

/// Owner-initiated hard delete.
#[derive(Debug, Clone)]
pub struct DeleteAssetCommand {
    pub asset_id: AssetId,
    /// The requesting actor (edge-resolved); must own the asset.
    pub owner_id: OwnerId,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DeleteOutcome {
    pub deleted: bool,
}

/// Deletes an asset: the domain `delete` is attempted **first** so a legal hold
/// (`LegalHoldActive`, MED-7003) blocks erasure before any byte is touched. On a
/// real delete it purges every object (master + renditions + staging), tombstones
/// the row, emits `AssetDeleted`, drops the cache, then purges the CDN.
pub struct DeleteAssetHandler {
    assets: Arc<dyn AssetRepository>,
    store: Arc<dyn ObjectStore>,
    cdn: Arc<dyn CdnGateway>,
    cache: Arc<dyn DeliveryCache>,
    publisher: Arc<dyn EventPublisher>,
}

impl DeleteAssetHandler {
    pub fn new(
        assets: Arc<dyn AssetRepository>,
        store: Arc<dyn ObjectStore>,
        cdn: Arc<dyn CdnGateway>,
        cache: Arc<dyn DeliveryCache>,
        publisher: Arc<dyn EventPublisher>,
    ) -> Self {
        Self { assets, store, cdn, cache, publisher }
    }

    pub async fn handle(
        &self,
        envelope: Envelope<DeleteAssetCommand>,
        now: DateTime<Utc>,
    ) -> Result<DeleteOutcome, MediaError> {
        let cmd = envelope.payload;
        let mut asset = self
            .assets
            .find_by_id(&cmd.asset_id)
            .await?
            .ok_or_else(|| MediaError::AssetNotFound { id: cmd.asset_id.as_str() })?;

        // Don't leak existence to a non-owner — same response as a missing asset.
        if asset.owner_id() != cmd.owner_id {
            return Err(MediaError::AssetNotFound { id: cmd.asset_id.as_str() });
        }

        // Legal-hold guard fires here, before any byte is purged.
        asset.delete(now)?;

        // Purge bytes: every object of the asset's trees, public or quarantined
        // (a video's whole HLS output too), and the staging object.
        let (mut objects, public) = if asset_objects::owns_objects(self.assets.as_ref(), &asset).await? {
            asset_objects::all(self.store.as_ref(), &asset).await?
        } else {
            (Vec::new(), Vec::new())
        };
        objects.push(StorageKey::staging(asset.id()));
        // A private document's object (#777) is keyed by the asset, never
        // shared with another asset's bytes: it always goes.
        let private = StorageKey::private_document(asset.id());
        objects.push(private.quarantined());
        objects.push(private);
        for key in &objects {
            self.store.delete(key).await?;
        }
        // Persist the tombstone before the edge purge: a CDN outage must not
        // leave the asset deliverable. Retrying the delete (already deleted: a
        // no-op transition, idempotent object deletes) only purges again.
        self.assets.save(&asset).await?;
        for event in asset.drain_events() {
            self.publisher.publish(&event).await?;
        }
        self.cache.invalidate(&asset.id()).await?;
        if !public.is_empty() {
            self.cdn.invalidate(&public).await?;
        }
        Ok(DeleteOutcome { deleted: true })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::application::fakes::{t0, Fixture};
    use crate::domain::value_object::{AssetState, MediaKind};
    use uuid::Uuid;

    fn env(asset_id: AssetId, owner_id: OwnerId) -> Envelope<DeleteAssetCommand> {
        Envelope::new(Uuid::now_v7(), DeleteAssetCommand { asset_id, owner_id })
    }

    #[tokio::test]
    async fn deletes_a_ready_asset_and_purges_bytes() {
        let fx = Fixture::new();
        let (asset_id, owner) = fx.ready_asset(MediaKind::PostImage).await;
        fx.publisher.clear();

        let out = fx.delete_handler().handle(env(asset_id, owner), t0()).await.unwrap();
        assert!(out.deleted);

        let asset = fx.assets.find_by_id(&asset_id).await.unwrap().unwrap();
        assert_eq!(asset.state(), AssetState::Deleted);
        assert_eq!(fx.publisher.event_types(), vec!["media.asset_deleted"]);
        // The CDN was invalidated for the purged keys.
        assert!(!fx.cdn.invalidated_keys().is_empty());
    }

    #[tokio::test]
    async fn deleting_a_quarantined_asset_erases_its_quarantined_objects_too() {
        use crate::application::command::{ApplyModerationCommand, ModerationAction};
        let fx = Fixture::new();
        let (asset_id, owner) = fx.ready_asset(MediaKind::PostImage).await;
        let asset = fx.assets.find_by_id(&asset_id).await.unwrap().unwrap();
        for r in asset.renditions() {
            fx.store.put_object(r.storage_key(), 10, "etag");
        }
        fx.apply_moderation_handler()
            .handle(
                Envelope::new(Uuid::now_v7(), ApplyModerationCommand { asset_id, action: ModerationAction::Quarantine, enforcement_id: None }),
                t0(),
            )
            .await
            .unwrap();
        assert!(fx.store.keys().iter().all(|k| k.starts_with("quarantine/")));

        fx.delete_handler().handle(env(asset_id, owner), t0()).await.unwrap();
        assert!(fx.store.keys().is_empty(), "no copy survives a delete");
    }

    #[tokio::test]
    async fn deleting_one_of_two_identical_assets_keeps_the_shared_objects() {
        let fx = Fixture::new();
        let (asset_id, owner) = fx.ready_asset(MediaKind::PostImage).await;
        let (_copy, _) = fx.ready_asset(MediaKind::PostImage).await;
        let asset = fx.assets.find_by_id(&asset_id).await.unwrap().unwrap();
        for r in asset.renditions() {
            fx.store.put_object(r.storage_key(), 10, "etag");
        }
        fx.delete_handler().handle(env(asset_id, owner), t0()).await.unwrap();
        for r in asset.renditions() {
            assert!(fx.store.keys().contains(&r.storage_key().as_str().to_owned()), "the copy still uses it");
        }
    }

    #[tokio::test]
    async fn a_legal_hold_blocks_deletion_before_any_byte_is_touched() {
        let fx = Fixture::new();
        let (asset_id, owner) = fx.ready_asset(MediaKind::PostImage).await;
        // Quarantine + legal hold via a CSAM screen path would set this; place it directly.
        {
            let mut a = fx.assets.find_by_id(&asset_id).await.unwrap().unwrap();
            a.place_legal_hold(t0());
            fx.assets.save(&a).await.unwrap();
        }
        let err = fx.delete_handler().handle(env(asset_id, owner), t0()).await.unwrap_err();
        assert!(matches!(err, MediaError::LegalHoldActive));
        // Nothing purged.
        assert!(fx.cdn.invalidated_keys().is_empty());
        let asset = fx.assets.find_by_id(&asset_id).await.unwrap().unwrap();
        assert_eq!(asset.state(), AssetState::Ready);
    }

    #[tokio::test]
    async fn a_non_owner_cannot_delete_and_sees_not_found() {
        let fx = Fixture::new();
        let (asset_id, _owner) = fx.ready_asset(MediaKind::PostImage).await;
        let stranger = OwnerId::from_uuid(Uuid::from_u128(999));
        let err = fx.delete_handler().handle(env(asset_id, stranger), t0()).await.unwrap_err();
        assert!(matches!(err, MediaError::AssetNotFound { .. }));
    }
}
