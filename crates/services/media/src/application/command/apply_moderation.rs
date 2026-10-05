use std::sync::Arc;

use chrono::{DateTime, Utc};
use cqrs::Envelope;

use crate::application::command::asset_objects;
use crate::application::port::{AssetRepository, CdnGateway, DeliveryCache, EventPublisher, ObjectStore};
use crate::domain::value_object::{AssetId, AssetState, StorageKey};
use crate::error::MediaError;

/// The takedown direction, distilled from a consumed `moderation.v1.events`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ModerationAction {
    Quarantine,
    Restore,
}

/// Apply a moderation decision to an asset (driven by the moderation consumer,
/// Phase 5).
#[derive(Debug, Clone)]
pub struct ApplyModerationCommand {
    pub asset_id: AssetId,
    pub action: ModerationAction,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ApplyModerationOutcome {
    /// `false` when the asset is unknown (the event is for media we don't hold) —
    /// a folded no-op the consumer commits.
    pub applied: bool,
    pub state: Option<AssetState>,
}

/// Reactively enforces a moderation verdict on the byte plane: a quarantine revokes
/// delivery (state flip + cache drop + CDN purge); a restore reinstates it.
/// This is the content-service side of moderation's enforcement — `media` flips
/// visibility, it does not decide.
///
/// A quarantine is **persisted first**: from then on `ResolveDelivery` stops
/// handing out URLs. Then the asset's objects move under `quarantine/` (the
/// origin refuses a URL handed out before), then the edge is purged. Any failure
/// propagates and the consumer redelivers; the redelivery — the asset already
/// quarantined, a no-op transition — redoes only what is left (moves are
/// idempotent). A restore moves the objects back **before** reinstating the
/// asset, so it is never deliverable with its objects away.
pub struct ApplyModerationHandler {
    assets: Arc<dyn AssetRepository>,
    store: Arc<dyn ObjectStore>,
    cdn: Arc<dyn CdnGateway>,
    cache: Arc<dyn DeliveryCache>,
    publisher: Arc<dyn EventPublisher>,
}

impl ApplyModerationHandler {
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
        envelope: Envelope<ApplyModerationCommand>,
        now: DateTime<Utc>,
    ) -> Result<ApplyModerationOutcome, MediaError> {
        let cmd = envelope.payload;
        let Some(mut asset) = self.assets.find_by_id(&cmd.asset_id).await? else {
            // Unknown asset — fold to a no-op so the consumer commits.
            return Ok(ApplyModerationOutcome { applied: false, state: None });
        };

        match cmd.action {
            ModerationAction::Quarantine => asset.quarantine(now)?,
            ModerationAction::Restore => {
                if asset.state() == AssetState::Quarantined {
                    asset_objects::release(self.store.as_ref(), &asset).await?;
                }
                asset.restore(now)?;
            }
        }
        self.assets.save(&asset).await?;
        for event in asset.drain_events() {
            self.publisher.publish(&event).await?;
        }
        // Drop the cached delivery so the next read re-resolves against the new state.
        self.cache.invalidate(&asset.id()).await?;

        if cmd.action == ModerationAction::Quarantine
            && asset_objects::owns_objects(self.assets.as_ref(), &asset).await?
        {
            // The origin stops serving, then the edge drops its cached copies.
            let keys: Vec<StorageKey> = asset_objects::quarantine(self.store.as_ref(), &asset).await?;
            if !keys.is_empty() {
                self.cdn.invalidate(&keys).await?;
            }
        }
        Ok(ApplyModerationOutcome { applied: true, state: Some(asset.state()) })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::application::fakes::{t0, Fixture};
    use crate::domain::value_object::MediaKind;
    use uuid::Uuid;

    fn env(asset_id: AssetId, action: ModerationAction) -> Envelope<ApplyModerationCommand> {
        Envelope::new(Uuid::now_v7(), ApplyModerationCommand { asset_id, action })
    }

    #[tokio::test]
    async fn quarantine_revokes_delivery_then_restore_reinstates() {
        let fx = Fixture::new();
        let (asset_id, _owner) = fx.ready_asset(MediaKind::PostImage).await;
        fx.publisher.clear();

        let out = fx
            .apply_moderation_handler()
            .handle(env(asset_id, ModerationAction::Quarantine), t0())
            .await
            .unwrap();
        assert!(out.applied);
        assert_eq!(out.state, Some(AssetState::Quarantined));
        assert!(!fx.cdn.invalidated_keys().is_empty(), "delivery revoked at the edge");
        assert_eq!(fx.publisher.event_types(), vec!["media.asset_quarantined"]);

        fx.publisher.clear();
        let out = fx
            .apply_moderation_handler()
            .handle(env(asset_id, ModerationAction::Restore), t0())
            .await
            .unwrap();
        assert_eq!(out.state, Some(AssetState::Ready));
        assert_eq!(fx.publisher.event_types(), vec!["media.asset_restored"]);
    }

    #[tokio::test]
    async fn a_failed_edge_purge_keeps_the_quarantine_and_the_redelivery_purges() {
        let fx = Fixture::new();
        let (asset_id, _owner) = fx.ready_asset(MediaKind::PostImage).await;
        fx.publisher.clear();

        // CloudFront is down: the takedown errors (so it is redelivered)…
        fx.cdn.fail(true);
        let err = fx
            .apply_moderation_handler()
            .handle(env(asset_id, ModerationAction::Quarantine), t0())
            .await
            .unwrap_err();
        assert!(matches!(err, MediaError::CdnInvalidationFailed { .. }));
        // …but the quarantine is already persisted and announced: nothing is
        // delivered any more.
        let asset = fx.assets.find_by_id(&asset_id).await.unwrap().unwrap();
        assert_eq!(asset.state(), AssetState::Quarantined);
        assert_eq!(fx.publisher.event_types(), vec!["media.asset_quarantined"]);
        assert!(fx.cdn.invalidated_keys().is_empty());

        // The redelivery, CloudFront back: only the purge happens.
        fx.cdn.fail(false);
        fx.publisher.clear();
        let out = fx
            .apply_moderation_handler()
            .handle(env(asset_id, ModerationAction::Quarantine), t0())
            .await
            .unwrap();
        assert_eq!(out.state, Some(AssetState::Quarantined));
        assert!(!fx.cdn.invalidated_keys().is_empty(), "purged on redelivery");
        assert!(fx.publisher.event_types().is_empty(), "no second quarantine event");
    }

    /// Stores the asset's renditions (and an extra object of the same tree, like
    /// an HLS segment) in the fake store; returns the tree's keys.
    async fn publish_objects(fx: &Fixture, asset_id: AssetId) -> Vec<String> {
        let asset = fx.assets.find_by_id(&asset_id).await.unwrap().unwrap();
        let mut keys: Vec<StorageKey> = asset.renditions().iter().map(|r| r.storage_key().clone()).collect();
        keys.push(StorageKey::from_raw(format!("{}seg_001.m4s", keys[0].tree_prefix())));
        for key in &keys {
            fx.store.put_object(key, 10, "etag");
        }
        let mut keys: Vec<String> = keys.iter().map(|k| k.as_str().to_owned()).collect();
        keys.sort();
        keys
    }

    #[tokio::test]
    async fn a_takedown_moves_the_whole_tree_out_of_the_origin_and_a_restore_brings_it_back() {
        let fx = Fixture::new();
        let (asset_id, _owner) = fx.ready_asset(MediaKind::PostImage).await;
        let public = publish_objects(&fx, asset_id).await;
        let stored = fx.store.keys(); // the tree and the original upload

        fx.apply_moderation_handler().handle(env(asset_id, ModerationAction::Quarantine), t0()).await.unwrap();
        let mut quarantined: Vec<String> = stored.iter().map(|k| format!("quarantine/{k}")).collect();
        quarantined.sort();
        assert_eq!(fx.store.keys(), quarantined, "nothing left at a public key, the upload included");
        let mut purged = fx.cdn.invalidated_keys();
        purged.sort();
        assert_eq!(purged, public, "the edge purges every public path, segments included");

        // Redelivered: nothing more to move, the purge is repeated.
        fx.apply_moderation_handler().handle(env(asset_id, ModerationAction::Quarantine), t0()).await.unwrap();
        assert_eq!(fx.store.keys(), quarantined);

        fx.apply_moderation_handler().handle(env(asset_id, ModerationAction::Restore), t0()).await.unwrap();
        assert_eq!(fx.store.keys(), stored, "restored to the public keys");
    }

    #[tokio::test]
    async fn objects_shared_with_another_delivered_asset_stay_put() {
        let fx = Fixture::new();
        // Same bytes (the fake probe hashes every upload alike): same keys.
        let (taken_down, _) = fx.ready_asset(MediaKind::PostImage).await;
        let (_still_live, _) = fx.ready_asset(MediaKind::PostImage).await;
        publish_objects(&fx, taken_down).await;
        let stored = fx.store.keys();

        let out =
            fx.apply_moderation_handler().handle(env(taken_down, ModerationAction::Quarantine), t0()).await.unwrap();
        assert_eq!(out.state, Some(AssetState::Quarantined), "the asset itself is still taken down");
        assert_eq!(fx.store.keys(), stored, "the other asset's objects are untouched");
    }

    #[tokio::test]
    async fn an_unknown_asset_is_a_folded_no_op() {
        let fx = Fixture::new();
        let out = fx
            .apply_moderation_handler()
            .handle(env(AssetId::new(), ModerationAction::Quarantine), t0())
            .await
            .unwrap();
        assert!(!out.applied);
        assert!(out.state.is_none());
    }
}
