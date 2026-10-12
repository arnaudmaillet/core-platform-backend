use std::sync::Arc;

use chrono::{DateTime, Utc};
use cqrs::Envelope;

use crate::application::command::asset_objects;
use crate::application::policy::MediaPolicy;
use crate::application::port::{AssetRepository, CdnGateway, DeliveryCache, ObjectStore};
use crate::domain::aggregate::Asset;
use crate::domain::value_object::{AssetId, DeliveryVisibility, ObjectRoot, OwnerId, StorageKey};
use crate::error::MediaError;

/// The owner changes how a READY asset is delivered (#919).
#[derive(Debug, Clone)]
pub struct ChangeVisibilityCommand {
    pub asset_id: AssetId,
    /// The requesting actor; must own the asset.
    pub owner_id: OwnerId,
    pub visibility: DeliveryVisibility,
}

/// Changes an asset's delivery visibility and moves its renditions where they
/// belong:
/// * to SIGNED, with `MEDIA_RESTRICTED_PREFIX_ENABLED` on: into its own
///   `restricted/` tree. Bytes it shares with another asset are copied (that
///   asset keeps them public); bytes it alone holds are moved, and the CDN is
///   purged of their public paths. Off, they stay where they are (still only
///   ever delivered signed).
/// * to PUBLIC: out of `restricted/`, back to the content-addressed tree.
///
/// Objects move before the row is saved, and the move is driven by listing, so
/// a retry after a crash finds them moved and completes. The same visibility
/// again only completes such a move (e.g. after the flag was turned on).
pub struct ChangeVisibilityHandler {
    assets: Arc<dyn AssetRepository>,
    store: Arc<dyn ObjectStore>,
    cdn: Arc<dyn CdnGateway>,
    cache: Arc<dyn DeliveryCache>,
    policy: MediaPolicy,
}

impl ChangeVisibilityHandler {
    pub fn new(
        assets: Arc<dyn AssetRepository>,
        store: Arc<dyn ObjectStore>,
        cdn: Arc<dyn CdnGateway>,
        cache: Arc<dyn DeliveryCache>,
        policy: MediaPolicy,
    ) -> Self {
        Self { assets, store, cdn, cache, policy }
    }

    pub async fn handle(&self, envelope: Envelope<ChangeVisibilityCommand>, now: DateTime<Utc>) -> Result<Asset, MediaError> {
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

        let changed = asset.change_visibility(cmd.visibility, now)?;
        let (moved, purge) = self.move_objects(&mut asset, now).await?;
        if !changed && !moved {
            return Ok(asset);
        }
        self.assets.save(&asset).await?;
        self.cache.invalidate(&asset.id()).await?;
        if !purge.is_empty() {
            self.cdn.invalidate(&purge).await?;
        }
        tracing::info!(asset.id = %asset.id(), visibility = %asset.delivery_visibility(), moved, "delivery visibility changed");
        Ok(asset)
    }

    /// Moves a READY asset's objects to the root its visibility calls for;
    /// whether they moved, and the public keys the CDN must forget.
    async fn move_objects(&self, asset: &mut Asset, now: DateTime<Utc>) -> Result<(bool, Vec<StorageKey>), MediaError> {
        // A private document has no renditions: its object is under `private/`.
        if !asset.is_deliverable() || asset.kind().is_private() {
            return Ok((false, Vec::new()));
        }
        let (Some(from), Some(to)) = (asset.object_root(), self.target_root(asset)) else {
            return Ok((false, Vec::new()));
        };
        if from == to {
            return Ok((false, Vec::new()));
        }
        // Bytes another asset still serves from the shared tree stay there.
        let shared = !asset_objects::owns_objects(self.assets.as_ref(), asset).await?;
        for key in self.store.list(&from.prefix()).await? {
            let Some(dest) = key.rebased(&from, &to) else { continue };
            if shared {
                self.store.copy(&key, &dest).await?;
            } else {
                self.store.relocate(&key, &dest).await?;
            }
        }
        // The public paths the moved objects had (listed at their destination,
        // so a retry after a crash still purges them all).
        let purge = if from.is_restricted() || shared {
            Vec::new()
        } else {
            self.store.list(&to.prefix()).await?.iter().filter_map(|key| key.rebased(&to, &from)).collect()
        };
        asset.rebase_renditions(&from, &to, now);
        Ok((true, purge))
    }

    /// Where the asset's objects belong: a SIGNED asset's stay put while the
    /// restricted prefix is off (never moved back to a shared public tree).
    fn target_root(&self, asset: &Asset) -> Option<ObjectRoot> {
        match asset.delivery_visibility() {
            DeliveryVisibility::Signed if !self.policy.restricted_prefix_enabled => asset.object_root(),
            _ => asset.storage_root(self.policy.restricted_prefix_enabled),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::application::fakes::{t0, Fixture};
    use crate::domain::value_object::{AssetState, MediaKind, RenditionKind};
    use uuid::Uuid;

    fn env(asset_id: AssetId, owner_id: OwnerId, visibility: DeliveryVisibility) -> Envelope<ChangeVisibilityCommand> {
        Envelope::new(Uuid::now_v7(), ChangeVisibilityCommand { asset_id, owner_id, visibility })
    }

    fn restricted_on(fx: &mut Fixture) {
        fx.policy.restricted_prefix_enabled = true;
    }

    /// Stores an object at each of the asset's rendition keys (and a segment
    /// under its tree), as the pipeline would have.
    fn store_renditions(fx: &Fixture, asset: &Asset) {
        for r in asset.renditions() {
            fx.store.put_object(r.storage_key(), 1, "e");
        }
        fx.store.put_object(&asset.object_root().unwrap().object("720w/seg_000.m4s"), 1, "e");
    }

    /// Every stored rendition object (not the staging uploads).
    fn objects(fx: &Fixture) -> Vec<String> {
        fx.store.keys().into_iter().filter(|k| !k.starts_with("uploads/")).collect()
    }

    #[tokio::test]
    async fn going_signed_moves_its_own_bytes_under_restricted_and_purges_the_cdn() {
        let mut fx = Fixture::new();
        restricted_on(&mut fx);
        let (id, owner) = fx.ready_asset(MediaKind::PostImage).await;
        let before = fx.assets.find_by_id(&id).await.unwrap().unwrap();
        store_renditions(&fx, &before);
        fx.cache.put(&before).await.unwrap();

        let after = fx.change_visibility_handler().handle(env(id, owner, DeliveryVisibility::Signed), t0()).await.unwrap();

        assert_eq!(after.delivery_visibility(), DeliveryVisibility::Signed);
        let prefix = format!("restricted/post-images/{id}/");
        assert!(after.renditions().iter().all(|r| r.storage_key().as_str().starts_with(&prefix)));
        let keys = objects(&fx);
        assert!(keys.iter().all(|k| k.starts_with(&prefix)), "nothing left in the shared tree: {keys:?}");
        assert_eq!(keys.len(), before.renditions().len() + 1, "the whole tree moved");
        let purged = fx.cdn.invalidated_keys();
        assert!(purged.contains(&before.rendition(RenditionKind::Original).unwrap().storage_key().as_str().to_owned()));
        assert_eq!(purged.len(), keys.len(), "every public path is purged");
        assert!(fx.cache.get(&id).await.unwrap().is_none(), "the delivery cache is evicted");
        assert_eq!(fx.assets.find_by_id(&id).await.unwrap().unwrap().renditions(), after.renditions());
    }

    #[tokio::test]
    async fn bytes_another_asset_shares_are_copied_and_left_public() {
        let mut fx = Fixture::new();
        restricted_on(&mut fx);
        let (id, owner) = fx.ready_asset(MediaKind::PostImage).await;
        let (_other, _) = fx.ready_asset(MediaKind::PostImage).await; // same bytes
        let before = fx.assets.find_by_id(&id).await.unwrap().unwrap();
        store_renditions(&fx, &before);

        let after = fx.change_visibility_handler().handle(env(id, owner, DeliveryVisibility::Signed), t0()).await.unwrap();

        let original = before.rendition(RenditionKind::Original).unwrap().storage_key().as_str().to_owned();
        let keys = objects(&fx);
        assert!(keys.contains(&original), "the other asset still serves its bytes");
        assert!(keys.contains(&after.rendition(RenditionKind::Original).unwrap().storage_key().as_str().to_owned()));
        assert!(fx.cdn.invalidated_keys().is_empty(), "the public paths still serve the other asset");
    }

    #[tokio::test]
    async fn going_public_moves_back_to_the_content_addressed_tree() {
        let mut fx = Fixture::new();
        restricted_on(&mut fx);
        let (id, owner) = fx.ready_asset(MediaKind::PostImage).await;
        let public = fx.assets.find_by_id(&id).await.unwrap().unwrap();
        store_renditions(&fx, &public);
        let h = fx.change_visibility_handler();
        h.handle(env(id, owner, DeliveryVisibility::Signed), t0()).await.unwrap();
        let invalidated = fx.cdn.invalidated_keys().len();

        let back = h.handle(env(id, owner, DeliveryVisibility::Public), t0()).await.unwrap();

        assert_eq!(back.renditions(), public.renditions());
        assert!(objects(&fx).iter().all(|k| !k.starts_with("restricted/")));
        assert_eq!(fx.cdn.invalidated_keys().len(), invalidated, "restricted paths were never on the CDN");
    }

    #[tokio::test]
    async fn with_the_prefix_off_only_the_visibility_changes() {
        let fx = Fixture::new();
        let (id, owner) = fx.ready_asset(MediaKind::PostImage).await;
        let before = fx.assets.find_by_id(&id).await.unwrap().unwrap();
        store_renditions(&fx, &before);
        let keys = objects(&fx);

        let after = fx.change_visibility_handler().handle(env(id, owner, DeliveryVisibility::Signed), t0()).await.unwrap();

        assert_eq!(after.delivery_visibility(), DeliveryVisibility::Signed);
        assert_eq!(after.renditions(), before.renditions());
        assert_eq!(objects(&fx), keys);
        assert!(fx.cdn.invalidated_keys().is_empty());
    }

    /// The flag turned on later: the same visibility again completes the move.
    #[tokio::test]
    async fn the_same_visibility_again_completes_the_move_once_enabled() {
        let mut fx = Fixture::new();
        let (id, owner) = fx.ready_asset(MediaKind::PostImage).await;
        store_renditions(&fx, &fx.assets.find_by_id(&id).await.unwrap().unwrap());
        fx.change_visibility_handler().handle(env(id, owner, DeliveryVisibility::Signed), t0()).await.unwrap();

        restricted_on(&mut fx);
        let after = fx.change_visibility_handler().handle(env(id, owner, DeliveryVisibility::Signed), t0()).await.unwrap();
        assert!(after.renditions().iter().all(|r| r.storage_key().is_restricted()));
        // Nothing left to do: a third call is a no-op.
        let keys = objects(&fx);
        fx.change_visibility_handler().handle(env(id, owner, DeliveryVisibility::Signed), t0()).await.unwrap();
        assert_eq!(objects(&fx), keys);
    }

    #[tokio::test]
    async fn a_private_document_never_moves_nor_goes_public() {
        let mut fx = Fixture::new();
        restricted_on(&mut fx);
        let (id, owner) = fx.ready_signed_asset(MediaKind::PrivateDocument).await;
        let before = fx.assets.find_by_id(&id).await.unwrap().unwrap();
        let keys = fx.store.keys();
        let same = fx.change_visibility_handler().handle(env(id, owner, DeliveryVisibility::Signed), t0()).await.unwrap();
        assert_eq!(same.updated_at(), before.updated_at(), "nothing to do, nothing saved");
        assert_eq!(fx.store.keys(), keys);
        let err = fx.change_visibility_handler().handle(env(id, owner, DeliveryVisibility::Public), t0()).await.unwrap_err();
        assert!(matches!(err, MediaError::DomainViolation { .. }), "{err:?}");
    }

    #[tokio::test]
    async fn a_non_owner_sees_not_found() {
        let fx = Fixture::new();
        let (id, _) = fx.ready_asset(MediaKind::PostImage).await;
        let stranger = OwnerId::from_uuid(Uuid::now_v7());
        let err = fx.change_visibility_handler().handle(env(id, stranger, DeliveryVisibility::Signed), t0()).await.unwrap_err();
        assert!(matches!(err, MediaError::AssetNotFound { .. }));
        let asset = fx.assets.find_by_id(&id).await.unwrap().unwrap();
        assert_eq!(asset.delivery_visibility(), DeliveryVisibility::Public);
    }

    #[tokio::test]
    async fn only_a_ready_asset_changes() {
        let fx = Fixture::new();
        let id = fx.uploaded_asset(MediaKind::PostImage).await;
        let owner = fx.assets.find_by_id(&id).await.unwrap().unwrap().owner_id();
        let err = fx.change_visibility_handler().handle(env(id, owner, DeliveryVisibility::Signed), t0()).await.unwrap_err();
        assert!(matches!(err, MediaError::InvalidStateTransition { .. }), "{err:?}");
        let asset = fx.assets.find_by_id(&id).await.unwrap().unwrap();
        assert_eq!(asset.state(), AssetState::Uploaded);
        assert_eq!(asset.delivery_visibility(), DeliveryVisibility::Public);
    }

    #[tokio::test]
    async fn the_pipeline_writes_a_signed_asset_under_restricted_once_enabled() {
        use crate::application::command::ProcessAssetCommand;
        let mut fx = Fixture::new();
        restricted_on(&mut fx);
        let (id, _) = fx.ready_signed_asset(MediaKind::PostImage).await;
        let asset = fx.assets.find_by_id(&id).await.unwrap().unwrap();
        assert_eq!(asset.state(), AssetState::Ready);
        let prefix = format!("restricted/post-images/{id}/");
        assert!(asset.renditions().iter().all(|r| r.storage_key().as_str().starts_with(&prefix)));

        // A public upload of the same bytes is written to the shared tree.
        let public = fx.uploaded_asset(MediaKind::PostImage).await;
        fx.process_handler().handle(Envelope::new(Uuid::now_v7(), ProcessAssetCommand { asset_id: public }), t0()).await.unwrap();
        let public = fx.assets.find_by_id(&public).await.unwrap().unwrap();
        assert!(public.renditions().iter().all(|r| !r.storage_key().is_restricted()));
    }
}
