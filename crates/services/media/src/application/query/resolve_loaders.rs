//! A viewer's own loaders (#975): each READY `LOADER` of theirs as a
//! short-lived signed GET of its strip. Nobody else's — a loader is private.

use std::sync::Arc;

use chrono::{DateTime, Duration, Utc};

use crate::application::port::{AssetRepository, CdnGateway, ResolvedUrl};
use crate::domain::value_object::{AssetId, MediaKind, OwnerId, StorageKey, LOADER_FRAME_PX};
use crate::error::MediaError;

/// The most ids one call resolves (an account holds at most 10 uploaded
/// loaders; the slack covers ids the app has not dropped yet).
pub const MAX_LOADERS_PER_CALL: usize = 20;

/// One resolved loader.
#[derive(Debug, Clone, PartialEq)]
pub struct ResolvedLoader {
    pub asset_id: AssetId,
    pub link:     ResolvedUrl,
    /// Its strip's frame count.
    pub frames:   u32,
}

pub struct ResolveLoadersHandler {
    assets: Arc<dyn AssetRepository>,
    cdn:    Arc<dyn CdnGateway>,
    ttl:    Duration,
}

impl ResolveLoadersHandler {
    pub fn new(assets: Arc<dyn AssetRepository>, cdn: Arc<dyn CdnGateway>, ttl: Duration) -> Self {
        Self { assets, cdn, ttl }
    }

    /// `owner`'s READY loaders among `ids`, in order; any other id (another
    /// kind or owner, not ready, quarantined, deleted) is left out.
    pub async fn handle_at(&self, owner: OwnerId, ids: &[AssetId], now: DateTime<Utc>) -> Result<Vec<ResolvedLoader>, MediaError> {
        if ids.len() > MAX_LOADERS_PER_CALL {
            return Err(MediaError::DomainViolation {
                field:   "asset_ids".into(),
                message: format!("at most {MAX_LOADERS_PER_CALL} loaders a call"),
            });
        }
        let mut loaders = Vec::with_capacity(ids.len());
        for id in ids {
            let Some(asset) = self
                .assets
                .find_by_id(id)
                .await?
                .filter(|a| a.kind() == MediaKind::Loader && a.owner_id() == owner && a.is_deliverable())
            else {
                continue;
            };
            let link = self.cdn.signed_download(&StorageKey::private(MediaKind::Loader, *id), self.ttl, now).await?;
            let frames = asset.dimensions().map_or(0, |d| d.width() / LOADER_FRAME_PX);
            loaders.push(ResolvedLoader { asset_id: *id, link, frames });
        }
        Ok(loaders)
    }
}

#[cfg(test)]
mod tests {
    use cqrs::Envelope;
    use uuid::Uuid;

    use super::*;
    use crate::application::command::DeleteAssetCommand;
    use crate::application::fakes::{owner, t0, Fixture};
    use crate::application::query::{ResolveDeliveryHandler, ResolveDeliveryQuery};
    use crate::domain::value_object::AssetState;

    fn handler(fx: &Fixture) -> ResolveLoadersHandler {
        ResolveLoadersHandler::new(Arc::clone(&fx.assets) as _, Arc::clone(&fx.cdn) as _, Duration::minutes(5))
    }

    /// #975: a loader's strip moves under `private/loaders/`, has no
    /// rendition, is never delivered, and only its owner gets a link.
    #[tokio::test]
    async fn a_loader_is_its_owners_alone() {
        let fx = Fixture::new();
        let loader = fx.ready_loader_of(owner(), 24).await;
        let asset = fx.assets.find_by_id(&loader).await.unwrap().unwrap();
        assert_eq!(asset.state(), AssetState::Ready);
        assert!(asset.renditions().is_empty());
        assert_eq!(asset.purge_after(), None, "a loader stays until its owner deletes it");
        let key = StorageKey::private(MediaKind::Loader, loader);
        assert!(key.as_str().starts_with("private/loaders/"));
        assert!(fx.store.keys().contains(&key.as_str().to_owned()), "{:?}", fx.store.keys());

        let resolve = ResolveDeliveryHandler::new(Arc::clone(&fx.assets) as _, Arc::clone(&fx.cache) as _, Arc::clone(&fx.cdn) as _);
        assert!(resolve.resolve_batch(&[loader], None, None, t0()).await.unwrap().is_empty());
        let query = ResolveDeliveryQuery { asset_id: loader, preferred: None, visibility: None };
        assert!(cqrs::QueryHandler::handle(&resolve, Envelope::new(Uuid::now_v7(), query)).await.is_err());
        // Staff links are for verification documents only.
        let staff = fx.private_document_url_handler(Duration::minutes(5));
        assert!(matches!(staff.handle_at(loader, "staff-1", t0()).await, Err(MediaError::AssetNotFound { .. })));

        let stranger = OwnerId::from_uuid(Uuid::from_u128(999));
        let theirs = fx.ready_loader_of(stranger, 12).await;
        let (image, _) = fx.ready_asset(MediaKind::PostImage).await;
        let resolved = handler(&fx).handle_at(owner(), &[image, theirs, loader], t0()).await.unwrap();
        assert_eq!(resolved.len(), 1, "only the caller's own loader");
        assert_eq!((resolved[0].asset_id, resolved[0].frames), (loader, 24));
        assert!(resolved[0].link.url.contains(key.as_str()));
        assert_eq!(resolved[0].link.expires_at, Some(t0() + Duration::minutes(5)));
        assert!(handler(&fx).handle_at(owner(), &vec![loader; MAX_LOADERS_PER_CALL + 1], t0()).await.is_err());

        // Deleted: its strip goes, and it no longer resolves.
        fx.delete_handler()
            .handle(Envelope::new(Uuid::now_v7(), DeleteAssetCommand { asset_id: loader, owner_id: owner() }), t0())
            .await
            .unwrap();
        assert!(!fx.store.keys().contains(&key.as_str().to_owned()));
        assert!(handler(&fx).handle_at(owner(), &[loader], t0()).await.unwrap().is_empty());
    }
}
