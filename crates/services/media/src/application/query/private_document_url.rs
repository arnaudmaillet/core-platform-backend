//! Staff review of verification evidence (#777): a short-lived signed link to
//! a private document. Mesh only — the staff tools call it; no client does.

use std::sync::Arc;

use chrono::{DateTime, Duration, Utc};

use crate::application::port::{AssetRepository, CdnGateway, ResolvedUrl};
use crate::domain::value_object::{AssetId, StorageKey};
use crate::error::MediaError;

pub struct PrivateDocumentUrlHandler {
    assets: Arc<dyn AssetRepository>,
    cdn:    Arc<dyn CdnGateway>,
    ttl:    Duration,
}

impl PrivateDocumentUrlHandler {
    pub fn new(assets: Arc<dyn AssetRepository>, cdn: Arc<dyn CdnGateway>, ttl: Duration) -> Self {
        Self { assets, cdn, ttl }
    }

    /// The document, READY and not taken down, as a signed GET valid for the
    /// policy's signed-URL TTL. Anything else (another kind, not ready,
    /// quarantined, deleted) reads as missing.
    pub async fn handle_at(&self, asset_id: AssetId, now: DateTime<Utc>) -> Result<ResolvedUrl, MediaError> {
        let asset = self
            .assets
            .find_by_id(&asset_id)
            .await?
            .filter(|a| a.kind().is_private() && a.is_deliverable())
            .ok_or_else(|| MediaError::AssetNotFound { id: asset_id.as_str() })?;
        self.cdn.signed_download(&StorageKey::private_document(asset.id()), self.ttl, now).await
    }
}

#[cfg(test)]
mod tests {
    use cqrs::{Envelope, QueryHandler};
    use uuid::Uuid;

    use super::*;
    use crate::application::fakes::{owner, t0, Fixture};
    use crate::application::query::{GetAssetHandler, GetAssetQuery, ResolveDeliveryHandler, ResolveDeliveryQuery};
    use crate::domain::value_object::{AssetState, MediaKind, OwnerId};

    /// A private document end to end: its bytes move under `private/`, it has
    /// no rendition, it is never delivered, its owner (and the mesh) alone see
    /// it, and staff get a short-lived link.
    #[tokio::test]
    async fn a_private_document_is_never_delivered_and_only_staff_get_a_link() {
        let fx = Fixture::new();
        let (doc, _) = fx.ready_asset(MediaKind::PrivateDocument).await;
        let asset = fx.assets.find_by_id(&doc).await.unwrap().unwrap();
        assert_eq!(asset.state(), AssetState::Ready);
        assert!(asset.renditions().is_empty());
        let key = StorageKey::private_document(doc);
        assert!(fx.store.keys().contains(&key.as_str().to_owned()), "{:?}", fx.store.keys());
        assert!(fx.store.keys().iter().all(|k| !k.starts_with("uploads/")), "the staging copy moved");

        // ResolveDelivery: missing, single or batch.
        let resolve = ResolveDeliveryHandler::new(Arc::clone(&fx.assets) as _, Arc::clone(&fx.cache) as _, Arc::clone(&fx.cdn) as _);
        let query = ResolveDeliveryQuery { asset_id: doc, preferred: None, visibility: None };
        assert!(matches!(
            resolve.handle(Envelope::new(Uuid::now_v7(), query)).await,
            Err(MediaError::AssetNotFound { .. })
        ));
        assert!(resolve.resolve_batch(&[doc], None, None, t0()).await.unwrap().is_empty());

        // GetAsset: the owner and the mesh; anyone else reads it as missing.
        let get = GetAssetHandler::new(Arc::clone(&fx.assets) as _);
        let as_caller = |caller| Envelope::new(Uuid::now_v7(), GetAssetQuery { asset_id: doc, caller });
        assert!(get.handle(as_caller(Some(owner()))).await.is_ok());
        assert!(get.handle(as_caller(None)).await.is_ok());
        let stranger = OwnerId::from_uuid(Uuid::from_u128(999));
        assert!(matches!(get.handle(as_caller(Some(stranger))).await, Err(MediaError::AssetNotFound { .. })));

        // Staff: a signed link to the private key, for 5 minutes; not for a post image.
        let staff = PrivateDocumentUrlHandler::new(Arc::clone(&fx.assets) as _, Arc::clone(&fx.cdn) as _, Duration::minutes(5));
        let link = staff.handle_at(doc, t0()).await.unwrap();
        assert!(link.url.contains(key.as_str()) && link.expires_at == Some(t0() + Duration::minutes(5)), "{link:?}");
        let (image, _) = fx.ready_asset(MediaKind::PostImage).await;
        assert!(matches!(staff.handle_at(image, t0()).await, Err(MediaError::AssetNotFound { .. })));

        // Deleting it removes the private object too.
        fx.delete_handler()
            .handle(Envelope::new(Uuid::now_v7(), crate::application::command::DeleteAssetCommand { asset_id: doc, owner_id: owner() }), t0())
            .await
            .unwrap();
        assert!(!fx.store.keys().contains(&key.as_str().to_owned()));
    }
}
