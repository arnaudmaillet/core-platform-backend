use std::sync::Arc;

use chrono::{DateTime, Utc};
use cqrs::{Envelope, Query, QueryHandler};

use crate::application::port::{AssetRepository, CdnGateway, DeliveryCache};
use crate::domain::aggregate::Asset;
use crate::domain::value_object::{AssetId, AssetState, DeliveryVisibility, RenditionKind};
use crate::error::MediaError;

/// Resolve one asset to a delivery URL set (Plane C, hot read).
#[derive(Debug, Clone)]
pub struct ResolveDeliveryQuery {
    pub asset_id: AssetId,
    /// `None` ⇒ all renditions; `Some(k)` ⇒ just that rendition if present.
    pub preferred: Option<RenditionKind>,
    /// `None` ⇒ the asset's own visibility. Only ever stricter (#919): PUBLIC
    /// for a SIGNED asset still resolves SIGNED.
    pub visibility: Option<DeliveryVisibility>,
}

impl Query for ResolveDeliveryQuery {
    type Response = DeliveredMediaView;
}

/// The resolved delivery view — URLs only. Fails OPEN: a not-yet-READY or
/// quarantined asset yields the BlurHash placeholder with `degraded = true`.
#[derive(Debug, Clone, PartialEq)]
pub struct DeliveredMediaView {
    pub asset_id: AssetId,
    pub state: AssetState,
    pub blurhash: Option<String>,
    pub renditions: Vec<DeliveredRenditionView>,
    pub degraded: bool,
}

#[derive(Debug, Clone, PartialEq)]
pub struct DeliveredRenditionView {
    pub kind: RenditionKind,
    pub url: String,
    pub visibility: DeliveryVisibility,
    pub expires_at: Option<DateTime<Utc>>,
    pub width: u32,
    pub height: u32,
}

pub struct ResolveDeliveryHandler {
    assets: Arc<dyn AssetRepository>,
    cache: Arc<dyn DeliveryCache>,
    cdn: Arc<dyn CdnGateway>,
}

impl ResolveDeliveryHandler {
    pub fn new(
        assets: Arc<dyn AssetRepository>,
        cache: Arc<dyn DeliveryCache>,
        cdn: Arc<dyn CdnGateway>,
    ) -> Self {
        Self { assets, cache, cdn }
    }

    /// Loads via the cache, falling back to the repository (and warming the cache).
    /// A cache error is treated as a miss — the read path fails open.
    async fn load(&self, id: &AssetId) -> Result<Option<Asset>, MediaError> {
        if let Ok(Some(hit)) = self.cache.get(id).await {
            return Ok(Some(hit));
        }
        match self.assets.find_by_id(id).await? {
            Some(asset) => {
                let _ = self.cache.put(&asset).await;
                Ok(Some(asset))
            }
            None => Ok(None),
        }
    }

    /// Builds the delivery view for a loaded asset (the shared core of single and
    /// batch resolution).
    async fn resolve_asset(
        &self,
        asset: &Asset,
        preferred: Option<RenditionKind>,
        visibility: Option<DeliveryVisibility>,
        now: DateTime<Utc>,
    ) -> DeliveredMediaView {
        let blurhash = asset.blurhash().map(|b| b.as_str().to_owned());

        // Not deliverable (processing / failed / quarantined) ⇒ placeholder.
        if !asset.is_deliverable() {
            return DeliveredMediaView {
                asset_id: asset.id(),
                state: asset.state(),
                blurhash,
                renditions: Vec::new(),
                degraded: true,
            };
        }

        // The server decides (#919): the asset's own visibility, which a
        // request may tighten but never relax.
        let visibility = asset.delivery_visibility().stricter(visibility);
        let mut views = Vec::new();
        let mut degraded = false;

        for rendition in asset.renditions() {
            if preferred.is_some_and(|k| k != rendition.kind()) {
                continue;
            }
            // Per-rendition fail-open: a signing/CDN error degrades the result
            // rather than erroring the whole read.
            match self.cdn.resolve(rendition.storage_key(), visibility, now).await {
                Ok(resolved) => views.push(DeliveredRenditionView {
                    kind: rendition.kind(),
                    url: resolved.url,
                    visibility,
                    expires_at: resolved.expires_at,
                    width: rendition.dimensions().width(),
                    height: rendition.dimensions().height(),
                }),
                Err(_) => degraded = true,
            }
        }

        if preferred.is_some() && views.is_empty() {
            degraded = true; // the requested rendition was absent
        }

        DeliveredMediaView {
            asset_id: asset.id(),
            state: asset.state(),
            blurhash,
            renditions: views,
            degraded,
        }
    }

    /// Clock-injected single resolve.
    pub async fn handle_at(
        &self,
        envelope: Envelope<ResolveDeliveryQuery>,
        now: DateTime<Utc>,
    ) -> Result<DeliveredMediaView, MediaError> {
        let q = envelope.payload;
        // A private document (#777) is never delivered: it reads as missing.
        let asset = self
            .load(&q.asset_id)
            .await?
            .filter(|a| !a.kind().is_private())
            .ok_or_else(|| MediaError::AssetNotFound { id: q.asset_id.as_str() })?;
        Ok(self.resolve_asset(&asset, q.preferred, q.visibility, now).await)
    }

    /// Batch resolution for feed/timeline hydration. Unresolvable assets (missing)
    /// are OMITTED — the boundary fails open rather than failing the batch.
    pub async fn resolve_batch(
        &self,
        ids: &[AssetId],
        preferred: Option<RenditionKind>,
        visibility: Option<DeliveryVisibility>,
        now: DateTime<Utc>,
    ) -> Result<Vec<DeliveredMediaView>, MediaError> {
        let mut out = Vec::with_capacity(ids.len());
        for id in ids {
            if let Some(asset) = self.load(id).await?.filter(|a| !a.kind().is_private()) {
                out.push(self.resolve_asset(&asset, preferred, visibility, now).await);
            }
        }
        Ok(out)
    }
}

impl QueryHandler<ResolveDeliveryQuery> for ResolveDeliveryHandler {
    type Error = MediaError;

    async fn handle(
        &self,
        envelope: Envelope<ResolveDeliveryQuery>,
    ) -> Result<DeliveredMediaView, Self::Error> {
        self.handle_at(envelope, Utc::now()).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::application::fakes::{t0, Fixture};
    use crate::domain::value_object::MediaKind;
    use uuid::Uuid;

    fn env(q: ResolveDeliveryQuery) -> Envelope<ResolveDeliveryQuery> {
        Envelope::new(Uuid::now_v7(), q)
    }

    #[tokio::test]
    async fn resolves_public_urls_for_a_ready_asset() {
        let fx = Fixture::new();
        let (asset_id, _owner) = fx.ready_asset(MediaKind::PostImage).await;
        let view = fx
            .resolve_delivery_handler()
            .handle_at(env(ResolveDeliveryQuery { asset_id, preferred: None, visibility: None }), t0())
            .await
            .unwrap();

        assert!(!view.degraded);
        assert!(!view.renditions.is_empty());
        // Public URLs are immutable — no expiry.
        assert!(view.renditions.iter().all(|r| r.expires_at.is_none()));
        assert!(view.renditions.iter().all(|r| r.url.contains("cdn")));
    }

    /// #919: the server decides. A SIGNED asset resolves SIGNED whatever the
    /// request asks (none, or PUBLIC) — singly and in a batch; a PUBLIC one
    /// may still be asked SIGNED (stricter).
    #[tokio::test]
    async fn a_signed_asset_never_resolves_public_whatever_the_request() {
        let fx = Fixture::new();
        let (signed, _) = fx.ready_signed_asset(MediaKind::PostImage).await;
        let handler = fx.resolve_delivery_handler();
        for asked in [None, Some(DeliveryVisibility::Public)] {
            let view = handler
                .handle_at(env(ResolveDeliveryQuery { asset_id: signed, preferred: None, visibility: asked }), t0())
                .await
                .unwrap();
            assert!(!view.renditions.is_empty());
            assert!(
                view.renditions.iter().all(|r| r.visibility == DeliveryVisibility::Signed && r.expires_at.is_some()),
                "{asked:?}"
            );
        }
        let batch = handler.resolve_batch(&[signed], None, Some(DeliveryVisibility::Public), t0()).await.unwrap();
        assert!(batch[0].renditions.iter().all(|r| r.visibility == DeliveryVisibility::Signed));

        let (public, _) = fx.ready_asset(MediaKind::PostImage).await;
        let stricter = handler
            .handle_at(
                env(ResolveDeliveryQuery { asset_id: public, preferred: None, visibility: Some(DeliveryVisibility::Signed) }),
                t0(),
            )
            .await
            .unwrap();
        assert!(stricter.renditions.iter().all(|r| r.visibility == DeliveryVisibility::Signed));
    }

    #[tokio::test]
    async fn signed_visibility_yields_expiring_urls() {
        let fx = Fixture::new();
        let (asset_id, _owner) = fx.ready_asset(MediaKind::Avatar).await;
        let view = fx
            .resolve_delivery_handler()
            .handle_at(
                env(ResolveDeliveryQuery {
                    asset_id,
                    preferred: Some(RenditionKind::Original),
                    visibility: Some(DeliveryVisibility::Signed),
                }),
                t0(),
            )
            .await
            .unwrap();
        assert_eq!(view.renditions.len(), 1);
        assert!(view.renditions[0].expires_at.is_some(), "signed URLs expire");
    }

    #[tokio::test]
    async fn a_processing_asset_fails_open_to_a_placeholder() {
        let fx = Fixture::new();
        // Uploaded (not yet READY) asset.
        let asset_id = fx.uploaded_asset(MediaKind::PostImage).await;
        let view = fx
            .resolve_delivery_handler()
            .handle_at(env(ResolveDeliveryQuery { asset_id, preferred: None, visibility: None }), t0())
            .await
            .unwrap();
        assert!(view.degraded);
        assert!(view.renditions.is_empty());
        assert_eq!(view.state, AssetState::Uploaded);
    }

    #[tokio::test]
    async fn batch_omits_unresolvable_assets() {
        let fx = Fixture::new();
        let (a, _) = fx.ready_asset(MediaKind::PostImage).await;
        let missing = AssetId::new();
        let views = fx
            .resolve_delivery_handler()
            .resolve_batch(&[a, missing], None, None, t0())
            .await
            .unwrap();
        assert_eq!(views.len(), 1, "the missing asset is omitted, not errored");
        assert_eq!(views[0].asset_id, a);
    }
}
