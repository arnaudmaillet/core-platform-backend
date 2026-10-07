use std::sync::Arc;

use chrono::{DateTime, Duration, Utc};
use cqrs::{Envelope, Query, QueryHandler};

use crate::application::port::{AssetRepository, CdnGateway, ResolvedUrl};
use crate::domain::aggregate::Asset;
use crate::domain::value_object::{AssetId, OwnerId, RenditionKind, StorageKey};
use crate::error::MediaError;

/// The longest a download URL handed out for an export lives (S3's cap).
pub const MAX_DOWNLOAD_TTL_SECS: i64 = 7 * 24 * 3600;

/// An account's assets with a signed download of each original — mesh only:
/// the GDPR data export (#653). An asset that is not deliverable (still
/// processing, failed, quarantined) comes without a URL.
#[derive(Debug, Clone)]
pub struct ListAssetsByOwnerQuery {
    pub owner_id: OwnerId,
    pub limit:    i64,
    pub after:    Option<AssetId>,
    /// How long each URL lives (clamped to 60 s – 7 days).
    pub url_ttl:  Duration,
}

impl Query for ListAssetsByOwnerQuery {
    type Response = Vec<OwnedAsset>;
}

/// One asset of the account and, when deliverable, its original's download.
#[derive(Debug, Clone)]
pub struct OwnedAsset {
    pub asset:    Asset,
    pub download: Option<ResolvedUrl>,
}

pub struct ListAssetsByOwnerHandler {
    assets: Arc<dyn AssetRepository>,
    cdn:    Arc<dyn CdnGateway>,
}

impl ListAssetsByOwnerHandler {
    pub fn new(assets: Arc<dyn AssetRepository>, cdn: Arc<dyn CdnGateway>) -> Self {
        Self { assets, cdn }
    }

    pub async fn handle_at(&self, query: ListAssetsByOwnerQuery, now: DateTime<Utc>) -> Result<Vec<OwnedAsset>, MediaError> {
        let ttl = query.url_ttl.clamp(Duration::seconds(60), Duration::seconds(MAX_DOWNLOAD_TTL_SECS));
        let assets = self.assets.list_by_owner(&query.owner_id, query.limit, query.after.as_ref()).await?;
        let mut owned = Vec::with_capacity(assets.len());
        for asset in assets {
            // A private document (#777) is the holder's too: its own key.
            let original = if !asset.is_deliverable() {
                None
            } else if asset.kind().is_private() {
                Some(StorageKey::private_document(asset.id()))
            } else {
                asset
                    .renditions()
                    .iter()
                    .find(|r| r.kind() == RenditionKind::Original)
                    .map(|r| r.storage_key().clone())
            };
            let download = match original {
                Some(key) => Some(self.cdn.signed_download(&key, ttl, now).await?),
                None => None,
            };
            owned.push(OwnedAsset { asset, download });
        }
        Ok(owned)
    }
}

impl QueryHandler<ListAssetsByOwnerQuery> for ListAssetsByOwnerHandler {
    type Error = MediaError;

    async fn handle(&self, envelope: Envelope<ListAssetsByOwnerQuery>) -> Result<Vec<OwnedAsset>, MediaError> {
        self.handle_at(envelope.payload, Utc::now()).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::application::fakes::{owner, t0, Fixture};
    use crate::domain::value_object::MediaKind;

    fn handler(fx: &Fixture) -> ListAssetsByOwnerHandler {
        ListAssetsByOwnerHandler::new(Arc::clone(&fx.assets) as _, Arc::clone(&fx.cdn) as _)
    }

    /// #653: every asset of the account, by id, a deliverable one with a
    /// signed download of its original (TTL capped at 7 days), one still in
    /// flight without.
    #[tokio::test]
    async fn an_accounts_assets_come_by_id_with_downloads_for_the_deliverable() {
        let fx = Fixture::new();
        let ready_a = fx.seed_ready_asset(&"a".repeat(64)).await;
        let pending = fx.reserve_only(MediaKind::PostImage).await;
        let ready_b = fx.seed_ready_asset(&"b".repeat(64)).await;
        let query = |after: Option<AssetId>, limit| ListAssetsByOwnerQuery {
            owner_id: owner(),
            limit,
            after,
            url_ttl: Duration::days(30),
        };

        let all = handler(&fx).handle_at(query(None, 10), t0()).await.unwrap();
        let mut expected = vec![ready_a, pending, ready_b];
        expected.sort_by_key(|id| id.as_uuid());
        assert_eq!(all.iter().map(|o| o.asset.id()).collect::<Vec<_>>(), expected);
        for owned in &all {
            match owned.asset.id() == pending {
                true => assert!(owned.download.is_none(), "not deliverable: no URL"),
                false => {
                    let download = owned.download.as_ref().expect("a download");
                    assert_eq!(download.expires_at, Some(t0() + Duration::days(7)), "capped at 7 days");
                    assert!(download.url.contains("original"), "{}", download.url);
                }
            }
        }

        let first = handler(&fx).handle_at(query(None, 2), t0()).await.unwrap();
        let rest = handler(&fx).handle_at(query(Some(first[1].asset.id()), 2), t0()).await.unwrap();
        assert_eq!(rest.iter().map(|o| o.asset.id()).collect::<Vec<_>>(), vec![expected[2]]);
    }
}
