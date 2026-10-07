use std::sync::Arc;

use cqrs::{Envelope, Query, QueryHandler};

use crate::application::port::AssetRepository;
use crate::domain::aggregate::Asset;
use crate::domain::value_object::{AssetId, OwnerId};
use crate::error::MediaError;

/// Fetch an asset's metadata + rendition catalog (no URLs — use `ResolveDelivery`).
#[derive(Debug, Clone)]
pub struct GetAssetQuery {
    pub asset_id: AssetId,
    /// The edge caller's account; `None` over the mesh. A private document
    /// (#777) is its owner's only: anyone else reads it as missing.
    pub caller:   Option<OwnerId>,
}

impl Query for GetAssetQuery {
    type Response = Asset;
}

pub struct GetAssetHandler {
    assets: Arc<dyn AssetRepository>,
}

impl GetAssetHandler {
    pub fn new(assets: Arc<dyn AssetRepository>) -> Self {
        Self { assets }
    }
}

impl QueryHandler<GetAssetQuery> for GetAssetHandler {
    type Error = MediaError;

    async fn handle(&self, envelope: Envelope<GetAssetQuery>) -> Result<Asset, Self::Error> {
        let GetAssetQuery { asset_id: id, caller } = envelope.payload;
        self.assets
            .find_by_id(&id)
            .await?
            .filter(|a| !a.kind().is_private() || caller.is_none_or(|c| c == a.owner_id()))
            .ok_or_else(|| MediaError::AssetNotFound { id: id.as_str() })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::application::fakes::Fixture;
    use crate::domain::value_object::MediaKind;
    use uuid::Uuid;

    #[tokio::test]
    async fn returns_a_stored_asset() {
        let fx = Fixture::new();
        let (asset_id, _owner) = fx.ready_asset(MediaKind::PostImage).await;
        let env = Envelope::new(Uuid::now_v7(), GetAssetQuery { asset_id, caller: None });
        let asset = fx.get_asset_handler().handle(env).await.unwrap();
        assert_eq!(asset.id(), asset_id);
    }

    #[tokio::test]
    async fn missing_asset_is_not_found() {
        let fx = Fixture::new();
        let env = Envelope::new(Uuid::now_v7(), GetAssetQuery { asset_id: AssetId::new(), caller: None });
        let err = fx.get_asset_handler().handle(env).await.unwrap_err();
        assert!(matches!(err, MediaError::AssetNotFound { .. }));
    }
}
