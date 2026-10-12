//! media over gRPC (#907): `GetAsset` to check an attachment's asset before it
//! is written, `BatchResolveDelivery` to deliver attachments on read.

use async_trait::async_trait;
use media_api::media_service_client::MediaServiceClient;
use media_api::{AssetState, BatchResolveDeliveryRequest, DeliveredMedia, GetAssetRequest, MediaKind};
use tonic::transport::Channel;
use tonic::Code;

use crate::application::port::{MediaAssetFacts, MediaCatalog};
use crate::domain::value_object::MediaAssetId;
use crate::error::PostError;

/// At most this many assets per `BatchResolveDelivery`.
const MAX_ASSETS_PER_CALL: usize = 100;

/// Delivers media assets on read; fail-open, so a read never waits on more
/// than the channel's deadline nor fails for media.
#[async_trait]
pub trait MediaDelivery: Send + Sync + 'static {
    /// Each asset media could resolve (its blurhash, and its renditions once
    /// ready); the ones it could not are left out.
    async fn deliver(&self, asset_ids: &[String]) -> Result<Vec<DeliveredMedia>, tonic::Status>;
}

/// The media client. The channel is `Arc`-backed, so each call clones the
/// client. It must carry a request timeout: tonic has none by default.
pub struct GrpcMedia {
    media: MediaServiceClient<Channel>,
}

impl GrpcMedia {
    pub fn new(media: Channel) -> Self {
        Self { media: MediaServiceClient::new(media) }
    }
}

#[async_trait]
impl MediaCatalog for GrpcMedia {
    async fn asset(&self, id: &MediaAssetId) -> Result<Option<MediaAssetFacts>, PostError> {
        let request = GetAssetRequest { asset_id: id.as_str() };
        let asset = match self.media.clone().get_asset(request).await {
            Ok(response) => response.into_inner().asset,
            Err(status) if status.code() == Code::NotFound => return Ok(None),
            Err(status) => return Err(PostError::MediaUnavailable { reason: status.to_string() }),
        };
        Ok(asset.map(|asset| MediaAssetFacts {
            attachable: matches!(MediaKind::try_from(asset.kind), Ok(MediaKind::PostImage | MediaKind::Video)),
            usable: !matches!(
                AssetState::try_from(asset.state),
                Ok(AssetState::MediaAssetStateFailed | AssetState::MediaAssetStateQuarantined | AssetState::MediaAssetStateDeleted)
                    | Err(_)
            ),
            owner_account_id: asset.owner_id,
        }))
    }
}

#[async_trait]
impl MediaDelivery for GrpcMedia {
    async fn deliver(&self, asset_ids: &[String]) -> Result<Vec<DeliveredMedia>, tonic::Status> {
        let mut delivered = Vec::with_capacity(asset_ids.len());
        for chunk in asset_ids.chunks(MAX_ASSETS_PER_CALL) {
            let request = BatchResolveDeliveryRequest { asset_ids: chunk.to_vec(), ..Default::default() };
            delivered.extend(self.media.clone().batch_resolve_delivery(request).await?.into_inner().media);
        }
        Ok(delivered)
    }
}
