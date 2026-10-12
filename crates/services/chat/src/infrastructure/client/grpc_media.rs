//! media over gRPC (#919 step 3): `GetAsset` + `ChangeDeliveryVisibility` to
//! check a message's asset and make it SIGNED, `BatchResolveDelivery` to
//! deliver history. Calls carry chat's mesh token: media takes
//! `ChangeDeliveryVisibility` from its intended callers only.

use async_trait::async_trait;
use media_api::media_service_client::MediaServiceClient;
use media_api::{
    AssetState, BatchResolveDeliveryRequest, ChangeDeliveryVisibilityRequest, DeliveredMedia, DeliveryVisibility,
    GetAssetRequest, MediaKind,
};
use tonic::service::interceptor::InterceptedService;
use tonic::transport::Channel;
use tonic::Code;
use transport::grpc::mesh::MeshTokenInterceptor;
use uuid::Uuid;

use crate::application::port::{MediaAssetFacts, MessageMedia};
use crate::error::ChatError;

/// At most this many assets per `BatchResolveDelivery`.
const MAX_ASSETS_PER_CALL: usize = 100;

/// Delivers messages' media on read; fail-open, so history never waits on
/// more than the channel's deadline nor fails for media.
#[async_trait]
pub trait MediaDelivery: Send + Sync + 'static {
    /// Each asset media could resolve; the ones it could not are left out.
    async fn deliver(&self, asset_ids: &[String]) -> Result<Vec<DeliveredMedia>, tonic::Status>;
}

/// The media client. It must carry a request timeout: tonic has none.
pub struct GrpcMedia {
    media: MediaServiceClient<InterceptedService<Channel, MeshTokenInterceptor>>,
}

impl GrpcMedia {
    pub fn new(media: Channel) -> Self {
        Self { media: MediaServiceClient::with_interceptor(media, MeshTokenInterceptor::from_env()) }
    }
}

#[async_trait]
impl MessageMedia for GrpcMedia {
    async fn asset(&self, id: Uuid) -> Result<Option<MediaAssetFacts>, ChatError> {
        let asset = match self.media.clone().get_asset(GetAssetRequest { asset_id: id.to_string() }).await {
            Ok(response) => response.into_inner().asset,
            Err(status) if matches!(status.code(), Code::NotFound | Code::InvalidArgument) => return Ok(None),
            Err(status) => return Err(ChatError::MediaUnavailable { reason: status.to_string() }),
        };
        Ok(asset.map(|asset| MediaAssetFacts {
            attachable: matches!(MediaKind::try_from(asset.kind), Ok(MediaKind::PostImage | MediaKind::Video)),
            usable: matches!(
                AssetState::try_from(asset.state),
                Ok(AssetState::MediaAssetStatePending
                    | AssetState::MediaAssetStateUploaded
                    | AssetState::MediaAssetStateProcessing
                    | AssetState::MediaAssetStateReady)
            ),
            signed: asset.delivery_visibility == DeliveryVisibility::MediaDeliveryVisibilitySigned as i32,
            ready: asset.state == AssetState::MediaAssetStateReady as i32,
            video: asset.kind == MediaKind::Video as i32,
            owner_account_id: asset.owner_id,
        }))
    }

    async fn make_signed(&self, id: Uuid, owner: &str) -> Result<bool, ChatError> {
        let request = ChangeDeliveryVisibilityRequest {
            asset_id: id.to_string(),
            owner_id: owner.to_owned(),
            delivery_visibility: DeliveryVisibility::MediaDeliveryVisibilitySigned as i32,
        };
        match self.media.clone().change_delivery_visibility(request).await {
            Ok(_) => Ok(true),
            // MED-2002: not READY yet.
            Err(status) if status.code() == Code::FailedPrecondition => Ok(false),
            Err(status) => Err(ChatError::MediaUnavailable { reason: status.to_string() }),
        }
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
