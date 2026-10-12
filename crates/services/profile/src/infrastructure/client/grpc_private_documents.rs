//! media on the mesh. [`PrivateDocuments`] over `GetAsset` (#777): the mesh
//! reads a private document's metadata, so profile checks it is the requester's
//! own, ready `PRIVATE_DOCUMENT`. [`MediaImages`] likewise for an avatar or
//! banner (#907), and [`MediaDelivery`] (`BatchResolveDelivery`) delivers them
//! on read.

use async_trait::async_trait;
use media_api::media_service_client::MediaServiceClient;
use media_api::{AssetState, BatchResolveDeliveryRequest, DeliveredMedia, GetAssetRequest, MediaKind};
use tonic::transport::Channel;
use tonic::Code;

use crate::application::port::{MediaImages, PrivateDocuments};
use crate::domain::value_object::{AccountId, MediaAssetId};
use crate::error::ProfileError;

pub struct GrpcPrivateDocuments {
    client: MediaServiceClient<Channel>,
}

impl GrpcPrivateDocuments {
    pub fn new(channel: Channel) -> Self {
        Self { client: MediaServiceClient::new(channel) }
    }
}

#[async_trait]
impl PrivateDocuments for GrpcPrivateDocuments {
    async fn check_owned(&self, asset_id: &str, account: &AccountId) -> Result<(), ProfileError> {
        let invalid = || ProfileError::VerificationDocumentInvalid { id: asset_id.to_owned() };
        let request = GetAssetRequest { asset_id: asset_id.to_owned() };
        let asset = match self.client.clone().get_asset(request).await {
            Ok(response) => response.into_inner().asset.ok_or_else(invalid)?,
            Err(status) if matches!(status.code(), Code::NotFound | Code::InvalidArgument) => {
                return Err(invalid());
            }
            Err(status) => return Err(ProfileError::MediaUnavailable { reason: status.message().to_owned() }),
        };
        let owned = asset.owner_id == account.to_string()
            && asset.kind == MediaKind::PrivateDocument as i32
            && asset.state == AssetState::MediaAssetStateReady as i32;
        if owned { Ok(()) } else { Err(invalid()) }
    }
}

#[async_trait]
impl MediaImages for GrpcPrivateDocuments {
    async fn check_image(&self, asset: &MediaAssetId, account: &AccountId) -> Result<(), ProfileError> {
        let not_usable = || ProfileError::MediaAssetNotUsable { id: asset.as_str() };
        let request = GetAssetRequest { asset_id: asset.as_str() };
        let asset = match self.client.clone().get_asset(request).await {
            Ok(response) => response.into_inner().asset.ok_or_else(not_usable)?,
            Err(status) if matches!(status.code(), Code::NotFound | Code::InvalidArgument) => return Err(not_usable()),
            Err(status) => return Err(ProfileError::MediaUnavailable { reason: status.message().to_owned() }),
        };
        let image = matches!(MediaKind::try_from(asset.kind), Ok(MediaKind::Avatar | MediaKind::PostImage));
        let usable = matches!(
            AssetState::try_from(asset.state),
            Ok(AssetState::MediaAssetStatePending
                | AssetState::MediaAssetStateUploaded
                | AssetState::MediaAssetStateProcessing
                | AssetState::MediaAssetStateReady)
        );
        if image && usable && asset.owner_id == account.to_string() { Ok(()) } else { Err(not_usable()) }
    }
}

/// Delivers avatars and banners on read (#907); fail-open, so a read never
/// waits on more than the channel's deadline nor fails for media.
#[async_trait]
pub trait MediaDelivery: Send + Sync + 'static {
    /// Each asset media could resolve (its blurhash, and its renditions once
    /// ready); the ones it could not are left out.
    async fn deliver(&self, asset_ids: &[String]) -> Result<Vec<DeliveredMedia>, tonic::Status>;
}

/// At most this many assets per `BatchResolveDelivery`.
const MAX_ASSETS_PER_CALL: usize = 100;

#[async_trait]
impl MediaDelivery for GrpcPrivateDocuments {
    async fn deliver(&self, asset_ids: &[String]) -> Result<Vec<DeliveredMedia>, tonic::Status> {
        let mut delivered = Vec::with_capacity(asset_ids.len());
        for chunk in asset_ids.chunks(MAX_ASSETS_PER_CALL) {
            let request = BatchResolveDeliveryRequest { asset_ids: chunk.to_vec(), ..Default::default() };
            delivered.extend(self.client.clone().batch_resolve_delivery(request).await?.into_inner().media);
        }
        Ok(delivered)
    }
}
