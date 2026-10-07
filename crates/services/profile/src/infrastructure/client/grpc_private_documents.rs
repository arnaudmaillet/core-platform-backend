//! [`PrivateDocuments`] over media's `GetAsset` on the mesh (#777): the mesh
//! reads a private document's metadata, so profile checks it is the requester's
//! own, ready `PRIVATE_DOCUMENT`.

use async_trait::async_trait;
use media_api::media_service_client::MediaServiceClient;
use media_api::{AssetState, GetAssetRequest, MediaKind};
use tonic::transport::Channel;
use tonic::Code;

use crate::application::port::PrivateDocuments;
use crate::domain::value_object::AccountId;
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
