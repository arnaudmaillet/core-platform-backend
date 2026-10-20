//! media on the mesh: [`MediaPreviews`] over `BatchResolveDelivery` (#963).

use std::collections::HashMap;

use async_trait::async_trait;
use media_api::media_service_client::MediaServiceClient;
use media_api::{BatchResolveDeliveryRequest, DeliveryVisibility, RenditionKind};
use tonic::transport::Channel;
use uuid::Uuid;

use crate::application::port::MediaPreviews;
use crate::error::GeoDiscoveryError;

/// At most this many assets per `BatchResolveDelivery`.
const MAX_ASSETS_PER_CALL: usize = 100;

pub struct GrpcMediaPreviews {
    client: MediaServiceClient<Channel>,
}

impl GrpcMediaPreviews {
    pub fn new(channel: Channel) -> Self {
        Self { client: MediaServiceClient::new(channel) }
    }
}

#[async_trait]
impl MediaPreviews for GrpcMediaPreviews {
    async fn previews(&self, asset_ids: &[Uuid]) -> Result<HashMap<Uuid, String>, GeoDiscoveryError> {
        let mut previews = HashMap::new();
        for chunk in asset_ids.chunks(MAX_ASSETS_PER_CALL) {
            let request = BatchResolveDeliveryRequest {
                asset_ids: chunk.iter().map(Uuid::to_string).collect(),
                ..Default::default()
            };
            let media = self
                .client
                .clone()
                .batch_resolve_delivery(request)
                .await
                .map_err(|status| GeoDiscoveryError::MediaUnavailable { reason: status.message().to_owned() })?
                .into_inner()
                .media;
            for delivered in media {
                // A SIGNED video's cookies cannot ride with a cover.
                if !delivered.cdn_cookies.is_empty() {
                    continue;
                }
                let Ok(asset) = Uuid::parse_str(&delivered.asset_id) else { continue };
                let preview = delivered.renditions.into_iter().find(|r| {
                    r.kind == RenditionKind::MediaRenditionKindPreview as i32
                        && r.visibility == DeliveryVisibility::MediaDeliveryVisibilityPublic as i32
                        && r.url_expires_at.is_none()
                });
                if let Some(preview) = preview {
                    previews.insert(asset, preview.url);
                }
            }
        }
        Ok(previews)
    }
}
