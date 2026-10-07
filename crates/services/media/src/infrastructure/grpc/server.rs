use tonic::{Request, Response, Status};

use super::handler::{proto, MediaServiceHandler};
use proto::media_service_server::MediaService;

/// Encoded protobuf descriptor set for gRPC server reflection, emitted by
/// `media-api`'s `build.rs`.
pub const FILE_DESCRIPTOR_SET: &[u8] = media_api::FILE_DESCRIPTOR_SET;

#[tonic::async_trait]
impl MediaService for MediaServiceHandler {
    async fn issue_upload_ticket(
        &self,
        request: Request<proto::IssueUploadTicketRequest>,
    ) -> Result<Response<proto::IssueUploadTicketResponse>, Status> {
        self.issue_upload_ticket(request).await
    }

    async fn commit_upload(
        &self,
        request: Request<proto::CommitUploadRequest>,
    ) -> Result<Response<proto::CommitUploadResponse>, Status> {
        self.commit_upload(request).await
    }

    async fn abort_upload(
        &self,
        request: Request<proto::AbortUploadRequest>,
    ) -> Result<Response<proto::AbortUploadResponse>, Status> {
        self.abort_upload(request).await
    }

    async fn get_asset(
        &self,
        request: Request<proto::GetAssetRequest>,
    ) -> Result<Response<proto::GetAssetResponse>, Status> {
        self.get_asset(request).await
    }

    async fn list_assets_by_owner(
        &self,
        request: Request<proto::ListAssetsByOwnerRequest>,
    ) -> Result<Response<proto::ListAssetsByOwnerResponse>, Status> {
        self.list_assets_by_owner(request).await
    }

    async fn get_private_document_url(
        &self,
        request: Request<proto::GetPrivateDocumentUrlRequest>,
    ) -> Result<Response<proto::GetPrivateDocumentUrlResponse>, Status> {
        self.get_private_document_url(request).await
    }

    async fn delete_asset(
        &self,
        request: Request<proto::DeleteAssetRequest>,
    ) -> Result<Response<proto::DeleteAssetResponse>, Status> {
        self.delete_asset(request).await
    }

    async fn resolve_delivery(
        &self,
        request: Request<proto::ResolveDeliveryRequest>,
    ) -> Result<Response<proto::ResolveDeliveryResponse>, Status> {
        self.resolve_delivery(request).await
    }

    async fn batch_resolve_delivery(
        &self,
        request: Request<proto::BatchResolveDeliveryRequest>,
    ) -> Result<Response<proto::BatchResolveDeliveryResponse>, Status> {
        self.batch_resolve_delivery(request).await
    }

    async fn reprocess(
        &self,
        request: Request<proto::ReprocessRequest>,
    ) -> Result<Response<proto::ReprocessResponse>, Status> {
        self.reprocess(request).await
    }
}
