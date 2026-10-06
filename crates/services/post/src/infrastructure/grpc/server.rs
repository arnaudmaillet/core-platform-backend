use tonic::{Request, Response, Status};

use cqrs::{CommandBus, QueryBus};

use super::handler::post_service_handler::{proto, PostServiceHandler};
use proto::post_service_server::PostService;

/// Encoded protobuf descriptor set for gRPC server reflection, emitted by
/// `build.rs`. Registered by the service's runtime adapter ([`crate::service`]).
pub const FILE_DESCRIPTOR_SET: &[u8] = post_api::FILE_DESCRIPTOR_SET;

#[tonic::async_trait]
impl<CB, QB> PostService for PostServiceHandler<CB, QB>
where
    CB: CommandBus + Send + Sync + 'static,
    QB: QueryBus + Send + Sync + 'static,
{
    // ── Commands ──────────────────────────────────────────────────────────────

    async fn create_post(
        &self,
        request: Request<proto::CreatePostRequest>,
    ) -> Result<Response<proto::CreatePostResponse>, Status> {
        self.create_post(request).await
    }

    async fn publish_post(
        &self,
        request: Request<proto::PublishPostRequest>,
    ) -> Result<Response<proto::CommandResponse>, Status> {
        self.publish_post(request).await
    }

    async fn update_post(
        &self,
        request: Request<proto::UpdatePostRequest>,
    ) -> Result<Response<proto::CommandResponse>, Status> {
        self.update_post(request).await
    }

    async fn restore_post(
        &self,
        request: Request<proto::RestorePostRequest>,
    ) -> Result<Response<proto::CommandResponse>, Status> {
        self.restore_post(request).await
    }

    async fn list_recently_deleted(
        &self,
        request: Request<proto::ListRecentlyDeletedRequest>,
    ) -> Result<Response<proto::ListRecentlyDeletedResponse>, Status> {
        self.list_recently_deleted(request).await
    }

    async fn delete_post(
        &self,
        request: Request<proto::DeletePostRequest>,
    ) -> Result<Response<proto::CommandResponse>, Status> {
        self.delete_post(request).await
    }

    // ── Queries ───────────────────────────────────────────────────────────────

    async fn get_post(
        &self,
        request: Request<proto::GetPostRequest>,
    ) -> Result<Response<proto::PostView>, Status> {
        self.get_post(request).await
    }

    async fn batch_get_like_visibility(
        &self,
        request: Request<proto::BatchGetLikeVisibilityRequest>,
    ) -> Result<Response<proto::BatchGetLikeVisibilityResponse>, Status> {
        self.batch_get_like_visibility(request).await
    }

    async fn list_posts_by_profile(
        &self,
        request: Request<proto::ListPostsByProfileRequest>,
    ) -> Result<Response<proto::ListPostsByProfileResponse>, Status> {
        self.list_posts_by_profile(request).await
    }
}
