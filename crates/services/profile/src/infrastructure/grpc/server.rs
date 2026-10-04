use tonic::{Request, Response, Status};

use cqrs::{CommandBus, QueryBus};

use super::handler::profile_service_handler::{proto, ProfileServiceHandler};
use proto::profile_service_server::ProfileService;

/// Encoded protobuf descriptor set for gRPC server reflection, emitted by
/// `build.rs`. Registered by the service's runtime adapter ([`crate::service`]).
pub const FILE_DESCRIPTOR_SET: &[u8] = profile_api::FILE_DESCRIPTOR_SET;

#[tonic::async_trait]
impl<CB, QB> ProfileService for ProfileServiceHandler<CB, QB>
where
    CB: CommandBus + Send + Sync + 'static,
    QB: QueryBus + Send + Sync + 'static,
{
    // ── Profile lifecycle ─────────────────────────────────────────────────────

    async fn create_profile(
        &self,
        request: Request<proto::CreateProfileRequest>,
    ) -> Result<Response<proto::CommandResponse>, Status> {
        self.create_profile(request).await
    }

    async fn update_profile(
        &self,
        request: Request<proto::UpdateProfileRequest>,
    ) -> Result<Response<proto::CommandResponse>, Status> {
        self.update_profile(request).await
    }

    async fn change_handle(
        &self,
        request: Request<proto::ChangeHandleRequest>,
    ) -> Result<Response<proto::CommandResponse>, Status> {
        self.change_handle(request).await
    }

    async fn update_avatar(
        &self,
        request: Request<proto::UpdateAvatarRequest>,
    ) -> Result<Response<proto::CommandResponse>, Status> {
        self.update_avatar(request).await
    }

    async fn update_banner(
        &self,
        request: Request<proto::UpdateBannerRequest>,
    ) -> Result<Response<proto::CommandResponse>, Status> {
        self.update_banner(request).await
    }

    async fn set_feed_settings(
        &self,
        request: Request<proto::SetFeedSettingsRequest>,
    ) -> Result<Response<proto::CommandResponse>, Status> {
        self.set_feed_settings(request).await
    }

    async fn set_tab_settings(
        &self,
        request: Request<proto::SetTabSettingsRequest>,
    ) -> Result<Response<proto::CommandResponse>, Status> {
        self.set_tab_settings(request).await
    }

    async fn set_comment_filters(
        &self,
        request: Request<proto::SetCommentFiltersRequest>,
    ) -> Result<Response<proto::CommandResponse>, Status> {
        self.set_comment_filters(request).await
    }

    async fn set_discovery_settings(
        &self,
        request: Request<proto::SetDiscoverySettingsRequest>,
    ) -> Result<Response<proto::CommandResponse>, Status> {
        self.set_discovery_settings(request).await
    }

    async fn set_location_settings(
        &self,
        request: Request<proto::SetLocationSettingsRequest>,
    ) -> Result<Response<proto::CommandResponse>, Status> {
        self.set_location_settings(request).await
    }

    async fn set_interaction_settings(
        &self,
        request: Request<proto::SetInteractionSettingsRequest>,
    ) -> Result<Response<proto::CommandResponse>, Status> {
        self.set_interaction_settings(request).await
    }

    async fn set_visibility(
        &self,
        request: Request<proto::SetVisibilityRequest>,
    ) -> Result<Response<proto::CommandResponse>, Status> {
        self.set_visibility(request).await
    }

    async fn verify_profile(
        &self,
        request: Request<proto::VerifyProfileRequest>,
    ) -> Result<Response<proto::CommandResponse>, Status> {
        self.verify_profile(request).await
    }

    async fn hide_profile(
        &self,
        request: Request<proto::HideProfileRequest>,
    ) -> Result<Response<proto::CommandResponse>, Status> {
        self.hide_profile(request).await
    }

    async fn restore_profile(
        &self,
        request: Request<proto::RestoreProfileRequest>,
    ) -> Result<Response<proto::CommandResponse>, Status> {
        self.restore_profile(request).await
    }

    async fn delete_profile(
        &self,
        request: Request<proto::DeleteProfileRequest>,
    ) -> Result<Response<proto::CommandResponse>, Status> {
        self.delete_profile(request).await
    }

    // ── Queries ───────────────────────────────────────────────────────────────

    async fn get_profile_by_id(
        &self,
        request: Request<proto::GetProfileByIdRequest>,
    ) -> Result<Response<proto::ProfileView>, Status> {
        self.get_profile_by_id(request).await
    }

    async fn get_profile_by_handle(
        &self,
        request: Request<proto::GetProfileByHandleRequest>,
    ) -> Result<Response<proto::ProfileView>, Status> {
        self.get_profile_by_handle(request).await
    }

    async fn check_handle_availability(
        &self,
        request: Request<proto::CheckHandleAvailabilityRequest>,
    ) -> Result<Response<proto::CheckHandleAvailabilityResponse>, Status> {
        self.check_handle_availability(request).await
    }

    async fn list_profiles_by_account(
        &self,
        request: Request<proto::ListProfilesByAccountRequest>,
    ) -> Result<Response<proto::ListProfilesByAccountResponse>, Status> {
        self.list_profiles_by_account(request).await
    }
}
