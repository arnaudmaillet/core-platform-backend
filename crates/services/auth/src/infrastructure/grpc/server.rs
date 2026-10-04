use tonic::{Request, Response, Status};

use super::handler::auth_service_handler::{proto, AuthServiceHandler};

// The tonic-generated trait from the bundled proto module.
use proto::auth_service_server::AuthService;

/// Encoded protobuf descriptor set for gRPC server reflection, emitted by
/// `auth-api`'s `build.rs`. Registered by the service's runtime adapter.
pub const FILE_DESCRIPTOR_SET: &[u8] = auth_api::FILE_DESCRIPTOR_SET;

#[tonic::async_trait]
impl AuthService for AuthServiceHandler {
    async fn login(
        &self,
        request: Request<proto::LoginRequest>,
    ) -> Result<Response<proto::LoginResponse>, Status> {
        self.login(request).await
    }

    async fn refresh(
        &self,
        request: Request<proto::RefreshRequest>,
    ) -> Result<Response<proto::RefreshResponse>, Status> {
        self.refresh(request).await
    }

    async fn sign_up(
        &self,
        request: Request<proto::SignUpRequest>,
    ) -> Result<Response<proto::SignUpResponse>, Status> {
        self.sign_up(request).await
    }

    async fn logout(
        &self,
        request: Request<proto::LogoutRequest>,
    ) -> Result<Response<proto::LogoutResponse>, Status> {
        self.logout(request).await
    }

    async fn logout_all_sessions(
        &self,
        request: Request<proto::LogoutAllSessionsRequest>,
    ) -> Result<Response<proto::LogoutAllSessionsResponse>, Status> {
        self.logout_all_sessions(request).await
    }

    async fn introspect(
        &self,
        request: Request<proto::IntrospectRequest>,
    ) -> Result<Response<proto::IntrospectResponse>, Status> {
        self.introspect(request).await
    }

    async fn change_password(
        &self,
        request: Request<proto::ChangePasswordRequest>,
    ) -> Result<Response<proto::ChangePasswordResponse>, Status> {
        self.change_password(request).await
    }

    async fn verify_credentials(
        &self,
        request: Request<proto::VerifyCredentialsRequest>,
    ) -> Result<Response<proto::VerifyCredentialsResponse>, Status> {
        self.verify_credentials(request).await
    }

    async fn list_sessions(
        &self,
        request: Request<proto::ListSessionsRequest>,
    ) -> Result<Response<proto::ListSessionsResponse>, Status> {
        self.list_sessions(request).await
    }

    async fn start_guest_session(
        &self,
        request: Request<proto::StartGuestSessionRequest>,
    ) -> Result<Response<proto::StartGuestSessionResponse>, Status> {
        self.start_guest_session(request).await
    }
}
