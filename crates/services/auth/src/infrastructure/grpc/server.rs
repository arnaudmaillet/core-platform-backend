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

    async fn start_mfa_enrollment(
        &self,
        request: Request<proto::StartMfaEnrollmentRequest>,
    ) -> Result<Response<proto::StartMfaEnrollmentResponse>, Status> {
        self.start_mfa_enrollment(request).await
    }

    async fn confirm_mfa_enrollment(
        &self,
        request: Request<proto::ConfirmMfaEnrollmentRequest>,
    ) -> Result<Response<proto::BackupCodesResponse>, Status> {
        self.confirm_mfa_enrollment(request).await
    }

    async fn disable_mfa(
        &self,
        request: Request<proto::DisableMfaRequest>,
    ) -> Result<Response<proto::DisableMfaResponse>, Status> {
        self.disable_mfa(request).await
    }

    async fn regenerate_backup_codes(
        &self,
        request: Request<proto::RegenerateBackupCodesRequest>,
    ) -> Result<Response<proto::BackupCodesResponse>, Status> {
        self.regenerate_backup_codes(request).await
    }

    async fn start_passkey_registration(
        &self,
        request: Request<proto::StartPasskeyRegistrationRequest>,
    ) -> Result<Response<proto::PasskeyRegistrationOptions>, Status> {
        self.start_passkey_registration(request).await
    }

    async fn finish_passkey_registration(
        &self,
        request: Request<proto::FinishPasskeyRegistrationRequest>,
    ) -> Result<Response<proto::Passkey>, Status> {
        self.finish_passkey_registration(request).await
    }

    async fn start_passkey_sign_in(
        &self,
        request: Request<proto::StartPasskeySignInRequest>,
    ) -> Result<Response<proto::PasskeySignInOptions>, Status> {
        self.start_passkey_sign_in(request).await
    }

    async fn list_passkeys(
        &self,
        request: Request<proto::ListPasskeysRequest>,
    ) -> Result<Response<proto::ListPasskeysResponse>, Status> {
        self.list_passkeys(request).await
    }

    async fn remove_passkey(
        &self,
        request: Request<proto::RemovePasskeyRequest>,
    ) -> Result<Response<proto::ListPasskeysResponse>, Status> {
        self.remove_passkey(request).await
    }

    async fn complete_login(
        &self,
        request: Request<proto::CompleteLoginRequest>,
    ) -> Result<Response<proto::LoginResponse>, Status> {
        self.complete_login(request).await
    }

    async fn refresh(
        &self,
        request: Request<proto::RefreshRequest>,
    ) -> Result<Response<proto::RefreshResponse>, Status> {
        self.refresh(request).await
    }

    async fn start_verification(
        &self,
        request: Request<proto::StartVerificationRequest>,
    ) -> Result<Response<proto::StartVerificationResponse>, Status> {
        self.start_verification(request).await
    }

    async fn sign_up(
        &self,
        request: Request<proto::SignUpRequest>,
    ) -> Result<Response<proto::SignUpResponse>, Status> {
        self.sign_up(request).await
    }

    async fn start_device_attestation(
        &self,
        request: Request<proto::StartDeviceAttestationRequest>,
    ) -> Result<Response<proto::StartDeviceAttestationResponse>, Status> {
        self.start_device_attestation(request).await
    }

    async fn start_federated_sign_in(
        &self,
        request: Request<proto::StartFederatedSignInRequest>,
    ) -> Result<Response<proto::StartFederatedSignInResponse>, Status> {
        self.start_federated_sign_in(request).await
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

    async fn change_contact(
        &self,
        request: Request<proto::ChangeContactRequest>,
    ) -> Result<Response<proto::ChangeContactResponse>, Status> {
        self.change_contact(request).await
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
