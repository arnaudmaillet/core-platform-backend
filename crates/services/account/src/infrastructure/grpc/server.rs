use tonic::{Request, Response, Status};

use cqrs::{CommandBus, QueryBus};

use super::handler::account_service_handler::{proto, AccountServiceHandler};

// Import the tonic-generated trait from the bundled proto module.
use proto::account_service_server::AccountService;

/// Encoded protobuf descriptor set for gRPC server reflection, emitted by
/// `build.rs`. Registered by the service's runtime adapter ([`crate::service`]).
pub const FILE_DESCRIPTOR_SET: &[u8] = account_api::FILE_DESCRIPTOR_SET;

#[tonic::async_trait]
impl<CB, QB> AccountService for AccountServiceHandler<CB, QB>
where
    CB: CommandBus + Send + Sync + 'static,
    QB: QueryBus + Send + Sync + 'static,
{
    // ── Registration & identity ───────────────────────────────────────────────

    async fn create_account(
        &self,
        request: Request<proto::CreateAccountRequest>,
    ) -> Result<Response<proto::CommandResponse>, Status> {
        self.create_account(request).await
    }

    async fn change_email(
        &self,
        request: Request<proto::ChangeEmailRequest>,
    ) -> Result<Response<proto::CommandResponse>, Status> {
        self.change_email(request).await
    }

    async fn change_phone(
        &self,
        request: Request<proto::ChangePhoneRequest>,
    ) -> Result<Response<proto::CommandResponse>, Status> {
        self.change_phone(request).await
    }

    async fn verify_email(
        &self,
        request: Request<proto::VerifyEmailRequest>,
    ) -> Result<Response<proto::CommandResponse>, Status> {
        self.verify_email(request).await
    }

    async fn verify_phone(
        &self,
        request: Request<proto::VerifyPhoneRequest>,
    ) -> Result<Response<proto::CommandResponse>, Status> {
        self.verify_phone(request).await
    }

    // ── Credentials ───────────────────────────────────────────────────────────

    async fn change_password(
        &self,
        request: Request<proto::ChangePasswordRequest>,
    ) -> Result<Response<proto::CommandResponse>, Status> {
        self.change_password(request).await
    }

    // ── MFA ───────────────────────────────────────────────────────────────────

    async fn enroll_mfa(
        &self,
        request: Request<proto::EnrollMfaRequest>,
    ) -> Result<Response<proto::CommandResponse>, Status> {
        self.enroll_mfa(request).await
    }

    async fn revoke_mfa(
        &self,
        request: Request<proto::RevokeMfaRequest>,
    ) -> Result<Response<proto::CommandResponse>, Status> {
        self.revoke_mfa(request).await
    }

    async fn get_mfa_secret(
        &self,
        request: Request<proto::GetMfaSecretRequest>,
    ) -> Result<Response<proto::MfaSecretView>, Status> {
        self.get_mfa_secret(request).await
    }

    async fn consume_recovery_code(
        &self,
        request: Request<proto::ConsumeRecoveryCodeRequest>,
    ) -> Result<Response<proto::CommandResponse>, Status> {
        self.consume_recovery_code(request).await
    }

    async fn replace_recovery_codes(
        &self,
        request: Request<proto::ReplaceRecoveryCodesRequest>,
    ) -> Result<Response<proto::CommandResponse>, Status> {
        self.replace_recovery_codes(request).await
    }

    // ── KYC ───────────────────────────────────────────────────────────────────

    async fn update_kyc_status(
        &self,
        request: Request<proto::UpdateKycStatusRequest>,
    ) -> Result<Response<proto::CommandResponse>, Status> {
        self.update_kyc_status(request).await
    }

    // ── Lifecycle ─────────────────────────────────────────────────────────────

    async fn suspend_account(
        &self,
        request: Request<proto::SuspendAccountRequest>,
    ) -> Result<Response<proto::CommandResponse>, Status> {
        self.suspend_account(request).await
    }

    async fn reactivate_account(
        &self,
        request: Request<proto::ReactivateAccountRequest>,
    ) -> Result<Response<proto::CommandResponse>, Status> {
        self.reactivate_account(request).await
    }

    async fn deactivate_account(
        &self,
        request: Request<proto::DeactivateAccountRequest>,
    ) -> Result<Response<proto::CommandResponse>, Status> {
        self.deactivate_account(request).await
    }

    async fn resume_deactivated_account(
        &self,
        request: Request<proto::ResumeDeactivatedAccountRequest>,
    ) -> Result<Response<proto::CommandResponse>, Status> {
        self.resume_deactivated_account(request).await
    }

    // ── Session tracking ──────────────────────────────────────────────────────

    async fn record_login(
        &self,
        request: Request<proto::RecordLoginRequest>,
    ) -> Result<Response<proto::CommandResponse>, Status> {
        self.record_login(request).await
    }

    async fn record_failed_login(
        &self,
        request: Request<proto::RecordFailedLoginRequest>,
    ) -> Result<Response<proto::CommandResponse>, Status> {
        self.record_failed_login(request).await
    }

    // ── GDPR / compliance ─────────────────────────────────────────────────────

    async fn request_gdpr_deletion(
        &self,
        request: Request<proto::RequestGdprDeletionRequest>,
    ) -> Result<Response<proto::CommandResponse>, Status> {
        self.request_gdpr_deletion(request).await
    }

    async fn cancel_gdpr_deletion(
        &self,
        request: Request<proto::CancelGdprDeletionRequest>,
    ) -> Result<Response<proto::CommandResponse>, Status> {
        self.cancel_gdpr_deletion(request).await
    }

    async fn anonymize_account(
        &self,
        request: Request<proto::AnonymizeAccountRequest>,
    ) -> Result<Response<proto::CommandResponse>, Status> {
        self.anonymize_account(request).await
    }

    async fn request_data_export(
        &self,
        request: Request<proto::RequestDataExportRequest>,
    ) -> Result<Response<proto::CommandResponse>, Status> {
        self.request_data_export(request).await
    }

    // ── Roles & permissions ───────────────────────────────────────────────────

    async fn assign_role(
        &self,
        request: Request<proto::AssignRoleRequest>,
    ) -> Result<Response<proto::CommandResponse>, Status> {
        self.assign_role(request).await
    }

    async fn revoke_role(
        &self,
        request: Request<proto::RevokeRoleRequest>,
    ) -> Result<Response<proto::CommandResponse>, Status> {
        self.revoke_role(request).await
    }

    // ── Queries ───────────────────────────────────────────────────────────────

    async fn get_account_by_id(
        &self,
        request: Request<proto::GetAccountByIdRequest>,
    ) -> Result<Response<proto::AccountView>, Status> {
        self.get_account_by_id(request).await
    }

    async fn set_date_of_birth(
        &self,
        request: Request<proto::SetDateOfBirthRequest>,
    ) -> Result<Response<proto::AccountView>, Status> {
        self.set_date_of_birth(request).await
    }

    async fn get_account_by_identity_id(
        &self,
        request: Request<proto::GetAccountByIdentityIdRequest>,
    ) -> Result<Response<proto::AccountView>, Status> {
        self.get_account_by_identity_id(request).await
    }

    async fn get_account_by_email(
        &self,
        request: Request<proto::GetAccountByEmailRequest>,
    ) -> Result<Response<proto::AccountView>, Status> {
        self.get_account_by_email(request).await
    }

    async fn get_account_by_phone(
        &self,
        request: Request<proto::GetAccountByPhoneRequest>,
    ) -> Result<Response<proto::AccountView>, Status> {
        self.get_account_by_phone(request).await
    }

    async fn get_account_status(
        &self,
        request: Request<proto::GetAccountStatusRequest>,
    ) -> Result<Response<proto::AccountStatusView>, Status> {
        self.get_account_status(request).await
    }

    async fn get_gdpr_record(
        &self,
        request: Request<proto::GetGdprRecordRequest>,
    ) -> Result<Response<proto::GdprRecordView>, Status> {
        self.get_gdpr_record(request).await
    }

    async fn update_consents(
        &self,
        request: Request<proto::UpdateConsentsRequest>,
    ) -> Result<Response<proto::GdprRecordView>, Status> {
        self.update_consents(request).await
    }

    async fn list_accounts_by_status(
        &self,
        request: Request<proto::ListAccountsByStatusRequest>,
    ) -> Result<Response<proto::ListAccountsByStatusResponse>, Status> {
        self.list_accounts_by_status(request).await
    }

    async fn find_profiles_by_contacts(
        &self,
        request: Request<proto::FindProfilesByContactsRequest>,
    ) -> Result<Response<proto::FindProfilesByContactsResponse>, Status> {
        self.find_profiles_by_contacts(request).await
    }
}
