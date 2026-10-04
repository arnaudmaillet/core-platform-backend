use chrono::{DateTime, Utc};
use tonic::{Request, Response, Status};
use uuid::Uuid;

use cqrs::{CommandBus, Envelope, QueryBus};

use transport::grpc::edge;
use crate::domain::value_object::AgeBracket;
use crate::application::command::{
    anonymize_account::AnonymizeAccountCommand,
    assign_role::AssignRoleCommand,
    cancel_gdpr_deletion::CancelGdprDeletionCommand,
    change_password::ChangePasswordCommand,
    create_account::CreateAccountCommand,
    deactivate_account::DeactivateAccountCommand,
    enroll_mfa::EnrollMfaCommand,
    reactivate_account::ReactivateAccountCommand,
    record_failed_login::RecordFailedLoginCommand,
    record_login::RecordLoginCommand,
    request_data_export::RequestDataExportCommand,
    request_gdpr_deletion::RequestGdprDeletionCommand,
    resume_deactivated_account::ResumeDeactivatedAccountCommand,
    revoke_mfa::RevokeMfaCommand,
    revoke_role::RevokeRoleCommand,
    set_date_of_birth::SetDateOfBirthCommand,
    suspend_account::SuspendAccountCommand,
    update_consents::UpdateConsentsCommand,
    update_kyc_status::UpdateKycStatusCommand,
    verify_email::VerifyEmailCommand,
    verify_phone::VerifyPhoneCommand,
};
use crate::application::query::{
    get_account_by_id::{AccountView, GetAccountByIdQuery},
    get_account_by_email::GetAccountByEmailQuery,
    get_account_by_phone::GetAccountByPhoneQuery,
    get_account_by_identity_id::GetAccountByIdentityIdQuery,
    get_account_status::{AccountStatusView, GetAccountStatusQuery},
    get_gdpr_record::{GdprRecordView, GetGdprRecordQuery},
    list_accounts_by_status::{AccountListView, ListAccountsByStatusQuery},
};
// ── Proto inclusion ───────────────────────────────────────────────────────────
pub use account_api as proto;

pub use proto::account_service_server::AccountServiceServer;

/// gRPC request handler for the Account service.
///
/// Converts every inbound Protobuf request into a CQRS `Envelope<Command>` or
/// `Envelope<Query>`, dispatches it through the respective bus, and converts
/// the result back to a Protobuf response.
pub struct AccountServiceHandler<CB, QB>
where
    CB: CommandBus + Send + Sync + 'static,
    QB: QueryBus + Send + Sync + 'static,
{
    command_bus: CB,
    query_bus: QB,
    /// `ACCOUNT_REQUIRE_STEP_UP`: destructive self-service RPCs
    /// (`DeactivateAccount`, `RequestGdprDeletion`) need a recent credential
    /// proof on the edge (`auth.v1.VerifyCredentials`). Off by default until
    /// clients step up.
    require_step_up: bool,
}

impl<CB, QB> AccountServiceHandler<CB, QB>
where
    CB: CommandBus + Send + Sync + 'static,
    QB: QueryBus + Send + Sync + 'static,
{
    pub fn new(command_bus: CB, query_bus: QB) -> Self {
        Self { command_bus, query_bus, require_step_up: false }
    }

    /// Requires a recent credential proof on the destructive self-service RPCs.
    pub fn with_step_up(mut self, required: bool) -> Self {
        self.require_step_up = required;
        self
    }

    /// The step-up gate (edge only; the mesh is trusted).
    fn step_up<T>(&self, request: &Request<T>) -> Result<(), Status> {
        if self.require_step_up {
            edge::require_recent_auth(request, edge::STEP_UP_MAX_AGE_SECS)?;
        }
        Ok(())
    }
}

// ── Command handlers ──────────────────────────────────────────────────────────

impl<CB, QB> AccountServiceHandler<CB, QB>
where
    CB: CommandBus + Send + Sync + 'static,
    QB: QueryBus + Send + Sync + 'static,
{
    fn ok_command(account_id: &str) -> Response<proto::CommandResponse> {
        Response::new(proto::CommandResponse {
            success: true,
            account_id: account_id.to_owned(),
        })
    }

    pub async fn create_account(
        &self,
        request: Request<proto::CreateAccountRequest>,
    ) -> Result<Response<proto::CommandResponse>, Status> {
        let req = request.into_inner();
        let correlation_id = Uuid::now_v7();

        let cmd = CreateAccountCommand {
            identity_id: req.identity_id.clone(),
            email: req.email,
            phone: Some(req.phone).filter(|s| !s.is_empty()),
            password_hash: Some(req.password_hash).filter(|s| !s.is_empty()),
            country_of_residence: Some(req.country_of_residence).filter(|s| !s.is_empty()),
            role: None,
            created_by: Some(req.created_by).filter(|s| !s.is_empty()),
            date_of_birth: Some(req.date_of_birth).filter(|s| !s.is_empty()),
        };
        self.command_bus
            .dispatch(Envelope::new(correlation_id, cmd))
            .await
            .map(|_| Self::ok_command(&req.identity_id))
            .map_err(cqrs_error_to_status)
    }

    /// Mesh only (not in `EDGE_POLICY`): it carries no proof, so auth calls it
    /// once the holder proved the address (a verified id_token or a code).
    pub async fn verify_email(
        &self,
        request: Request<proto::VerifyEmailRequest>,
    ) -> Result<Response<proto::CommandResponse>, Status> {
        edge::require_account(&request, &request.get_ref().account_id)?;
        let req = request.into_inner();
        let cmd = VerifyEmailCommand { account_id: req.account_id.clone() };
        self.command_bus
            .dispatch(Envelope::new(Uuid::now_v7(), cmd))
            .await
            .map(|_| Self::ok_command(&req.account_id))
            .map_err(cqrs_error_to_status)
    }

    /// Mesh only (not in `EDGE_POLICY`): auth calls it once the holder proved
    /// the number (a one-time SMS code).
    pub async fn verify_phone(
        &self,
        request: Request<proto::VerifyPhoneRequest>,
    ) -> Result<Response<proto::CommandResponse>, Status> {
        edge::require_account(&request, &request.get_ref().account_id)?;
        let req = request.into_inner();
        let cmd = VerifyPhoneCommand { account_id: req.account_id.clone() };
        self.command_bus
            .dispatch(Envelope::new(Uuid::now_v7(), cmd))
            .await
            .map(|_| Self::ok_command(&req.account_id))
            .map_err(cqrs_error_to_status)
    }

    pub async fn change_password(
        &self,
        request: Request<proto::ChangePasswordRequest>,
    ) -> Result<Response<proto::CommandResponse>, Status> {
        edge::require_account(&request, &request.get_ref().account_id)?;
        let req = request.into_inner();
        let cmd = ChangePasswordCommand {
            account_id: req.account_id.clone(),
            new_password_hash: req.new_password_hash,
        };
        self.command_bus
            .dispatch(Envelope::new(Uuid::now_v7(), cmd))
            .await
            .map(|_| Self::ok_command(&req.account_id))
            .map_err(cqrs_error_to_status)
    }

    pub async fn enroll_mfa(
        &self,
        request: Request<proto::EnrollMfaRequest>,
    ) -> Result<Response<proto::CommandResponse>, Status> {
        edge::require_account(&request, &request.get_ref().account_id)?;
        let req = request.into_inner();
        let cmd = EnrollMfaCommand {
            account_id: req.account_id.clone(),
            totp_secret_ciphertext: req.totp_secret,
            // Recovery codes are generated server-side; none are sent via gRPC in this flow.
            recovery_code_hashes: Vec::new(),
        };
        self.command_bus
            .dispatch(Envelope::new(Uuid::now_v7(), cmd))
            .await
            .map(|_| Self::ok_command(&req.account_id))
            .map_err(cqrs_error_to_status)
    }

    pub async fn revoke_mfa(
        &self,
        request: Request<proto::RevokeMfaRequest>,
    ) -> Result<Response<proto::CommandResponse>, Status> {
        edge::require_account(&request, &request.get_ref().account_id)?;
        let req = request.into_inner();
        let cmd = RevokeMfaCommand { account_id: req.account_id.clone() };
        self.command_bus
            .dispatch(Envelope::new(Uuid::now_v7(), cmd))
            .await
            .map(|_| Self::ok_command(&req.account_id))
            .map_err(cqrs_error_to_status)
    }

    pub async fn update_kyc_status(
        &self,
        request: Request<proto::UpdateKycStatusRequest>,
    ) -> Result<Response<proto::CommandResponse>, Status> {
        let req = request.into_inner();
        let new_status = kyc_status_i32_to_str(req.kyc_status)
            .ok_or_else(|| Status::invalid_argument("unknown kyc_status value"))?;
        let cmd = UpdateKycStatusCommand {
            account_id: req.account_id.clone(),
            new_status: new_status.to_owned(),
            reviewer_id: req.reviewer_id,
        };
        self.command_bus
            .dispatch(Envelope::new(Uuid::now_v7(), cmd))
            .await
            .map(|_| Self::ok_command(&req.account_id))
            .map_err(cqrs_error_to_status)
    }

    pub async fn suspend_account(
        &self,
        request: Request<proto::SuspendAccountRequest>,
    ) -> Result<Response<proto::CommandResponse>, Status> {
        let req = request.into_inner();
        let cmd = SuspendAccountCommand {
            account_id: req.account_id.clone(),
            reason: req.reason,
        };
        self.command_bus
            .dispatch(Envelope::new(Uuid::now_v7(), cmd))
            .await
            .map(|_| Self::ok_command(&req.account_id))
            .map_err(cqrs_error_to_status)
    }

    pub async fn reactivate_account(
        &self,
        request: Request<proto::ReactivateAccountRequest>,
    ) -> Result<Response<proto::CommandResponse>, Status> {
        let req = request.into_inner();
        let cmd = ReactivateAccountCommand { account_id: req.account_id.clone() };
        self.command_bus
            .dispatch(Envelope::new(Uuid::now_v7(), cmd))
            .await
            .map(|_| Self::ok_command(&req.account_id))
            .map_err(cqrs_error_to_status)
    }

    pub async fn deactivate_account(
        &self,
        request: Request<proto::DeactivateAccountRequest>,
    ) -> Result<Response<proto::CommandResponse>, Status> {
        edge::require_account(&request, &request.get_ref().account_id)?;
        self.step_up(&request)?;
        let req = request.into_inner();
        let cmd = DeactivateAccountCommand { account_id: req.account_id.clone() };
        self.command_bus
            .dispatch(Envelope::new(Uuid::now_v7(), cmd))
            .await
            .map(|_| Self::ok_command(&req.account_id))
            .map_err(cqrs_error_to_status)
    }

    pub async fn resume_deactivated_account(
        &self,
        request: Request<proto::ResumeDeactivatedAccountRequest>,
    ) -> Result<Response<proto::CommandResponse>, Status> {
        let req = request.into_inner();
        let cmd = ResumeDeactivatedAccountCommand { account_id: req.account_id.clone() };
        self.command_bus
            .dispatch(Envelope::new(Uuid::now_v7(), cmd))
            .await
            .map(|_| Self::ok_command(&req.account_id))
            .map_err(cqrs_error_to_status)
    }

    pub async fn record_login(
        &self,
        request: Request<proto::RecordLoginRequest>,
    ) -> Result<Response<proto::CommandResponse>, Status> {
        let req = request.into_inner();
        let cmd = RecordLoginCommand { account_id: req.account_id.clone() };
        self.command_bus
            .dispatch(Envelope::new(Uuid::now_v7(), cmd))
            .await
            .map(|_| Self::ok_command(&req.account_id))
            .map_err(cqrs_error_to_status)
    }

    pub async fn record_failed_login(
        &self,
        request: Request<proto::RecordFailedLoginRequest>,
    ) -> Result<Response<proto::CommandResponse>, Status> {
        let req = request.into_inner();
        let cmd = RecordFailedLoginCommand {
            account_id: req.account_id.clone(),
            max_attempts: 5,            // default; can be made configurable
            lockout_duration_secs: 900, // default: 15 minutes
        };
        self.command_bus
            .dispatch(Envelope::new(Uuid::now_v7(), cmd))
            .await
            .map(|_| Self::ok_command(&req.account_id))
            .map_err(cqrs_error_to_status)
    }

    pub async fn request_gdpr_deletion(
        &self,
        request: Request<proto::RequestGdprDeletionRequest>,
    ) -> Result<Response<proto::CommandResponse>, Status> {
        edge::require_account(&request, &request.get_ref().account_id)?;
        self.step_up(&request)?;
        let req = request.into_inner();
        let cmd = RequestGdprDeletionCommand {
            account_id: req.account_id.clone(),
            retention_days: 30, // default legal retention; can be made configurable
        };
        self.command_bus
            .dispatch(Envelope::new(Uuid::now_v7(), cmd))
            .await
            .map(|_| Self::ok_command(&req.account_id))
            .map_err(cqrs_error_to_status)
    }

    pub async fn cancel_gdpr_deletion(
        &self,
        request: Request<proto::CancelGdprDeletionRequest>,
    ) -> Result<Response<proto::CommandResponse>, Status> {
        edge::require_account(&request, &request.get_ref().account_id)?;
        let req = request.into_inner();
        let cmd = CancelGdprDeletionCommand { account_id: req.account_id.clone() };
        self.command_bus
            .dispatch(Envelope::new(Uuid::now_v7(), cmd))
            .await
            .map(|_| Self::ok_command(&req.account_id))
            .map_err(cqrs_error_to_status)
    }

    pub async fn anonymize_account(
        &self,
        request: Request<proto::AnonymizeAccountRequest>,
    ) -> Result<Response<proto::CommandResponse>, Status> {
        let req = request.into_inner();
        let cmd = AnonymizeAccountCommand { account_id: req.account_id.clone() };
        self.command_bus
            .dispatch(Envelope::new(Uuid::now_v7(), cmd))
            .await
            .map(|_| Self::ok_command(&req.account_id))
            .map_err(cqrs_error_to_status)
    }

    pub async fn request_data_export(
        &self,
        request: Request<proto::RequestDataExportRequest>,
    ) -> Result<Response<proto::CommandResponse>, Status> {
        edge::require_account(&request, &request.get_ref().account_id)?;
        let req = request.into_inner();
        let cmd = RequestDataExportCommand { account_id: req.account_id.clone() };
        self.command_bus
            .dispatch(Envelope::new(Uuid::now_v7(), cmd))
            .await
            .map(|_| Self::ok_command(&req.account_id))
            .map_err(cqrs_error_to_status)
    }

    pub async fn assign_role(
        &self,
        request: Request<proto::AssignRoleRequest>,
    ) -> Result<Response<proto::CommandResponse>, Status> {
        let req = request.into_inner();
        let role = account_role_i32_to_str(req.role)
            .ok_or_else(|| Status::invalid_argument("unknown role value"))?;
        let cmd = AssignRoleCommand {
            account_id: req.account_id.clone(),
            role: role.to_owned(),
        };
        self.command_bus
            .dispatch(Envelope::new(Uuid::now_v7(), cmd))
            .await
            .map(|_| Self::ok_command(&req.account_id))
            .map_err(cqrs_error_to_status)
    }

    pub async fn revoke_role(
        &self,
        request: Request<proto::RevokeRoleRequest>,
    ) -> Result<Response<proto::CommandResponse>, Status> {
        let req = request.into_inner();
        let role = account_role_i32_to_str(req.role)
            .ok_or_else(|| Status::invalid_argument("unknown role value"))?;
        let cmd = RevokeRoleCommand {
            account_id: req.account_id.clone(),
            role: role.to_owned(),
        };
        self.command_bus
            .dispatch(Envelope::new(Uuid::now_v7(), cmd))
            .await
            .map(|_| Self::ok_command(&req.account_id))
            .map_err(cqrs_error_to_status)
    }
}

// ── Query handlers ────────────────────────────────────────────────────────────

impl<CB, QB> AccountServiceHandler<CB, QB>
where
    CB: CommandBus + Send + Sync + 'static,
    QB: QueryBus + Send + Sync + 'static,
{
    pub async fn get_account_by_id(
        &self,
        request: Request<proto::GetAccountByIdRequest>,
    ) -> Result<Response<proto::AccountView>, Status> {
        edge::require_account(&request, &request.get_ref().account_id)?;
        let req = request.into_inner();
        let query = GetAccountByIdQuery { account_id: req.account_id };
        let view: AccountView = self
            .query_bus
            .dispatch(Envelope::new(Uuid::now_v7(), query))
            .await
            .map_err(cqrs_error_to_status)?;
        Ok(Response::new(account_view_to_proto(view)))
    }

    /// Records the holder's date of birth when none is on file; returns the
    /// updated view (with its age bracket).
    pub async fn set_date_of_birth(
        &self,
        request: Request<proto::SetDateOfBirthRequest>,
    ) -> Result<Response<proto::AccountView>, Status> {
        edge::require_account(&request, &request.get_ref().account_id)?;
        let req = request.into_inner();
        let cmd = SetDateOfBirthCommand {
            account_id: req.account_id.clone(),
            date_of_birth: req.date_of_birth,
        };
        self.command_bus
            .dispatch(Envelope::new(Uuid::now_v7(), cmd))
            .await
            .map_err(cqrs_error_to_status)?;
        let view: AccountView = self
            .query_bus
            .dispatch(Envelope::new(Uuid::now_v7(), GetAccountByIdQuery { account_id: req.account_id }))
            .await
            .map_err(cqrs_error_to_status)?;
        Ok(Response::new(account_view_to_proto(view)))
    }

    /// Mesh only (not in `EDGE_POLICY`): it would enumerate numbers.
    pub async fn get_account_by_phone(
        &self,
        request: Request<proto::GetAccountByPhoneRequest>,
    ) -> Result<Response<proto::AccountView>, Status> {
        let req = request.into_inner();
        let view: AccountView = self
            .query_bus
            .dispatch(Envelope::new(Uuid::now_v7(), GetAccountByPhoneQuery { phone: req.phone }))
            .await
            .map_err(cqrs_error_to_status)?;
        Ok(Response::new(account_view_to_proto(view)))
    }

    /// Mesh only (not in `EDGE_POLICY`): it would enumerate addresses.
    pub async fn get_account_by_email(
        &self,
        request: Request<proto::GetAccountByEmailRequest>,
    ) -> Result<Response<proto::AccountView>, Status> {
        let req = request.into_inner();
        let view: AccountView = self
            .query_bus
            .dispatch(Envelope::new(Uuid::now_v7(), GetAccountByEmailQuery { email: req.email }))
            .await
            .map_err(cqrs_error_to_status)?;
        Ok(Response::new(account_view_to_proto(view)))
    }

    pub async fn get_account_by_identity_id(
        &self,
        request: Request<proto::GetAccountByIdentityIdRequest>,
    ) -> Result<Response<proto::AccountView>, Status> {
        let req = request.into_inner();
        let query = GetAccountByIdentityIdQuery { identity_id: req.identity_id };
        let view: AccountView = self
            .query_bus
            .dispatch(Envelope::new(Uuid::now_v7(), query))
            .await
            .map_err(cqrs_error_to_status)?;
        Ok(Response::new(account_view_to_proto(view)))
    }

    pub async fn get_account_status(
        &self,
        request: Request<proto::GetAccountStatusRequest>,
    ) -> Result<Response<proto::AccountStatusView>, Status> {
        edge::require_account(&request, &request.get_ref().account_id)?;
        let req = request.into_inner();
        let query = GetAccountStatusQuery { account_id: req.account_id };
        let view: AccountStatusView = self
            .query_bus
            .dispatch(Envelope::new(Uuid::now_v7(), query))
            .await
            .map_err(cqrs_error_to_status)?;
        Ok(Response::new(proto::AccountStatusView {
            account_id: view.account_id,
            status: account_status_str_to_i32(view.status.as_str()),
            suspension_reason: view.suspension_reason.unwrap_or_default(),
        }))
    }

    pub async fn get_gdpr_record(
        &self,
        request: Request<proto::GetGdprRecordRequest>,
    ) -> Result<Response<proto::GdprRecordView>, Status> {
        edge::require_account(&request, &request.get_ref().account_id)?;
        let req = request.into_inner();
        self.gdpr_record(req.account_id).await.map(Response::new)
    }

    pub async fn update_consents(
        &self,
        request: Request<proto::UpdateConsentsRequest>,
    ) -> Result<Response<proto::GdprRecordView>, Status> {
        edge::require_account(&request, &request.get_ref().account_id)?;
        let req = request.into_inner();
        let cmd = UpdateConsentsCommand {
            account_id: req.account_id.clone(),
            data_processing: req.data_processing,
            marketing: req.marketing,
            analytics: req.analytics,
            policy_version: (!req.policy_version.is_empty()).then_some(req.policy_version),
        };
        self.command_bus
            .dispatch(Envelope::new(Uuid::now_v7(), cmd))
            .await
            .map_err(cqrs_error_to_status)?;
        self.gdpr_record(req.account_id).await.map(Response::new)
    }

    async fn gdpr_record(&self, account_id: String) -> Result<proto::GdprRecordView, Status> {
        let query = GetGdprRecordQuery { account_id };
        let view: GdprRecordView = self
            .query_bus
            .dispatch(Envelope::new(Uuid::now_v7(), query))
            .await
            .map_err(cqrs_error_to_status)?;
        Ok(proto::GdprRecordView {
            account_id: view.account_id,
            data_processing_consented: view.data_processing_consented_at.is_some(),
            marketing_consented: view.marketing_consented_at.is_some(),
            analytics_consented: view.analytics_consented_at.is_some(),
            data_processing_consented_at: view.data_processing_consented_at.map(dt_to_ts),
            marketing_consented_at: view.marketing_consented_at.map(dt_to_ts),
            analytics_consented_at: view.analytics_consented_at.map(dt_to_ts),
            consent_policy_version: view.last_consent_version.unwrap_or_default(),
            deletion_requested_at: view.deletion_requested_at.map(dt_to_ts),
            deletion_scheduled_at: view.deletion_scheduled_at.map(dt_to_ts),
            anonymized_at: view.anonymized_at.map(dt_to_ts),
            data_export_requested_at: view.data_export_requested_at.map(dt_to_ts),
            data_export_completed_at: view.data_export_completed_at.map(dt_to_ts),
        })
    }

    pub async fn list_accounts_by_status(
        &self,
        request: Request<proto::ListAccountsByStatusRequest>,
    ) -> Result<Response<proto::ListAccountsByStatusResponse>, Status> {
        let req = request.into_inner();
        let status = account_status_i32_to_str(req.status)
            .ok_or_else(|| Status::invalid_argument("invalid status value"))?;
        let query = ListAccountsByStatusQuery {
            status: status.to_owned(),
            limit: req.limit.clamp(1, 1000) as i64,
            offset: req.offset.max(0) as i64,
        };
        let view: AccountListView = self
            .query_bus
            .dispatch(Envelope::new(Uuid::now_v7(), query))
            .await
            .map_err(cqrs_error_to_status)?;
        Ok(Response::new(proto::ListAccountsByStatusResponse {
            accounts: view.accounts.into_iter().map(account_view_to_proto).collect(),
            total: view.total,
        }))
    }
}

// ── Proto conversion helpers ──────────────────────────────────────────────────

fn dt_to_ts(dt: DateTime<Utc>) -> prost_types::Timestamp {
    prost_types::Timestamp {
        seconds: dt.timestamp(),
        nanos: dt.timestamp_subsec_nanos() as i32,
    }
}

fn account_view_to_proto(v: AccountView) -> proto::AccountView {
    proto::AccountView {
        id: v.id,
        identity_id: v.identity_id,
        status: account_status_str_to_i32(&v.status),
        suspension_reason: v.suspension_reason.unwrap_or_default(),
        email: v.email,
        email_verified: v.email_verified,
        phone: v.phone.unwrap_or_default(),
        phone_verified: v.phone_verified,
        kyc_status: kyc_status_str_to_i32(&v.kyc_status),
        country_of_residence: v.country_of_residence.unwrap_or_default(),
        roles: v.roles,
        permissions: v.permissions,
        version: v.version,
        created_at: Some(dt_to_ts(v.created_at)),
        updated_at: Some(dt_to_ts(v.updated_at)),
        age_bracket: age_bracket_to_proto(v.age_bracket),
        date_of_birth: v.date_of_birth.map(|d| d.format("%Y-%m-%d").to_string()).unwrap_or_default(),
    }
}

fn age_bracket_to_proto(bracket: Option<AgeBracket>) -> i32 {
    (match bracket {
        None => proto::AgeBracket::Unspecified,
        Some(AgeBracket::Teen13To15) => proto::AgeBracket::AgeBracket1315,
        Some(AgeBracket::Teen16To17) => proto::AgeBracket::AgeBracket1617,
        Some(AgeBracket::Adult) => proto::AgeBracket::Adult,
    }) as i32
}

fn account_status_str_to_i32(s: &str) -> i32 {
    match s {
        "pending_verification" => 1,
        "active"               => 2,
        "suspended"            => 3,
        "deactivated"          => 4,
        "deleted"              => 5,
        _                      => 0,
    }
}

fn account_status_i32_to_str(v: i32) -> Option<&'static str> {
    match v {
        1 => Some("pending_verification"),
        2 => Some("active"),
        3 => Some("suspended"),
        4 => Some("deactivated"),
        5 => Some("deleted"),
        _ => None,
    }
}

fn kyc_status_str_to_i32(s: &str) -> i32 {
    match s {
        "not_started" => 1,
        "submitted"   => 2,
        "in_review"   => 3,
        "approved"    => 4,
        "rejected"    => 5,
        _             => 0,
    }
}

fn kyc_status_i32_to_str(v: i32) -> Option<&'static str> {
    match v {
        1 => Some("not_started"),
        2 => Some("submitted"),
        3 => Some("in_review"),
        4 => Some("approved"),
        5 => Some("rejected"),
        _ => None,
    }
}

fn account_role_i32_to_str(v: i32) -> Option<&'static str> {
    match v {
        1 => Some("user"),
        2 => Some("content_moderator"),
        3 => Some("support_agent"),
        4 => Some("finance_operator"),
        5 => Some("admin"),
        6 => Some("super_admin"),
        _ => None,
    }
}

// ── Error mapping ─────────────────────────────────────────────────────────────

/// Status metadata key carrying the account error code (`ACC-xxxx`).
pub const ERROR_CODE_METADATA: &str = "x-error-code";

pub fn cqrs_error_to_status(err: cqrs::error::CqrsError) -> Status {
    use cqrs::error::CqrsError;
    match err {
        CqrsError::HandlerNotFound { type_name } => {
            Status::unimplemented(format!("no handler registered for {type_name}"))
        }
        CqrsError::DuplicateRegistration { type_name } => {
            Status::internal(format!("duplicate handler for {type_name}"))
        }
        CqrsError::Handler(boxed) => {
            // BoxedDynAppError has no downcast path; map via AppError trait metadata.
            use error::AppError as _;
            let msg = boxed.to_string();
            let retryable = boxed.is_retryable();
            let mut status = match boxed.http_status().as_u16() {
                404 => Status::not_found(msg),
                409 if retryable => Status::aborted(msg),
                409 => Status::already_exists(msg),
                400 | 422 => Status::failed_precondition(msg),
                503 | 502 => Status::unavailable(msg),
                _ => Status::internal(msg),
            };
            // The stable error code (e.g. `ACC-2004`), so a mesh caller can tell
            // failures sharing a gRPC code apart without parsing the message.
            if let Ok(code) = boxed.error_code().parse() {
                status.metadata_mut().insert(ERROR_CODE_METADATA, code);
            }
            status
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use cqrs::command::CommandBusBuilder;
    use cqrs::query::QueryBusBuilder;
    use tonic::Code;

    use super::*;

    /// No command handler is registered: a call that passes the gates reaches
    /// the bus and comes back UNIMPLEMENTED — a gate refusal never gets there.
    fn handler(step_up: bool) -> AccountServiceHandler<impl CommandBus, impl QueryBus> {
        AccountServiceHandler::new(
            Arc::new(CommandBusBuilder::new().build()),
            Arc::new(QueryBusBuilder::new().build()),
        )
        .with_step_up(step_up)
    }

    fn as_owner<T>(message: T, account_id: &str, auth_time: Option<i64>) -> Request<T> {
        let mut claims = serde_json::json!({ "sub": account_id, "exp": 4_102_444_800_i64 });
        if let Some(at) = auth_time {
            claims["auth_time"] = serde_json::json!(at);
        }
        let raw: auth_context::OidcClaims = serde_json::from_value(claims).unwrap();
        let mut request = Request::new(message);
        request.extensions_mut().insert(edge::EdgePrincipal::new(Arc::new(
            auth_context::CurrentPrincipal {
                user_id: auth_context::PrincipalId::new(account_id),
                tenant_id: None,
                permissions: vec![],
                raw_claims: raw,
            },
        )));
        request
    }

    fn now() -> i64 {
        Utc::now().timestamp()
    }

    #[tokio::test]
    async fn deleting_or_deactivating_needs_a_recent_credential_proof_when_required() {
        let me = Uuid::now_v7().to_string();
        let delete = |at| as_owner(proto::RequestGdprDeletionRequest { account_id: me.clone() }, &me, at);
        let deactivate = |at| as_owner(proto::DeactivateAccountRequest { account_id: me.clone() }, &me, at);

        let gated = handler(true);
        for stale in [None, Some(now() - 3_600)] {
            let status = gated.request_gdpr_deletion(delete(stale)).await.unwrap_err();
            assert_eq!(status.code(), Code::PermissionDenied);
            assert_eq!(status.message(), edge::STEP_UP_REQUIRED);
            let status = gated.deactivate_account(deactivate(stale)).await.unwrap_err();
            assert_eq!(status.code(), Code::PermissionDenied);
        }
        // A fresh proof passes the gate (and reaches the empty bus).
        let status = gated.request_gdpr_deletion(delete(Some(now()))).await.unwrap_err();
        assert_eq!(status.code(), Code::Unimplemented);
        // The mesh is trusted.
        let mesh = Request::new(proto::DeactivateAccountRequest { account_id: me.clone() });
        assert_eq!(gated.deactivate_account(mesh).await.unwrap_err().code(), Code::Unimplemented);

        // Off (the default): no proof needed.
        let open = handler(false);
        let status = open.request_gdpr_deletion(delete(None)).await.unwrap_err();
        assert_eq!(status.code(), Code::Unimplemented);
    }

    #[test]
    fn a_status_carries_the_account_error_code() {
        let status = cqrs_error_to_status(cqrs::CqrsError::from_handler(crate::error::AccountError::AgeBelowMinimum { minimum: 13 }));
        assert_eq!(status.code(), Code::FailedPrecondition);
        assert_eq!(status.metadata().get(ERROR_CODE_METADATA).and_then(|v| v.to_str().ok()), Some("ACC-2004"));
    }
}
