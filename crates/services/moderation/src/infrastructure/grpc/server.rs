use tonic::{Request, Response, Status};

use super::handler::moderation_service_handler::{proto, ModerationServiceHandler};

// The tonic-generated trait from the bundled proto module.
use proto::moderation_service_server::ModerationService;

/// Encoded protobuf descriptor set for gRPC server reflection, emitted by
/// `moderation-api`'s `build.rs`. Registered by the service's runtime adapter.
pub const FILE_DESCRIPTOR_SET: &[u8] = moderation_api::FILE_DESCRIPTOR_SET;

#[tonic::async_trait]
impl ModerationService for ModerationServiceHandler {
    async fn screen(
        &self,
        request: Request<proto::ScreenRequest>,
    ) -> Result<Response<proto::ScreenResponse>, Status> {
        self.screen(request).await
    }

    async fn open_case(
        &self,
        request: Request<proto::OpenCaseRequest>,
    ) -> Result<Response<proto::OpenCaseResponse>, Status> {
        self.open_case(request).await
    }

    async fn assign_case(
        &self,
        request: Request<proto::AssignCaseRequest>,
    ) -> Result<Response<proto::AssignCaseResponse>, Status> {
        self.assign_case(request).await
    }

    async fn decide_case(
        &self,
        request: Request<proto::DecideCaseRequest>,
    ) -> Result<Response<proto::DecideCaseResponse>, Status> {
        self.decide_case(request).await
    }

    async fn list_queue(
        &self,
        request: Request<proto::ListQueueRequest>,
    ) -> Result<Response<proto::ListQueueResponse>, Status> {
        self.list_queue(request).await
    }

    async fn file_appeal(
        &self,
        request: Request<proto::FileAppealRequest>,
    ) -> Result<Response<proto::FileAppealResponse>, Status> {
        self.file_appeal(request).await
    }

    async fn resolve_appeal(
        &self,
        request: Request<proto::ResolveAppealRequest>,
    ) -> Result<Response<proto::ResolveAppealResponse>, Status> {
        self.resolve_appeal(request).await
    }

    async fn get_statement_of_reasons(
        &self,
        request: Request<proto::GetStatementOfReasonsRequest>,
    ) -> Result<Response<proto::GetStatementOfReasonsResponse>, Status> {
        self.get_statement_of_reasons(request).await
    }

    async fn get_enforcement_state(
        &self,
        request: Request<proto::GetEnforcementStateRequest>,
    ) -> Result<Response<proto::GetEnforcementStateResponse>, Status> {
        self.get_enforcement_state(request).await
    }

    async fn submit_report(
        &self,
        request: Request<proto::SubmitReportRequest>,
    ) -> Result<Response<proto::SubmitReportResponse>, Status> {
        self.submit_report(request).await
    }
}
