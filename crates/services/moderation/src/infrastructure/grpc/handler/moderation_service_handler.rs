use std::sync::Arc;

use chrono::{DateTime, Utc};
use cqrs::{Envelope, QueryHandler};
use error::AppError;
use tonic::{Request, Response, Status};
use uuid::Uuid;

use transport::grpc::edge;
use crate::application::command::{
    AssignCaseCommand, AssignCaseHandler, DecideCaseCommand, DecideCaseHandler, DecideOutcome,
    FileAppealCommand, FileAppealHandler, OpenCaseCommand, OpenCaseHandler, OpenedCase,
    ResolveAppealCommand, ResolveAppealHandler, ResolveAppealOutcome, ScreenCommand, ScreenHandler,
    ScreenVerdict, Reporter, SubmitReportCommand, SubmitReportHandler,
};
use crate::application::port::ContentHash;
use crate::application::query::{
    GetEnforcementStateHandler, GetEnforcementStateQuery, GetStatementOfReasonsHandler,
    GetStatementOfReasonsQuery, ListMyReportsHandler, ListMyReportsQuery, ListQueueHandler,
    ListQueueQuery, MyReport, StatementOfReasons,
};
use crate::domain::aggregate::{Appeal, Case, Decision, EnforcementAction};
use crate::domain::value_object::{
    ActionType, ActorId, AppealId, CaseId, CaseStatus, DecisionId, EnforcementStatus, EntityType,
    PolicyCategory, ReportStatus, Signal, SubjectRef,
};
use crate::error::ModerationError;

// ── Proto inclusion ───────────────────────────────────────────────────────────
pub use moderation_api as proto;

/// gRPC request handler for the `moderation.v1` service. Each method translates an
/// inbound Protobuf request into an application command/query, runs it with a
/// fresh correlation id + the wall clock, and maps the result (or
/// [`ModerationError`]) back to Protobuf / [`Status`].
#[derive(Clone)]
pub struct ModerationServiceHandler {
    screen: Arc<ScreenHandler>,
    open_case: Arc<OpenCaseHandler>,
    assign_case: Arc<AssignCaseHandler>,
    decide_case: Arc<DecideCaseHandler>,
    list_queue: Arc<ListQueueHandler>,
    file_appeal: Arc<FileAppealHandler>,
    resolve_appeal: Arc<ResolveAppealHandler>,
    statement_of_reasons: Arc<GetStatementOfReasonsHandler>,
    enforcement_state: Arc<GetEnforcementStateHandler>,
    submit_report: Arc<SubmitReportHandler>,
    list_my_reports: Arc<ListMyReportsHandler>,
}

impl ModerationServiceHandler {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        screen: Arc<ScreenHandler>,
        open_case: Arc<OpenCaseHandler>,
        assign_case: Arc<AssignCaseHandler>,
        decide_case: Arc<DecideCaseHandler>,
        list_queue: Arc<ListQueueHandler>,
        file_appeal: Arc<FileAppealHandler>,
        resolve_appeal: Arc<ResolveAppealHandler>,
        statement_of_reasons: Arc<GetStatementOfReasonsHandler>,
        enforcement_state: Arc<GetEnforcementStateHandler>,
        submit_report: Arc<SubmitReportHandler>,
        list_my_reports: Arc<ListMyReportsHandler>,
    ) -> Self {
        Self {
            screen,
            open_case,
            assign_case,
            decide_case,
            list_queue,
            file_appeal,
            resolve_appeal,
            statement_of_reasons,
            enforcement_state,
            submit_report,
            list_my_reports,
        }
    }

    fn envelope<T>(payload: T) -> Envelope<T> {
        Envelope::new(Uuid::now_v7(), payload)
    }

    pub async fn screen(
        &self,
        request: Request<proto::ScreenRequest>,
    ) -> Result<Response<proto::ScreenResponse>, Status> {
        let req = request.into_inner();
        let cmd = ScreenCommand {
            subject: subject_from_proto(req.subject)?,
            hashes: req
                .hashes
                .into_iter()
                .map(|h| ContentHash { algorithm: h.algorithm, value: h.value })
                .collect(),
            text: (!req.text.is_empty()).then_some(req.text),
            categories: req
                .categories
                .into_iter()
                .map(category_from_proto)
                .collect::<Result<Vec<_>, _>>()?,
        };
        let out = self
            .screen
            .handle(Self::envelope(cmd), Utc::now())
            .await
            .map_err(status)?;
        Ok(Response::new(proto::ScreenResponse {
            verdict: verdict_to_proto(out.verdict),
            matched_categories: out.matched_categories.iter().map(|c| category_to_proto(*c)).collect(),
            match_reference: out.match_reference.unwrap_or_default(),
        }))
    }

    /// Client report (edge `member_or_guest`). The reporter is the verified
    /// caller: a member's account, or a guest's id. A mesh call (no principal)
    /// is refused: client reports always have a reporter.
    pub async fn submit_report(
        &self,
        request: Request<proto::SubmitReportRequest>,
    ) -> Result<Response<proto::SubmitReportResponse>, Status> {
        let reporter = reporter_of(&request).map_err(status)?;
        let req = request.into_inner();
        let cmd = SubmitReportCommand {
            reporter,
            entity_type: entity_type_from_proto(req.entity_type)?,
            entity_id: req.entity_id,
            surface: req.surface,
            category: category_from_proto(req.category)?,
            reason: req.reason,
        };
        let report_id = self
            .submit_report
            .handle(Self::envelope(cmd), Utc::now())
            .await
            .map_err(status)?;
        Ok(Response::new(proto::SubmitReportResponse { report_id: report_id.as_str() }))
    }

    /// The caller's own reports (edge `member_or_guest`), each with what became
    /// of it. Like `SubmitReport`, the reporter is the verified token; a mesh
    /// call has no reporter and is refused.
    pub async fn list_my_reports(
        &self,
        request: Request<proto::ListMyReportsRequest>,
    ) -> Result<Response<proto::ListMyReportsResponse>, Status> {
        let reporter = reporter_of(&request).map_err(status)?;
        let req = request.into_inner();
        let query = ListMyReportsQuery {
            reporter,
            page_token: (!req.page_token.is_empty()).then_some(req.page_token),
            page_size: usize::try_from(req.page_size).unwrap_or(0),
        };
        let page = self.list_my_reports.handle(Self::envelope(query)).await.map_err(status)?;
        Ok(Response::new(proto::ListMyReportsResponse {
            reports: page.reports.iter().map(report_view).collect(),
            next_page_token: page.next_page_token.unwrap_or_default(),
        }))
    }

    pub async fn open_case(
        &self,
        request: Request<proto::OpenCaseRequest>,
    ) -> Result<Response<proto::OpenCaseResponse>, Status> {
        let req = request.into_inner();
        let cmd = OpenCaseCommand {
            subject: subject_from_proto(req.subject)?,
            category: category_from_proto(req.category)?,
            queue: defaulted(req.reason.clone(), "default"),
            priority: "normal".to_owned(),
        };
        let OpenedCase { case, created } = self
            .open_case
            .handle(Self::envelope(cmd), Utc::now())
            .await
            .map_err(status)?;
        Ok(Response::new(proto::OpenCaseResponse { case: Some(case_view(&case)), created }))
    }

    pub async fn assign_case(
        &self,
        request: Request<proto::AssignCaseRequest>,
    ) -> Result<Response<proto::AssignCaseResponse>, Status> {
        let req = request.into_inner();
        let cmd = AssignCaseCommand { case_id: parse_case_id(&req.case_id)?, reviewer_id: req.reviewer_id };
        let case = self.assign_case.handle(Self::envelope(cmd)).await.map_err(status)?;
        Ok(Response::new(proto::AssignCaseResponse { case: Some(case_view(&case)) }))
    }

    pub async fn decide_case(
        &self,
        request: Request<proto::DecideCaseRequest>,
    ) -> Result<Response<proto::DecideCaseResponse>, Status> {
        let req = request.into_inner();
        let cmd = DecideCaseCommand {
            case_id: parse_case_id(&req.case_id)?,
            action: action_from_proto(req.action)?,
            category: category_from_proto(req.category)?,
            rationale: req.rationale,
            reviewer_id: req.reviewer_id,
            policy_version: req.policy_version,
        };
        let DecideOutcome { decision, enforcement } = self
            .decide_case
            .handle(Self::envelope(cmd), Utc::now())
            .await
            .map_err(status)?;
        Ok(Response::new(proto::DecideCaseResponse {
            decision: Some(decision_to_proto(&decision)),
            enforcement: enforcement.as_ref().map(enforcement_view),
        }))
    }

    pub async fn list_queue(
        &self,
        request: Request<proto::ListQueueRequest>,
    ) -> Result<Response<proto::ListQueueResponse>, Status> {
        let req = request.into_inner();
        let query = ListQueueQuery {
            queue: req.queue,
            status_filter: case_status_filter(req.status_filter),
            limit: if req.page_size <= 0 { 50 } else { req.page_size as usize },
        };
        let cases = self.list_queue.handle(Self::envelope(query)).await.map_err(status)?;
        Ok(Response::new(proto::ListQueueResponse {
            cases: cases.iter().map(case_view).collect(),
            next_page_token: String::new(),
        }))
    }

    pub async fn file_appeal(
        &self,
        request: Request<proto::FileAppealRequest>,
    ) -> Result<Response<proto::FileAppealResponse>, Status> {
        edge::require_account(&request, &request.get_ref().actor_id)?;
        let req = request.into_inner();
        let cmd = FileAppealCommand {
            decision_id: DecisionId::try_from(req.decision_id.as_str()).map_err(status)?,
            actor_id: ActorId::try_from(req.actor_id.as_str()).map_err(status)?,
            statement: req.statement,
        };
        let appeal = self
            .file_appeal
            .handle(Self::envelope(cmd), Utc::now())
            .await
            .map_err(status)?;
        Ok(Response::new(proto::FileAppealResponse { appeal: Some(appeal_view(&appeal)) }))
    }

    pub async fn resolve_appeal(
        &self,
        request: Request<proto::ResolveAppealRequest>,
    ) -> Result<Response<proto::ResolveAppealResponse>, Status> {
        let req = request.into_inner();
        let cmd = ResolveAppealCommand {
            appeal_id: AppealId::try_from(req.appeal_id.as_str()).map_err(status)?,
            overturn: req.overturn,
            rationale: req.rationale,
            reviewer_id: req.reviewer_id,
        };
        let ResolveAppealOutcome { appeal, reversal } = self
            .resolve_appeal
            .handle(Self::envelope(cmd), Utc::now())
            .await
            .map_err(status)?;
        Ok(Response::new(proto::ResolveAppealResponse {
            appeal: Some(appeal_view(&appeal)),
            reversal: reversal.as_ref().map(decision_to_proto),
        }))
    }

    pub async fn get_statement_of_reasons(
        &self,
        request: Request<proto::GetStatementOfReasonsRequest>,
    ) -> Result<Response<proto::GetStatementOfReasonsResponse>, Status> {
        let query = GetStatementOfReasonsQuery {
            decision_id: DecisionId::try_from(request.get_ref().decision_id.as_str()).map_err(status)?,
        };
        let sor = self.statement_of_reasons.handle(Self::envelope(query)).await.map_err(status)?;
        // On the edge only the sanctioned account reads its statement (DSA Art.
        // 17); anyone else learns nothing, not even that the decision exists.
        if let Some(principal) = edge::principal(&request)
            && principal.account_id() != sor.subject.actor_id().as_str()
        {
            return Err(status(ModerationError::DecisionNotFound { id: sor.decision_id.as_str() }));
        }
        Ok(Response::new(proto::GetStatementOfReasonsResponse {
            statement: Some(statement_to_proto(&sor)),
        }))
    }

    pub async fn get_enforcement_state(
        &self,
        request: Request<proto::GetEnforcementStateRequest>,
    ) -> Result<Response<proto::GetEnforcementStateResponse>, Status> {
        // On the edge an account reads its own state only.
        edge::require_account(&request, &request.get_ref().actor_id)?;
        let req = request.into_inner();
        let query = GetEnforcementStateQuery {
            actor_id: ActorId::try_from(req.actor_id.as_str()).map_err(status)?,
        };
        let view = self.enforcement_state.handle(Self::envelope(query)).await.map_err(status)?;
        Ok(Response::new(proto::GetEnforcementStateResponse {
            actor_restricted: view.actor_restricted,
            active_enforcements: view.active_enforcements.iter().map(enforcement_view).collect(),
        }))
    }
}

// ── Mapping helpers ─────────────────────────────────────────────────────────

fn defaulted(s: String, fallback: &str) -> String {
    if s.trim().is_empty() { fallback.to_owned() } else { s }
}

fn parse_case_id(s: &str) -> Result<CaseId, Status> {
    Uuid::parse_str(s)
        .map(CaseId::from_uuid)
        .map_err(|_| Status::invalid_argument(format!("invalid case_id: '{s}'")))
}

/// The reporter of a client report, from the verified edge token: a guest's
/// `sub` is `guest:<id>`, a member's is its account id.
fn reporter_of<T>(request: &Request<T>) -> Result<Reporter, ModerationError> {
    let principal = edge::principal(request).ok_or(ModerationError::ReporterRequired)?;
    let sub = principal.account_id();
    let (raw, guest) = match sub.strip_prefix("guest:") {
        Some(id) => (id, true),
        None => (sub, principal.is_guest()),
    };
    let id = ActorId::try_from(raw).map_err(|_| ModerationError::ReporterRequired)?;
    Ok(if guest { Reporter::Guest(id) } else { Reporter::Member(id) })
}

fn subject_from_proto(s: Option<proto::SubjectRef>) -> Result<SubjectRef, Status> {
    let s = s.ok_or_else(|| Status::invalid_argument("subject is required"))?;
    SubjectRef::new(
        entity_type_from_proto(s.entity_type)?,
        s.entity_id,
        ActorId::try_from(s.actor_id.as_str()).map_err(status)?,
        s.surface,
    )
    .map_err(status)
}

fn subject_to_proto(s: &SubjectRef) -> proto::SubjectRef {
    proto::SubjectRef {
        entity_type: entity_type_to_proto(s.entity_type()),
        entity_id: s.entity_id().to_owned(),
        actor_id: s.actor_id().as_str(),
        surface: s.surface().to_owned(),
    }
}

fn case_view(case: &Case) -> proto::CaseView {
    proto::CaseView {
        case_id: case.id().as_str(),
        subject: Some(subject_to_proto(case.subject())),
        status: case_status_to_proto(case.status()),
        category: category_to_proto(case.category()),
        queue: case.queue().to_owned(),
        priority: case.priority().to_owned(),
        assignee: case.assignee().unwrap_or_default().to_owned(),
        signals: case.signals().iter().map(signal_summary).collect(),
        opened_at: Some(to_ts(case.opened_at())),
    }
}

fn signal_summary(s: &Signal) -> proto::SignalSummary {
    proto::SignalSummary {
        source: s.source().to_owned(),
        category: category_to_proto(s.category()),
        confidence: s.confidence().value() as f32,
        observed_at: Some(to_ts(s.observed_at())),
    }
}

fn decision_to_proto(d: &Decision) -> proto::Decision {
    proto::Decision {
        decision_id: d.id().as_str(),
        subject: Some(subject_to_proto(d.subject())),
        action: action_to_proto(d.action()),
        category: category_to_proto(d.category()),
        policy_version: d.policy_version().as_str().to_owned(),
        rationale: d.rationale().to_owned(),
        decided_by: d.author().id().to_owned(),
        automated: d.is_automated(),
        reverses_decision_id: d.reverses().map(|r| r.as_str()).unwrap_or_default(),
        decided_at: Some(to_ts(d.decided_at())),
    }
}

fn enforcement_view(e: &EnforcementAction) -> proto::EnforcementView {
    proto::EnforcementView {
        enforcement_id: e.id().as_str(),
        subject: Some(subject_to_proto(e.subject())),
        action: action_to_proto(e.action()),
        status: enforcement_status_to_proto(e.status()),
        version: e.version().value(),
        applied_at: Some(to_ts(e.applied_at())),
        expires_at: e.expires_at().map(to_ts),
        decision_id: e.decision_id().as_str(),
    }
}

fn report_view(r: &MyReport) -> proto::ReportView {
    let report = &r.filed.report;
    proto::ReportView {
        report_id: report.id().as_str(),
        entity_type: entity_type_to_proto(report.subject().entity_type()),
        entity_id: report.subject().entity_id().to_owned(),
        category: category_to_proto(report.category()),
        reason: report.reason().to_owned(),
        status: report_status_to_proto(r.status),
        reported_at: Some(to_ts(report.reported_at())),
    }
}

fn report_status_to_proto(s: ReportStatus) -> i32 {
    (match s {
        ReportStatus::UnderReview => proto::ReportStatus::UnderReview,
        ReportStatus::ActionTaken => proto::ReportStatus::ActionTaken,
        ReportStatus::NoViolation => proto::ReportStatus::NoViolation,
    }) as i32
}

fn appeal_view(a: &Appeal) -> proto::AppealView {
    proto::AppealView {
        appeal_id: a.id().as_str(),
        decision_id: a.decision_id().as_str(),
        status: appeal_status_to_proto(a.status()),
        statement: a.statement().to_owned(),
        filed_at: Some(to_ts(a.filed_at())),
        resolved_at: a.resolved_at().map(to_ts),
    }
}

fn statement_to_proto(s: &StatementOfReasons) -> proto::StatementOfReasons {
    proto::StatementOfReasons {
        decision_id: s.decision_id.as_str(),
        subject: Some(subject_to_proto(&s.subject)),
        category: category_to_proto(s.category),
        action: action_to_proto(s.action),
        policy_version: s.policy_version.clone(),
        facts: s.facts.clone(),
        legal_ground: String::new(),
        automated: s.automated,
        territorial_eu: false,
        decided_at: Some(to_ts(s.decided_at)),
    }
}

fn to_ts(dt: DateTime<Utc>) -> prost_types::Timestamp {
    prost_types::Timestamp { seconds: dt.timestamp(), nanos: dt.timestamp_subsec_nanos() as i32 }
}

// ── Enum conversions ──────────────────────────────────────────────────────────

fn entity_type_to_proto(e: EntityType) -> i32 {
    let p = match e {
        EntityType::Post => proto::EntityType::Post,
        EntityType::Comment => proto::EntityType::Comment,
        EntityType::ChatMessage => proto::EntityType::ChatMessage,
        EntityType::Media => proto::EntityType::Media,
        EntityType::Account => proto::EntityType::Account,
        EntityType::Profile => proto::EntityType::Profile,
    };
    p as i32
}

fn entity_type_from_proto(v: i32) -> Result<EntityType, Status> {
    match proto::EntityType::try_from(v) {
        Ok(proto::EntityType::Post) => Ok(EntityType::Post),
        Ok(proto::EntityType::Comment) => Ok(EntityType::Comment),
        Ok(proto::EntityType::ChatMessage) => Ok(EntityType::ChatMessage),
        Ok(proto::EntityType::Media) => Ok(EntityType::Media),
        Ok(proto::EntityType::Account) => Ok(EntityType::Account),
        Ok(proto::EntityType::Profile) => Ok(EntityType::Profile),
        _ => Err(Status::invalid_argument("entity_type is unspecified or unknown")),
    }
}

fn category_to_proto(c: PolicyCategory) -> i32 {
    let p = match c {
        PolicyCategory::Spam => proto::PolicyCategory::Spam,
        PolicyCategory::Harassment => proto::PolicyCategory::Harassment,
        PolicyCategory::Hate => proto::PolicyCategory::Hate,
        PolicyCategory::ViolentExtremism => proto::PolicyCategory::ViolentExtremism,
        PolicyCategory::Csam => proto::PolicyCategory::Csam,
        PolicyCategory::Ncii => proto::PolicyCategory::Ncii,
        PolicyCategory::SelfHarm => proto::PolicyCategory::SelfHarm,
        PolicyCategory::Misinformation => proto::PolicyCategory::Misinformation,
        PolicyCategory::Other => proto::PolicyCategory::Other,
    };
    p as i32
}

fn category_from_proto(v: i32) -> Result<PolicyCategory, Status> {
    match proto::PolicyCategory::try_from(v) {
        Ok(proto::PolicyCategory::Spam) => Ok(PolicyCategory::Spam),
        Ok(proto::PolicyCategory::Harassment) => Ok(PolicyCategory::Harassment),
        Ok(proto::PolicyCategory::Hate) => Ok(PolicyCategory::Hate),
        Ok(proto::PolicyCategory::ViolentExtremism) => Ok(PolicyCategory::ViolentExtremism),
        Ok(proto::PolicyCategory::Csam) => Ok(PolicyCategory::Csam),
        Ok(proto::PolicyCategory::Ncii) => Ok(PolicyCategory::Ncii),
        Ok(proto::PolicyCategory::SelfHarm) => Ok(PolicyCategory::SelfHarm),
        Ok(proto::PolicyCategory::Misinformation) => Ok(PolicyCategory::Misinformation),
        Ok(proto::PolicyCategory::Other) => Ok(PolicyCategory::Other),
        _ => Err(Status::invalid_argument("category is unspecified or unknown")),
    }
}

fn action_to_proto(a: ActionType) -> i32 {
    let p = match a {
        ActionType::NoAction => proto::ActionType::NoAction,
        ActionType::Warn => proto::ActionType::Warn,
        ActionType::VisibilityLimit => proto::ActionType::VisibilityLimit,
        ActionType::AgeGate => proto::ActionType::AgeGate,
        ActionType::RemoveContent => proto::ActionType::RemoveContent,
        ActionType::RestrictActor => proto::ActionType::RestrictActor,
        ActionType::Suspend => proto::ActionType::Suspend,
        ActionType::Ban => proto::ActionType::Ban,
    };
    p as i32
}

fn action_from_proto(v: i32) -> Result<ActionType, Status> {
    match proto::ActionType::try_from(v) {
        Ok(proto::ActionType::NoAction) => Ok(ActionType::NoAction),
        Ok(proto::ActionType::Warn) => Ok(ActionType::Warn),
        Ok(proto::ActionType::VisibilityLimit) => Ok(ActionType::VisibilityLimit),
        Ok(proto::ActionType::AgeGate) => Ok(ActionType::AgeGate),
        Ok(proto::ActionType::RemoveContent) => Ok(ActionType::RemoveContent),
        Ok(proto::ActionType::RestrictActor) => Ok(ActionType::RestrictActor),
        Ok(proto::ActionType::Suspend) => Ok(ActionType::Suspend),
        Ok(proto::ActionType::Ban) => Ok(ActionType::Ban),
        _ => Err(Status::invalid_argument("action is unspecified or unknown")),
    }
}

fn case_status_to_proto(s: CaseStatus) -> i32 {
    let p = match s {
        CaseStatus::Open => proto::CaseStatus::Open,
        CaseStatus::Triaged => proto::CaseStatus::Triaged,
        CaseStatus::Actioned => proto::CaseStatus::Actioned,
        CaseStatus::Dismissed => proto::CaseStatus::Dismissed,
        CaseStatus::Appealed => proto::CaseStatus::Appealed,
    };
    p as i32
}

fn case_status_filter(v: i32) -> Option<CaseStatus> {
    match proto::CaseStatus::try_from(v) {
        Ok(proto::CaseStatus::Open) => Some(CaseStatus::Open),
        Ok(proto::CaseStatus::Triaged) => Some(CaseStatus::Triaged),
        Ok(proto::CaseStatus::Actioned) => Some(CaseStatus::Actioned),
        Ok(proto::CaseStatus::Dismissed) => Some(CaseStatus::Dismissed),
        Ok(proto::CaseStatus::Appealed) => Some(CaseStatus::Appealed),
        _ => None, // UNSPECIFIED ⇒ all open work
    }
}

fn enforcement_status_to_proto(s: EnforcementStatus) -> i32 {
    let p = match s {
        EnforcementStatus::Active => proto::EnforcementStatus::Active,
        EnforcementStatus::Expired => proto::EnforcementStatus::Expired,
        EnforcementStatus::Reversed => proto::EnforcementStatus::Reversed,
    };
    p as i32
}

fn appeal_status_to_proto(s: crate::domain::value_object::AppealStatus) -> i32 {
    use crate::domain::value_object::AppealStatus;
    let p = match s {
        AppealStatus::Filed => proto::AppealStatus::Filed,
        AppealStatus::UnderReview => proto::AppealStatus::UnderReview,
        AppealStatus::Upheld => proto::AppealStatus::Upheld,
        AppealStatus::Overturned => proto::AppealStatus::Overturned,
    };
    p as i32
}

fn verdict_to_proto(v: ScreenVerdict) -> i32 {
    let p = match v {
        ScreenVerdict::Allow => proto::ScreenVerdict::Allow,
        ScreenVerdict::Block => proto::ScreenVerdict::Block,
        ScreenVerdict::Review => proto::ScreenVerdict::Review,
    };
    p as i32
}

/// Maps a [`ModerationError`] to a gRPC [`Status`] using its [`AppError`]
/// metadata, so the HTTP semantics defined once in `error.rs` drive the gRPC code.
pub fn status(err: ModerationError) -> Status {
    let msg = err.to_string();
    let retryable = err.is_retryable();
    match err.http_status().as_u16() {
        401 => Status::unauthenticated(msg),
        403 => Status::permission_denied(msg),
        404 => Status::not_found(msg),
        409 if retryable => Status::aborted(msg),
        409 => Status::already_exists(msg),
        429 => Status::resource_exhausted(msg),
        451 => Status::permission_denied(msg), // content blocked for legal reasons
        400 | 422 => Status::failed_precondition(msg),
        502 | 503 => Status::unavailable(msg),
        _ => Status::internal(msg),
    }
}
