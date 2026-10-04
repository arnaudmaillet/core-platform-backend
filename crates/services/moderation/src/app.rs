//! The moderation service's composition root.
//!
//! [`App::compose`] is *pure* wiring: the ten port handles in, the assembled gRPC
//! handler out — it binds no socket and reads no environment, so the live
//! integration harness and the binary entrypoint build the exact same graph.
//! [`App::build`] is the I/O variant that constructs the concrete adapters from
//! config + backend connections, then defers to `compose`. It also retains the
//! ingestion handlers so [`crate::service`] can self-spawn the Plane A consumers.

use std::sync::Arc;

use postgres_storage::{PgPoolBuilder, PostgresConfig, TransactionManager};
use redis_storage::{RedisClient, RedisClientBuilder, RedisConfig};
use scylla_storage::{ScyllaClient, ScyllaConfig, ScyllaSessionBuilder};
use sqlx::PgPool;
use tonic::transport::Channel;
use transport::kafka::config::client::KafkaClientConfig;
use transport::kafka::config::producer::ProducerConfig;
use transport::kafka::producer::KafkaProducerBuilder;

use crate::application::command::{
    AssignCaseHandler, DecideCaseHandler, FileAppealHandler, IngestReportHandler,
    IngestSignalHandler, OpenCaseHandler, ResolveAppealHandler, ScreenHandler, SubmitReportHandler};
use crate::application::port::{
    AccountDirectory, AppealRepository, CaseRepository, ClassifierGateway, DecisionRepository,
    EnforcementProjection, EnforcementRepository, EventPublisher, PenaltyRepository, ScreenCorpus, ReportRateLimiter, SubjectResolver};
use crate::application::query::{
    GetEnforcementStateHandler, GetStatementOfReasonsHandler, ListQueueHandler,
};
use crate::application::ModerationPolicy;
use crate::config::ModerationConfig;
use crate::infrastructure::cache::{RedisEnforcementProjection, RedisScreenCorpus, RedisReportRateLimiter};
use crate::infrastructure::classifier::LogClassifierGateway;
use crate::infrastructure::directory::{GrpcAccountDirectory, GrpcSubjectResolver};
use crate::infrastructure::event::{
    FanoutEventPublisher, KafkaEventPublisher, LogEventPublisher,
};
use crate::infrastructure::grpc::ModerationServiceHandler;
use crate::infrastructure::history::ScyllaEvidenceHistory;
use crate::infrastructure::persistence::{
    PgAppealRepository, PgCaseRepository, PgDecisionRepository, PgEnforcementRepository,
    PgPenaltyRepository,
};

/// The ten ports the application layer depends on, plus the policy.
pub struct AppDeps {
    pub cases: Arc<dyn CaseRepository>,
    pub decisions: Arc<dyn DecisionRepository>,
    pub enforcements: Arc<dyn EnforcementRepository>,
    pub penalties: Arc<dyn PenaltyRepository>,
    pub appeals: Arc<dyn AppealRepository>,
    pub projection: Arc<dyn EnforcementProjection>,
    pub corpus: Arc<dyn ScreenCorpus>,
    pub classifiers: Arc<dyn ClassifierGateway>,
    pub accounts: Arc<dyn AccountDirectory>,
    pub publisher: Arc<dyn EventPublisher>,
    /// Whose content a client report targets (post / comment / profile lookups).
    pub subjects: Arc<dyn SubjectResolver>,
    /// Per-reporter report quotas.
    pub report_quota: Arc<dyn ReportRateLimiter>,
    pub policy: ModerationPolicy,
}

/// Backend connection configs. `kafka` is optional: absent ⇒ the log publisher.
pub struct Backends {
    pub postgres: PostgresConfig,
    pub scylla: ScyllaConfig,
    pub redis: RedisConfig,
    pub kafka: Option<KafkaClientConfig>,
}

/// A fully-wired moderation service. Retains the storage clients so the runtime
/// builds liveness probes over the same connections, and the ingestion handlers
/// so the service self-spawns the Plane A consumers.
pub struct App {
    pub handler: ModerationServiceHandler,
    pub ingest_report: Arc<IngestReportHandler>,
    pub ingest_signal: Arc<IngestSignalHandler>,
    pub pool: PgPool,
    pub scylla: Arc<ScyllaClient>,
    pub redis: RedisClient,
}

impl App {
    /// Pure composition: assemble the nine application handlers from the ports and
    /// wrap them in the gRPC handler. No I/O — drives the unit/integration graph.
    pub fn compose(deps: AppDeps) -> ModerationServiceHandler {
        let screen = Arc::new(ScreenHandler::new(
            Arc::clone(&deps.corpus),
            Arc::clone(&deps.decisions),
            Arc::clone(&deps.enforcements),
            Arc::clone(&deps.penalties),
            Arc::clone(&deps.projection),
            Arc::clone(&deps.publisher),
            deps.policy.clone(),
        ));
        let open_case =
            Arc::new(OpenCaseHandler::new(Arc::clone(&deps.cases), Arc::clone(&deps.publisher)));
        let assign_case = Arc::new(AssignCaseHandler::new(Arc::clone(&deps.cases)));
        let decide_case = Arc::new(DecideCaseHandler::new(
            Arc::clone(&deps.cases),
            Arc::clone(&deps.decisions),
            Arc::clone(&deps.enforcements),
            Arc::clone(&deps.penalties),
            Arc::clone(&deps.projection),
            Arc::clone(&deps.accounts),
            Arc::clone(&deps.publisher),
            deps.policy.clone(),
        ));
        let list_queue = Arc::new(ListQueueHandler::new(Arc::clone(&deps.cases)));
        let file_appeal = Arc::new(FileAppealHandler::new(
            Arc::clone(&deps.decisions),
            Arc::clone(&deps.appeals),
            Arc::clone(&deps.cases),
        ));
        let resolve_appeal = Arc::new(ResolveAppealHandler::new(
            Arc::clone(&deps.appeals),
            Arc::clone(&deps.decisions),
            Arc::clone(&deps.enforcements),
            Arc::clone(&deps.cases),
            Arc::clone(&deps.projection),
            Arc::clone(&deps.publisher),
        ));
        let statement_of_reasons =
            Arc::new(GetStatementOfReasonsHandler::new(Arc::clone(&deps.decisions)));
        let enforcement_state = Arc::new(GetEnforcementStateHandler::new(
            Arc::clone(&deps.projection),
            Arc::clone(&deps.enforcements),
        ));
        let submit_report = Arc::new(SubmitReportHandler::new(
            Arc::clone(&deps.subjects),
            Arc::clone(&deps.report_quota),
            Arc::new(IngestReportHandler::new(
                Arc::clone(&deps.cases),
                Arc::clone(&deps.publisher),
                Arc::clone(&deps.classifiers),
            )),
        ));

        ModerationServiceHandler::new(
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
        )
    }

    /// Builds the concrete adapter graph from config + backend connections.
    pub async fn build(
        config: ModerationConfig,
        backends: Backends,
    ) -> Result<App, Box<dyn std::error::Error>> {
        let pool = PgPoolBuilder::build(backends.postgres).await?;
        let tx = TransactionManager::new(pool.clone());
        let scylla = Arc::new(ScyllaSessionBuilder::new(backends.scylla).build().await?);
        let redis = RedisClientBuilder::new(backends.redis).build().await?;

        // Kafka is the authoritative Plane B notification; the Scylla evidence
        // history is a best-effort audit sink composed alongside it.
        let primary: Arc<dyn EventPublisher> = match backends.kafka {
            Some(cfg) => {
                let producer = KafkaProducerBuilder::new(ProducerConfig::new(cfg)).build()?;
                Arc::new(KafkaEventPublisher::new(producer))
            }
            None => Arc::new(LogEventPublisher),
        };
        let history: Arc<dyn EventPublisher> = Arc::new(ScyllaEvidenceHistory::new(scylla.clone()));
        let publisher: Arc<dyn EventPublisher> =
            Arc::new(FanoutEventPublisher::new(primary, vec![history]));

        // Lazy connect: dials `account` on first use, so a cold start does not
        // require the dependency to be up at boot. Both deadlines are mandatory —
        // tonic has no default request timeout.
        let channel = Channel::from_shared(config.account_endpoint)?
            .timeout(config.account_rpc_timeout)
            .connect_timeout(config.account_connect_timeout)
            .connect_lazy();

        // Report subject resolution: post / comment / profile over the mesh,
        // lazily connected, with the same mandatory deadlines.
        let lazy = |endpoint: String| -> Result<Channel, Box<dyn std::error::Error>> {
            Ok(Channel::from_shared(endpoint)?
                .timeout(config.content_rpc_timeout)
                .connect_timeout(config.content_connect_timeout)
                .connect_lazy())
        };
        let subjects = Arc::new(GrpcSubjectResolver::new(
            lazy(config.post_endpoint.clone())?,
            lazy(config.comment_endpoint.clone())?,
            lazy(config.profile_endpoint.clone())?,
        ));
        let report_quota = Arc::new(RedisReportRateLimiter::new(
            redis.clone(),
            config.reports_per_hour,
            config.reports_per_day,
        ));

        let deps = AppDeps {
            cases: Arc::new(PgCaseRepository::new(tx.clone())),
            decisions: Arc::new(PgDecisionRepository::new(tx.clone())),
            enforcements: Arc::new(PgEnforcementRepository::new(tx.clone())),
            penalties: Arc::new(PgPenaltyRepository::new(tx.clone())),
            appeals: Arc::new(PgAppealRepository::new(tx.clone())),
            projection: Arc::new(RedisEnforcementProjection::new(redis.clone())),
            corpus: Arc::new(RedisScreenCorpus::new(redis.clone())),
            classifiers: Arc::new(LogClassifierGateway),
            accounts: Arc::new(GrpcAccountDirectory::new(channel)),
            publisher,
            subjects,
            report_quota,
            policy: config.policy,
        };

        // The ingestion handlers share the same ports; build them before `compose`
        // consumes `deps`.
        let ingest_report = Arc::new(IngestReportHandler::new(
            Arc::clone(&deps.cases),
            Arc::clone(&deps.publisher),
            Arc::clone(&deps.classifiers),
        ));
        let ingest_signal = Arc::new(IngestSignalHandler::new(
            Arc::clone(&deps.cases),
            Arc::clone(&deps.publisher),
        ));

        let handler = App::compose(deps);
        Ok(App { handler, ingest_report, ingest_signal, pool, scylla, redis })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::application::command::OpenCaseCommand;
    use crate::application::fakes::Fixture;
    use crate::domain::value_object::{ActorId, EntityType, PolicyCategory, SubjectRef};
    use crate::infrastructure::grpc::proto;
    use cqrs::Envelope;
    use tonic::{Code, Request};
    use uuid::Uuid;

    /// Composes the gRPC handler over the in-memory fakes — the exact graph
    /// `App::build` produces, minus the real backends.
    fn handler_from_fakes(fx: &Fixture) -> ModerationServiceHandler {
        App::compose(AppDeps {
            cases: fx.cases.clone(),
            decisions: fx.decisions.clone(),
            enforcements: fx.enforcements.clone(),
            penalties: fx.penalties.clone(),
            appeals: fx.appeals.clone(),
            projection: fx.projection.clone(),
            corpus: fx.corpus.clone(),
            classifiers: fx.classifiers.clone(),
            accounts: fx.accounts.clone(),
            publisher: fx.publisher.clone(),
            subjects: fx.subjects.clone(),
            report_quota: fx.report_quota.clone(),
            policy: fx.policy.clone(),
        })
    }

    fn subject() -> SubjectRef {
        SubjectRef::new(EntityType::Media, "m1", ActorId::from_uuid(Uuid::from_u128(1)), "upload").unwrap()
    }

    #[tokio::test]
    async fn screen_rpc_blocks_a_known_bad_hash() {
        let fx = Fixture::new();
        fx.corpus.add_known_bad("abc", vec![PolicyCategory::Csam], "ncmec:1");
        let handler = handler_from_fakes(&fx);

        let request = Request::new(proto::ScreenRequest {
            subject: Some(proto::SubjectRef {
                entity_type: proto::EntityType::Media as i32,
                entity_id: "m1".into(),
                actor_id: ActorId::from_uuid(Uuid::from_u128(1)).as_str(),
                surface: "upload".into(),
            }),
            hashes: vec![proto::ContentHash { algorithm: "pdq".into(), value: "abc".into() }],
            text: String::new(),
            categories: vec![proto::PolicyCategory::Csam as i32],
        });

        let resp = handler.screen(request).await.unwrap().into_inner();
        assert_eq!(resp.verdict, proto::ScreenVerdict::Block as i32);
        assert_eq!(resp.match_reference, "ncmec:1");
    }

    #[tokio::test]
    async fn screen_rpc_allows_clean_content() {
        let fx = Fixture::new();
        let handler = handler_from_fakes(&fx);
        let request = Request::new(proto::ScreenRequest {
            subject: Some(proto::SubjectRef {
                entity_type: proto::EntityType::Media as i32,
                entity_id: "m1".into(),
                actor_id: ActorId::from_uuid(Uuid::from_u128(1)).as_str(),
                surface: "upload".into(),
            }),
            hashes: vec![proto::ContentHash { algorithm: "pdq".into(), value: "clean".into() }],
            text: String::new(),
            categories: vec![],
        });
        let resp = handler.screen(request).await.unwrap().into_inner();
        assert_eq!(resp.verdict, proto::ScreenVerdict::Allow as i32);
    }

    #[tokio::test]
    async fn open_then_decide_rpc_records_enforcement() {
        let fx = Fixture::new();
        let handler = handler_from_fakes(&fx);

        // Open via the application handler (deterministic case id) ...
        let opened = fx
            .open_case_handler()
            .handle(
                Envelope::new(
                    Uuid::now_v7(),
                    OpenCaseCommand {
                        subject: subject(),
                        category: PolicyCategory::Harassment,
                        queue: "default".into(),
                        priority: "normal".into(),
                    },
                ),
                crate::application::fakes::t0(),
            )
            .await
            .unwrap();

        // ... then decide via the gRPC handler.
        let request = Request::new(proto::DecideCaseRequest {
            case_id: opened.case.id().as_str(),
            action: proto::ActionType::RemoveContent as i32,
            category: proto::PolicyCategory::Harassment as i32,
            rationale: "violation".into(),
            reviewer_id: "rev-1".into(),
            policy_version: "2026.06.1".into(),
        });
        let resp = handler.decide_case(request).await.unwrap().into_inner();
        assert!(resp.decision.is_some());
        assert!(resp.enforcement.is_some());
    }

    #[tokio::test]
    async fn decide_unknown_case_is_not_found() {
        let fx = Fixture::new();
        let handler = handler_from_fakes(&fx);
        let request = Request::new(proto::DecideCaseRequest {
            case_id: Uuid::now_v7().to_string(),
            action: proto::ActionType::Warn as i32,
            category: proto::PolicyCategory::Spam as i32,
            rationale: "x".into(),
            reviewer_id: "r".into(),
            policy_version: "2026.06.1".into(),
        });
        let status = handler.decide_case(request).await.unwrap_err();
        assert_eq!(status.code(), Code::NotFound);
    }

    fn principal(sub: &str, kind: Option<&str>) -> transport::grpc::edge::EdgePrincipal {
        let mut claims = serde_json::json!({ "sub": sub, "exp": 4_102_444_800_i64 });
        if let Some(kind) = kind {
            claims["kind"] = serde_json::json!(kind);
        }
        let raw: auth_context::OidcClaims = serde_json::from_value(claims).unwrap();
        transport::grpc::edge::EdgePrincipal::new(std::sync::Arc::new(auth_context::CurrentPrincipal {
            user_id: auth_context::PrincipalId::new(sub),
            tenant_id: None,
            permissions: vec![auth_context::Permission::new("read:public")],
            raw_claims: raw,
        }))
    }

    fn report(entity_id: &str) -> proto::SubmitReportRequest {
        proto::SubmitReportRequest {
            entity_type: proto::EntityType::Post as i32,
            entity_id: entity_id.into(),
            category: proto::PolicyCategory::Spam as i32,
            reason: "spam".into(),
            surface: "post_menu".into(),
        }
    }

    #[tokio::test]
    async fn submit_report_rpc_takes_the_reporter_from_the_token_guests_included() {
        let fx = Fixture::new();
        fx.subjects.own("post-1", ActorId::from_uuid(Uuid::from_u128(7)));
        let handler = handler_from_fakes(&fx);

        let guest = Uuid::now_v7();
        let mut request = Request::new(report("post-1"));
        request.extensions_mut().insert(principal(&format!("guest:{guest}"), Some("guest")));
        let resp = handler.submit_report(request).await.expect("a guest may report").into_inner();
        assert!(!resp.report_id.is_empty());
        assert_eq!(fx.cases.count(), 1);

        let mut request = Request::new(report("post-1"));
        request.extensions_mut().insert(principal(&Uuid::now_v7().to_string(), None));
        handler.submit_report(request).await.expect("a member may report");

        // No principal (a mesh call): no reporter, refused.
        let status = handler.submit_report(Request::new(report("post-1"))).await.unwrap_err();
        assert_eq!(status.code(), Code::FailedPrecondition);
        // Unknown content.
        let mut request = Request::new(report("missing"));
        request.extensions_mut().insert(principal(&format!("guest:{guest}"), Some("guest")));
        assert_eq!(handler.submit_report(request).await.unwrap_err().code(), Code::NotFound);
    }
}
