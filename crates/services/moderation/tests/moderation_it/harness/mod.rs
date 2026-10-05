//! Integration harness: boots ephemeral Postgres + Scylla + Redis containers,
//! applies the `.sql` + `.cql` migrations, and wires a real moderation graph
//! against them through the production composition root
//! ([`moderation::app::App::compose`]). The only external dependency — the
//! `account` directory — is stubbed at its port boundary.
#![allow(dead_code)]

use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use fred::interfaces::KeysInterface;
use sqlx::PgPool;
use tonic::{Request, Status};

use moderation::app::{App, AppDeps};
use moderation::application::command::{
    IngestReportCommand, IngestReportHandler, IngestSignalCommand, IngestSignalHandler,
};
use moderation::application::port::AccountDirectory;
use moderation::application::ModerationPolicy;
use moderation::domain::value_object::ActorId;
use moderation::error::ModerationError;
use moderation::infrastructure::cache::{RedisEnforcementProjection, RedisScreenCorpus};
use moderation::infrastructure::classifier::LogClassifierGateway;
use moderation::infrastructure::grpc::{proto, ModerationServiceHandler};
use moderation::infrastructure::history::ScyllaEvidenceHistory;
use moderation::infrastructure::persistence::{
    PgAppealRepository, PgCaseRepository, PgDecisionRepository, PgEnforcementRepository,
    PgPenaltyRepository, PgReportRepository,
};

use postgres_storage::config::StatementLogLevel;
use postgres_storage::{PgPoolBuilder, PostgresConfig, TransactionManager};
use redis_storage::{RedisClient, RedisClientBuilder, RedisConfig};
use scylla_storage::{ScyllaConfig, ScyllaSessionBuilder};

const MIGRATIONS_DIR: &str = concat!(env!("CARGO_MANIFEST_DIR"), "/migrations");
const KEYSPACE: &str = "moderation";

/// `account` stub: every actor exists (so actor-level enforcement is permitted).
struct StubAccounts;

#[async_trait]
impl AccountDirectory for StubAccounts {
    async fn actor_exists(&self, _actor_id: &ActorId) -> Result<bool, ModerationError> {
        Ok(true)
    }
}

/// Report subject resolution stub: every `post-*` id belongs to [`OWNER`];
/// anything else does not exist.
struct StubSubjects;

/// The account every reportable `post-*` stub content belongs to.
pub const OWNER: uuid::Uuid = uuid::Uuid::from_u128(0x0190_f0a0_0000_7000_8000_0000_0000_0007);

#[async_trait]
impl moderation::application::port::SubjectResolver for StubSubjects {
    async fn responsible_account(
        &self,
        _entity_type: moderation::domain::value_object::EntityType,
        entity_id: &str,
    ) -> Result<Option<ActorId>, ModerationError> {
        Ok(entity_id.starts_with("post-").then(|| ActorId::from_uuid(OWNER)))
    }

    async fn profiles_of(&self, account: &ActorId) -> Result<Vec<String>, ModerationError> {
        Ok(vec![format!("profile-of-{}", account.as_str())])
    }
}

/// Report quota in this suite: 3 per hour (real Redis counters).
pub const REPORTS_PER_HOUR: u32 = 3;

pub struct Harness {
    pub handler: ModerationServiceHandler,
    pub ingest_report: Arc<IngestReportHandler>,
    pub ingest_signal: Arc<IngestSignalHandler>,
    pub pool: PgPool,
    pub redis: RedisClient,
}

impl Harness {
    pub async fn start() -> Self {
        let pg_url = test_support::containers::postgres_ready(MIGRATIONS_DIR).await;
        let scylla_cp = test_support::containers::scylla_ready(KEYSPACE, MIGRATIONS_DIR).await;
        let redis_endpoint = test_support::containers::redis_endpoint().await;

        let pg_config = PostgresConfig {
            database_url: pg_url,
            max_connections: 8,
            min_connections: 1,
            acquire_timeout: Duration::from_secs(5),
            idle_timeout: None,
            max_lifetime: None,
            statement_log_level: StatementLogLevel::Debug,
            slow_statement_threshold: Duration::from_millis(500),
        };
        let pool = PgPoolBuilder::build(pg_config).await.expect("it: postgres pool");
        let tx = TransactionManager::new(pool.clone());

        let scylla = Arc::new(
            ScyllaSessionBuilder::new(ScyllaConfig {
                contact_points: vec![scylla_cp],
                keyspace: None,
                ..ScyllaConfig::default()
            })
            .build()
            .await
            .expect("it: scylla client"),
        );

        let redis = RedisClientBuilder::new(RedisConfig {
            hosts: vec![redis_endpoint],
            ..RedisConfig::default()
        })
        .build()
        .await
        .expect("it: redis client");

        // The evidence history (Scylla) is the publisher, so the suite exercises all
        // three stores: decisions/cases (Postgres), projection/corpus (Redis),
        // history (Scylla).
        let publisher = Arc::new(ScyllaEvidenceHistory::new(scylla.clone()));
        let cases = Arc::new(PgCaseRepository::new(tx.clone()));
        let classifiers = Arc::new(LogClassifierGateway);

        let reports = Arc::new(PgReportRepository::new(tx.clone()));
        let ingest_report = Arc::new(IngestReportHandler::new(
            reports.clone(),
            cases.clone(),
            publisher.clone(),
            classifiers.clone(),
        ));
        let ingest_signal = Arc::new(IngestSignalHandler::new(cases.clone(), publisher.clone()));

        let deps = AppDeps {
            cases,
            reports,
            decisions: Arc::new(PgDecisionRepository::new(tx.clone())),
            enforcements: Arc::new(PgEnforcementRepository::new(tx.clone())),
            penalties: Arc::new(PgPenaltyRepository::new(tx.clone())),
            appeals: Arc::new(PgAppealRepository::new(tx.clone())),
            projection: Arc::new(RedisEnforcementProjection::new(redis.clone())),
            corpus: Arc::new(RedisScreenCorpus::new(redis.clone())),
            classifiers,
            accounts: Arc::new(StubAccounts),
            publisher,
            subjects: Arc::new(StubSubjects),
            report_quota: Arc::new(moderation::infrastructure::cache::RedisReportRateLimiter::new(
                redis.clone(),
                REPORTS_PER_HOUR,
                100,
            )),
            policy: ModerationPolicy::standard(),
        };

        Self {
            handler: App::compose(deps),
            ingest_report,
            ingest_signal,
            pool,
            redis,
        }
    }

    // ── Corpus seeding ─────────────────────────────────────────────────────────

    /// Seeds a known-bad corpus entry the screen path will match.
    pub async fn seed_corpus(&self, algorithm: &str, value: &str, categories: &[&str], reference: &str) {
        let key = format!("mod:corpus:{{{algorithm}}}:{value}");
        let payload = serde_json::json!({ "categories": categories, "reference": reference }).to_string();
        let _: () = self.redis.set(key, payload, None, None, false).await.expect("seed corpus");
    }

    // ── RPC helpers ────────────────────────────────────────────────────────────

    pub async fn screen(
        &self,
        subject: proto::SubjectRef,
        algorithm: &str,
        value: &str,
        categories: Vec<i32>,
    ) -> Result<proto::ScreenResponse, Status> {
        let request = Request::new(proto::ScreenRequest {
            subject: Some(subject),
            hashes: vec![proto::ContentHash { algorithm: algorithm.into(), value: value.into() }],
            text: String::new(),
            categories,
        });
        self.handler.screen(request).await.map(|r| r.into_inner())
    }

    pub async fn open_case(
        &self,
        subject: proto::SubjectRef,
        category: i32,
    ) -> Result<proto::OpenCaseResponse, Status> {
        let request = Request::new(proto::OpenCaseRequest {
            subject: Some(subject),
            category,
            reason: "default".into(),
        });
        self.handler.open_case(request).await.map(|r| r.into_inner())
    }

    pub async fn decide_case(
        &self,
        case_id: &str,
        action: i32,
        category: i32,
    ) -> Result<proto::DecideCaseResponse, Status> {
        let request = Request::new(proto::DecideCaseRequest {
            case_id: case_id.into(),
            action,
            category,
            rationale: "violation".into(),
            reviewer_id: "rev-1".into(),
            policy_version: "2026.06.1".into(),
        });
        self.handler.decide_case(request).await.map(|r| r.into_inner())
    }

    pub async fn file_appeal(
        &self,
        decision_id: &str,
        actor_id: &str,
    ) -> Result<proto::FileAppealResponse, Status> {
        let request = Request::new(proto::FileAppealRequest {
            decision_id: decision_id.into(),
            actor_id: actor_id.into(),
            statement: "I dispute this".into(),
        });
        self.handler.file_appeal(request).await.map(|r| r.into_inner())
    }

    pub async fn resolve_appeal(
        &self,
        appeal_id: &str,
        overturn: bool,
    ) -> Result<proto::ResolveAppealResponse, Status> {
        let request = Request::new(proto::ResolveAppealRequest {
            appeal_id: appeal_id.into(),
            overturn,
            rationale: "reviewed".into(),
            reviewer_id: "rev-2".into(),
        });
        self.handler.resolve_appeal(request).await.map(|r| r.into_inner())
    }

    pub async fn enforcement_state(
        &self,
        actor_id: &str,
    ) -> Result<proto::GetEnforcementStateResponse, Status> {
        let request = Request::new(proto::GetEnforcementStateRequest { actor_id: actor_id.into() });
        self.handler.get_enforcement_state(request).await.map(|r| r.into_inner())
    }

    pub async fn statement_of_reasons(
        &self,
        decision_id: &str,
    ) -> Result<proto::GetStatementOfReasonsResponse, Status> {
        let request =
            Request::new(proto::GetStatementOfReasonsRequest { decision_id: decision_id.into() });
        self.handler.get_statement_of_reasons(request).await.map(|r| r.into_inner())
    }

    pub async fn ingest_report(
        &self,
        cmd: IngestReportCommand,
    ) -> Result<(), ModerationError> {
        use cqrs::Envelope;
        self.ingest_report
            .handle(Envelope::new(uuid::Uuid::now_v7(), cmd), chrono::Utc::now())
            .await
            .map(|_| ())
    }

    pub async fn ingest_signal(
        &self,
        cmd: IngestSignalCommand,
    ) -> Result<(), ModerationError> {
        use cqrs::Envelope;
        self.ingest_signal
            .handle(Envelope::new(uuid::Uuid::now_v7(), cmd), chrono::Utc::now())
            .await
            .map(|_| ())
    }

    // ── Direct DB assertions ─────────────────────────────────────────────────

    pub async fn count_decisions(&self, actor_id: &str) -> i64 {
        let id = uuid::Uuid::parse_str(actor_id).unwrap();
        sqlx::query_scalar("SELECT COUNT(*) FROM decisions WHERE actor_id = $1")
            .bind(id)
            .fetch_one(&self.pool)
            .await
            .expect("count decisions")
    }

    pub async fn count_enforcements(&self, actor_id: &str, status: &str) -> i64 {
        let id = uuid::Uuid::parse_str(actor_id).unwrap();
        sqlx::query_scalar("SELECT COUNT(*) FROM enforcements WHERE actor_id = $1 AND status = $2")
            .bind(id)
            .bind(status)
            .fetch_one(&self.pool)
            .await
            .expect("count enforcements")
    }

    pub async fn count_cases(&self, actor_id: &str) -> i64 {
        let id = uuid::Uuid::parse_str(actor_id).unwrap();
        sqlx::query_scalar("SELECT COUNT(*) FROM cases WHERE actor_id = $1")
            .bind(id)
            .fetch_one(&self.pool)
            .await
            .expect("count cases")
    }

    /// Number of accrued evidence signals on an actor's (single) case.
    pub async fn count_signals(&self, actor_id: &str) -> i64 {
        let id = uuid::Uuid::parse_str(actor_id).unwrap();
        sqlx::query_scalar("SELECT COALESCE(jsonb_array_length(signals), 0)::bigint FROM cases WHERE actor_id = $1")
            .bind(id)
            .fetch_one(&self.pool)
            .await
            .expect("count signals")
    }
}

/// A proto subject with a fresh actor — so each scenario is isolated.
pub fn subject(entity_type: proto::EntityType, surface: &str) -> proto::SubjectRef {
    proto::SubjectRef {
        entity_type: entity_type as i32,
        entity_id: format!("e-{}", uuid::Uuid::now_v7()),
        actor_id: uuid::Uuid::now_v7().to_string(),
        surface: surface.into(),
    }
}
