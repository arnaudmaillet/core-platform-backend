//! Adapts the moderation composition root to the fleet [`service_runtime::Service`]
//! contract. Maps env → config, defers to [`App::build`], self-spawns the Plane A
//! ingestion consumers (mirroring how profile/chat spawn their workers in
//! `build`), registers the concrete tonic service, and reports Postgres + Scylla +
//! Redis liveness via the storage crates' ready-made probes.

use std::sync::Arc;
use std::time::Duration;

use anyhow::Context;
use async_trait::async_trait;
use postgres_storage::PostgresConfig;
use redis_storage::RedisConfig;
use scylla_storage::ScyllaConfig;
use service_runtime::{HealthProbe, InfraRegistry, Service};
use service_runtime::edge::{authenticated, member_or_guest};
use service_runtime::EdgePolicy;
use tonic::service::RoutesBuilder;
use tonic_reflection::server::Builder as ReflectionBuilder;
use transport::kafka::config::{ConsumerConfig, KafkaClientConfig, ProducerConfig};
use transport::kafka::consumer::{KafkaConsumerBuilder, KafkaConsumerHandle};
use transport::kafka::producer::{KafkaProducerBuilder, KafkaProducerHandle};

use crate::app::{App, Backends};
use crate::application::command::{IngestReportHandler, IngestSignalHandler};
use crate::config::ModerationConfig;
use crate::infrastructure::consumer::{run_report_consumer, run_signal_consumer};
use crate::infrastructure::grpc::server::FILE_DESCRIPTOR_SET;
use crate::infrastructure::grpc::{ModerationServiceHandler, ModerationServiceServer};

const REPORTS_TOPIC: &str = "moderation.reports";
const REPORTS_GROUP: &str = "moderation-report-consumer";
const SIGNALS_TOPIC: &str = "moderation.signals";
const SIGNALS_GROUP: &str = "moderation-signal-consumer";
/// Backoff before respawning a consumer after the runner returns.
const CONSUMER_RESPAWN_BACKOFF: Duration = Duration::from_secs(5);

/// The concrete tonic server type, named once so the health key and reflection
/// registration agree.
type ModerationServer = ModerationServiceServer<ModerationServiceHandler>;

/// The moderation service as hosted by [`service_runtime`].
pub struct ModerationService {
    app: App,
}

#[async_trait]
impl Service for ModerationService {
    const NAME: &'static str = "moderation";
    const VERSION: &'static str = env!("CARGO_PKG_VERSION");
    const GRPC_SERVICE_NAME: &'static str =
        <ModerationServer as tonic::server::NamedService>::NAME;

    /// The RPCs exposed on the client edge listener (`GRPC_EDGE_ADDR`); anything
    /// else on this service is mesh-only. See `transport::grpc::edge`.
    // The reviewer console (assign/decide/queue/resolve) stays mesh-only until a
    // staff permission catalogue exists; Screen is a mesh callee (media).
    const EDGE_POLICY: EdgePolicy = &[
        // DSA Art. 17 / 20: the sanctioned account reads its restrictions, each
        // one's statement of reasons, and appeals — its own only (actor_id = sub;
        // a statement of someone else's decision reads as NOT_FOUND).
        authenticated("/moderation.v1.ModerationService/GetEnforcementState"),
        authenticated("/moderation.v1.ModerationService/GetStatementOfReasons"),
        authenticated("/moderation.v1.ModerationService/FileAppeal"),
        // DSA Art. 20(4)–(5): the appellant follows their appeals (their own:
        // the appellant is the token).
        authenticated("/moderation.v1.ModerationService/ListMyAppeals"),
        // DSA Art. 16: anyone may report, guests included (reporter = the token),
        // and is told what became of the report.
        member_or_guest("/moderation.v1.ModerationService/SubmitReport"),
        member_or_guest("/moderation.v1.ModerationService/ListMyReports"),
    ];

    async fn build(_infra: Arc<InfraRegistry>) -> anyhow::Result<Self> {
        let config = ModerationConfig::from_env()?;
        let backends = Backends {
            postgres: PostgresConfig::from_env(),
            scylla: ScyllaConfig::from_env(),
            redis: RedisConfig::from_env(),
            kafka: Some(KafkaClientConfig::from_env()),
        };

        let mut app = App::build(config, backends)
            .await
            .map_err(|e| anyhow::anyhow!("moderation app build: {e}"))?;
        // Its mesh-only reads check who calls them (#852).
        app.handler = app.handler.with_mesh_gate(service_runtime::mesh_gate_from_env());

        // Plane A inbound integration: user reports + classifier signals.
        spawn_report_consumer(Arc::clone(&app.ingest_report));
        spawn_signal_consumer(Arc::clone(&app.ingest_signal));

        Ok(Self { app })
    }

    fn health_probes(&self) -> Vec<Arc<dyn HealthProbe>> {
        vec![
            postgres_storage::health::probe(self.app.pool.clone()),
            scylla_storage::health::probe(Arc::clone(&self.app.scylla)),
            redis_storage::health::probe(self.app.redis.clone()),
        ]
    }

    fn register(self, routes: &mut RoutesBuilder) -> anyhow::Result<()> {
        let reflection = ReflectionBuilder::configure()
            .register_encoded_file_descriptor_set(FILE_DESCRIPTOR_SET)
            .build_v1()?;

        routes.add_service(reflection);
        routes.add_service(ModerationServiceServer::new(self.app.handler));
        Ok(())
    }
}

/// Spawns the supervised report consumer, rebuilding its Kafka handles and
/// restarting after a backoff whenever the runner returns (per its contract).
fn spawn_report_consumer(handler: Arc<IngestReportHandler>) {
    tokio::spawn(async move {
        loop {
            match build_consumer(REPORTS_TOPIC, REPORTS_GROUP) {
                Ok((consumer, producer)) => {
                    run_report_consumer(consumer, Arc::clone(&handler), producer).await;
                    tracing::warn!("report consumer exited; respawning after backoff");
                }
                Err(error) => tracing::error!(%error, "failed to build report consumer; retrying"),
            }
            tokio::time::sleep(CONSUMER_RESPAWN_BACKOFF).await;
        }
    });
}

/// Spawns the supervised classifier-signal consumer.
fn spawn_signal_consumer(handler: Arc<IngestSignalHandler>) {
    tokio::spawn(async move {
        loop {
            match build_consumer(SIGNALS_TOPIC, SIGNALS_GROUP) {
                Ok((consumer, producer)) => {
                    run_signal_consumer(consumer, Arc::clone(&handler), producer).await;
                    tracing::warn!("signal consumer exited; respawning after backoff");
                }
                Err(error) => tracing::error!(%error, "failed to build signal consumer; retrying"),
            }
            tokio::time::sleep(CONSUMER_RESPAWN_BACKOFF).await;
        }
    });
}

/// Builds a manual-commit consumer (subscribed to `topic`) and the dead-letter
/// producer the runner needs.
fn build_consumer(
    topic: &str,
    group: &str,
) -> anyhow::Result<(KafkaConsumerHandle, KafkaProducerHandle)> {
    let kafka = KafkaClientConfig::from_env();
    let consumer = KafkaConsumerBuilder::new(ConsumerConfig::new(kafka.clone(), group))
        .subscribe(topic)
        .build()
        .with_context(|| format!("build consumer for {topic}"))?;
    let producer = KafkaProducerBuilder::new(ProducerConfig::new(kafka))
        .build()
        .with_context(|| format!("build dead-letter producer for {topic}"))?;
    Ok((consumer, producer))
}
