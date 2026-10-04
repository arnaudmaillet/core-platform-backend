//! Adapts the engagement composition root to the fleet
//! [`service_runtime::Service`] contract.
//!
//! Engagement is Redis-primary (the always-on hot path) with a ScyllaDB
//! write-behind ledger driven by Kafka workers spawned inside [`App::build`].
//! Readiness therefore gates on Redis only. Reaction weights are loaded from the
//! externalized weights config.

use std::sync::Arc;

use async_trait::async_trait;
use cqrs::command::InMemoryCommandBus;
use cqrs::query::InMemoryQueryBus;
use redis_storage::RedisConfig;
use scylla_storage::ScyllaConfig;
use service_runtime::{HealthProbe, InfraRegistry, Service};
use service_runtime::edge::{authenticated, public_read};
use service_runtime::EdgePolicy;
use tonic::service::RoutesBuilder;
use tonic_reflection::server::Builder as ReflectionBuilder;
use transport::kafka::config::{KafkaClientConfig, ProducerConfig};
use transport::kafka::producer::KafkaProducerBuilder;

use crate::app::{App, Backends};
use crate::config::ReactionWeightsConfig;
use crate::infrastructure::grpc::handler::engagement_handler::EngagementServiceServer;
use crate::infrastructure::grpc::handler::EngagementServiceHandler;
use crate::infrastructure::grpc::server::FILE_DESCRIPTOR_SET;
use crate::infrastructure::publisher::KafkaEngagementEventPublisher;

type EngagementServer =
    EngagementServiceServer<EngagementServiceHandler<Arc<InMemoryCommandBus>, Arc<InMemoryQueryBus>>>;

/// The engagement service as hosted by [`service_runtime`].
pub struct EngagementService {
    app: App,
}

#[async_trait]
impl Service for EngagementService {
    const NAME: &'static str = "engagement";
    const VERSION: &'static str = env!("CARGO_PKG_VERSION");
    const GRPC_SERVICE_NAME: &'static str =
        <EngagementServer as tonic::server::NamedService>::NAME;

    /// The RPCs exposed on the client edge listener (`GRPC_EDGE_ADDR`); anything
    /// else on this service is mesh-only. See `transport::grpc::edge`.
    // RecordView/RecordShare carry no actor by design (fire-and-forget counters);
    // requiring a token still keeps them off the anonymous internet.
    const EDGE_POLICY: EdgePolicy = &[
        authenticated("/engagement.v1.EngagementService/UpsertReaction"),
        authenticated("/engagement.v1.EngagementService/RemoveReaction"),
        public_read("/engagement.v1.EngagementService/RecordView"),
        public_read("/engagement.v1.EngagementService/RecordShare"),
        public_read("/engagement.v1.EngagementService/GetPostEngagement"),
    ];

    async fn build(_infra: Arc<InfraRegistry>) -> anyhow::Result<Self> {
        let backends = Backends {
            scylla: ScyllaConfig::from_env(),
            redis:  RedisConfig::from_env(),
            kafka:  Some(KafkaClientConfig::from_env()),
        };

        let weights = Arc::new(
            ReactionWeightsConfig::from_env()
                .map_err(|e| anyhow::anyhow!("engagement reaction weights: {e}"))?,
        );

        let producer = KafkaProducerBuilder::new(ProducerConfig::new(KafkaClientConfig::from_env()))
            .build()?;
        let publisher = Arc::new(KafkaEngagementEventPublisher::new(producer));

        let app = App::build(backends, weights, publisher)
            .await
            .map_err(|e| anyhow::anyhow!("engagement app build: {e}"))?;

        Ok(Self { app })
    }

    fn health_probes(&self) -> Vec<Arc<dyn HealthProbe>> {
        vec![redis_storage::health::probe(self.app.redis.clone())]
    }

    fn register(self, routes: &mut RoutesBuilder) -> anyhow::Result<()> {
        let handler = EngagementServiceHandler::new(
            Arc::clone(&self.app.command_bus),
            Arc::clone(&self.app.query_bus),
        );
        let reflection = ReflectionBuilder::configure()
            .register_encoded_file_descriptor_set(FILE_DESCRIPTOR_SET)
            .build_v1()?;

        routes.add_service(reflection);
        routes.add_service(EngagementServiceServer::new(handler));
        Ok(())
    }
}
