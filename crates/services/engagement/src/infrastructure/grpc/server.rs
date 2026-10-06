use std::net::SocketAddr;
use std::sync::Arc;

use cqrs::command::InMemoryCommandBus;
use cqrs::query::InMemoryQueryBus;
use tonic::transport::Server;
use tonic_health::server::health_reporter;
use tonic_reflection::server::Builder as ReflectionBuilder;

use redis_storage::RedisConfig;
use scylla_storage::ScyllaConfig;
use transport::kafka::config::client::KafkaClientConfig;
use transport::kafka::config::producer::ProducerConfig;
use transport::kafka::producer::builder::KafkaProducerBuilder;

use crate::app::{App, Backends};
use crate::config::ReactionWeightsConfig;
use crate::infrastructure::grpc::handler::engagement_handler::{
    EngagementServiceHandler, EngagementServiceServer,
};
use crate::infrastructure::publisher::KafkaEngagementEventPublisher;

/// Proto file descriptor blob embedded at build time for server reflection.
pub const FILE_DESCRIPTOR_SET: &[u8] =
    engagement_api::FILE_DESCRIPTOR_SET;

/// The gRPC handler type the server serves: the buses are shared by `Arc` (the
/// same instances the composition root retains).
type ServingHandler =
    EngagementServiceHandler<Arc<InMemoryCommandBus>, Arc<InMemoryQueryBus>>;

/// Bootstraps and runs the engagement gRPC server.
///
/// Reads configuration from the environment, builds the full service graph via
/// the shared composition root ([`App::build`]) — which also spawns the
/// write-behind workers — then binds the socket and serves until shutdown.
pub async fn serve(addr: SocketAddr) -> Result<(), Box<dyn std::error::Error>> {
    let weights = Arc::new(ReactionWeightsConfig::from_env()?);

    // The Kafka producer backs the durable write-behind publisher; the broker
    // config is also threaded into the workers via `Backends::kafka`.
    let kafka_client = KafkaClientConfig::from_env();
    let producer = KafkaProducerBuilder::new(ProducerConfig::new(kafka_client.clone())).build()?;
    let publisher = Arc::new(KafkaEngagementEventPublisher::new(producer));

    let backends = Backends {
        scylla: ScyllaConfig::from_env(),
        redis:  RedisConfig::from_env(),
        kafka:  Some(kafka_client),
    };

    let app = App::build(backends, weights, publisher, crate::service::like_visibility_from_env()?).await?;

    // ── gRPC server ───────────────────────────────────────────────────────────

    let (health_reporter, health_service) = health_reporter();
    health_reporter
        .set_serving::<EngagementServiceServer<ServingHandler>>()
        .await;

    let reflection = ReflectionBuilder::configure()
        .register_encoded_file_descriptor_set(FILE_DESCRIPTOR_SET)
        .build_v1()?;

    let svc = EngagementServiceServer::new(EngagementServiceHandler::new(
        Arc::clone(&app.command_bus),
        Arc::clone(&app.query_bus),
    ));

    tracing::info!(addr = %addr, "engagement gRPC server listening");

    Server::builder()
        .add_service(health_service)
        .add_service(reflection)
        .add_service(svc)
        .serve(addr)
        .await?;

    Ok(())
}
