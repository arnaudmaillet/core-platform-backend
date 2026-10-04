use std::net::SocketAddr;
use std::sync::Arc;

use cqrs::command::InMemoryCommandBus;
use cqrs::query::InMemoryQueryBus;
use scylla_storage::ScyllaConfig;
use tonic::transport::Server;
use tonic_health::server::health_reporter;
use tonic_reflection::server::Builder as ReflectionBuilder;
use transport::kafka::config::client::KafkaClientConfig;
use transport::kafka::config::producer::ProducerConfig;
use transport::kafka::producer::builder::KafkaProducerBuilder;

use crate::app::{App, Backends};
use crate::infrastructure::grpc::handler::comment_service_handler::{
    CommentServiceHandler, CommentServiceServer,
};
use crate::infrastructure::publisher::KafkaCommentEventPublisher;

pub const FILE_DESCRIPTOR_SET: &[u8] =
    comment_api::FILE_DESCRIPTOR_SET;

/// The gRPC handler type the server serves: the buses are shared by `Arc` (the
/// same instances the composition root retains).
type ServingHandler =
    CommentServiceHandler<Arc<InMemoryCommandBus>, Arc<InMemoryQueryBus>>;

/// Bootstraps and runs the comment gRPC server.
///
/// Builds the Kafka event publisher, then the full service graph via the shared
/// composition root ([`App::build`]), and serves until shutdown.
pub async fn serve(addr: SocketAddr) -> Result<(), Box<dyn std::error::Error>> {
    // The Kafka producer backs the outbound event publisher.
    let producer = KafkaProducerBuilder::new(ProducerConfig::new(KafkaClientConfig::from_env()))
        .build()?;
    let publisher = Arc::new(KafkaCommentEventPublisher::new(producer));

    let backends = Backends { scylla: ScyllaConfig::from_env() };
    let gate = crate::service::read_gate_from_env().map_err(|e| e.to_string())?;
    let app = App::build(backends, publisher, gate).await?;

    // ── gRPC server ───────────────────────────────────────────────────────────

    let (health_reporter, health_service) = health_reporter();
    health_reporter
        .set_serving::<CommentServiceServer<ServingHandler>>()
        .await;

    let reflection = ReflectionBuilder::configure()
        .register_encoded_file_descriptor_set(FILE_DESCRIPTOR_SET)
        .build_v1()?;

    let svc = CommentServiceServer::new(CommentServiceHandler::new(
        Arc::clone(&app.command_bus),
        Arc::clone(&app.query_bus),
    ));

    tracing::info!(addr = %addr, "comment gRPC server listening");

    Server::builder()
        .add_service(health_service)
        .add_service(reflection)
        .add_service(svc)
        .serve(addr)
        .await?;

    Ok(())
}
