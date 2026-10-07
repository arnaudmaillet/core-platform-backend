use std::net::SocketAddr;
use std::sync::Arc;

use cqrs::command::InMemoryCommandBus;
use cqrs::query::InMemoryQueryBus;
use redis_storage::RedisConfig;
use scylla_storage::ScyllaConfig;
use tonic::transport::Server;
use tonic_health::server::health_reporter;
use tonic_reflection::server::Builder as ReflectionBuilder;
use transport::kafka::config::client::KafkaClientConfig;

use crate::app::{App, Backends};
use crate::config::NotificationConfig;
use crate::infrastructure::grpc::handler::notification_handler::{
    NotificationServiceHandler, NotificationServiceServer,
};
use crate::infrastructure::streaming::BroadcastRegistry;

pub const FILE_DESCRIPTOR_SET: &[u8] =
    notification_api::FILE_DESCRIPTOR_SET;

/// The gRPC handler type the server serves: the buses are shared by `Arc` (the
/// same instances the composition root retains and the workers use).
type ServingHandler =
    NotificationServiceHandler<Arc<InMemoryCommandBus>, Arc<InMemoryQueryBus>, BroadcastRegistry>;

/// Bootstraps and runs the notification gRPC server.
///
/// Reads configuration from the environment, builds the full service graph via
/// the shared composition root ([`App::build`]) — which also spawns the Kafka
/// workers and the broadcast-registry reaper — then binds the socket and serves
/// until shutdown.
pub async fn serve(addr: SocketAddr) -> Result<(), Box<dyn std::error::Error>> {
    let config = Arc::new(NotificationConfig::from_env());

    let backends = Backends {
        scylla: ScyllaConfig::from_env(),
        redis:  RedisConfig::from_env(),
        kafka:  Some(KafkaClientConfig::from_env()),
        push:   crate::infrastructure::push::from_env(),
    };

    let app = App::build(Arc::clone(&config), backends).await?;

    // ── gRPC server ───────────────────────────────────────────────────────────

    let (health_reporter, health_service) = health_reporter();
    health_reporter
        .set_serving::<NotificationServiceServer<ServingHandler>>()
        .await;

    let reflection = ReflectionBuilder::configure()
        .register_encoded_file_descriptor_set(FILE_DESCRIPTOR_SET)
        .build_v1()?;

    let svc = NotificationServiceServer::new(NotificationServiceHandler::new(
        Arc::clone(&app.command_bus),
        Arc::clone(&app.query_bus),
        Arc::clone(&app.stream_registry),
    ));

    tracing::info!(addr = %addr, "notification gRPC server listening");

    Server::builder()
        .add_service(health_service)
        .add_service(reflection)
        .add_service(svc)
        .serve(addr)
        .await?;

    Ok(())
}
