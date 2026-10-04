use std::net::SocketAddr;
use std::sync::Arc;

use cqrs::query::InMemoryQueryBus;
use tonic::transport::Server;
use tonic_health::server::health_reporter;
use tonic_reflection::server::Builder as ReflectionBuilder;

use redis_storage::RedisConfig;
use scylla_storage::ScyllaConfig;
use transport::kafka::config::client::KafkaClientConfig;

use crate::app::{App, Backends};
use crate::config::GeoDiscoveryConfig;
use crate::infrastructure::grpc::handler::{GeoDiscoveryHandler, GeoDiscoveryServiceServer};

/// Proto file descriptor set embedded at build time for server reflection.
pub const FILE_DESCRIPTOR_SET: &[u8] =
    geo_discovery_api::FILE_DESCRIPTOR_SET;

/// The gRPC handler type the server serves: the query bus is shared by `Arc`
/// (the same instance the composition root retains).
type ServingHandler = GeoDiscoveryHandler<Arc<InMemoryQueryBus>>;

/// Bootstraps and runs the geo-discovery gRPC server.
///
/// Reads configuration from the environment, builds the full service graph via
/// the shared composition root ([`App::build`]) — which also spawns the
/// indexer/score/tier/pruner workers — then binds the socket and serves until
/// shutdown.
pub async fn serve(addr: SocketAddr) -> Result<(), Box<dyn std::error::Error>> {
    let cfg = GeoDiscoveryConfig::from_env();

    let backends = Backends {
        scylla: ScyllaConfig::from_env(),
        redis:  RedisConfig::from_env(),
        kafka:  Some(KafkaClientConfig::from_env()),
        audience: crate::service::audience_gate_from_env().map_err(|e| e.to_string())?,
        geo_ip:   crate::service::geo_ip_from_env(),
    };

    let app = App::build(cfg, backends).await?;

    // ── gRPC server ───────────────────────────────────────────────────────────

    let (health_reporter, health_service) = health_reporter();
    health_reporter
        .set_serving::<GeoDiscoveryServiceServer<ServingHandler>>()
        .await;

    let reflection = ReflectionBuilder::configure()
        .register_encoded_file_descriptor_set(FILE_DESCRIPTOR_SET)
        .build_v1()?;

    let svc = GeoDiscoveryServiceServer::new(GeoDiscoveryHandler::new(
        Arc::clone(&app.query_bus),
        Arc::clone(&app.country_access),
        app.trusted_proxy_hops,
    ));

    tracing::info!(addr = %addr, "geo-discovery gRPC server listening");

    Server::builder()
        .add_service(health_service)
        .add_service(reflection)
        .add_service(svc)
        .serve(addr)
        .await?;

    Ok(())
}
