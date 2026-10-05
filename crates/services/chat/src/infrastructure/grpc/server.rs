use std::net::SocketAddr;

use cqrs::command::InMemoryCommandBus;
use cqrs::query::InMemoryQueryBus;
use redis_storage::RedisConfig;
use scylla_storage::ScyllaConfig;
use tonic::transport::Server;
use tonic_health::server::health_reporter;
use tonic_reflection::server::Builder as ReflectionBuilder;
use transport::kafka::config::client::KafkaClientConfig;

use crate::app::{App, AppConfig, Backends};
use crate::config::ChatConfig;
use crate::infrastructure::grpc::handler::{ChatServiceHandler, ChatServiceServer};

pub const FILE_DESCRIPTOR_SET: &[u8] = chat_api::FILE_DESCRIPTOR_SET;

/// Bootstraps and runs the chat gRPC server.
///
/// Reads configuration from the environment, builds the full service graph via
/// the shared composition root ([`App::build`]) — which also starts the per-pod
/// plane subscriber, registry reapers, and visibility worker — then binds the
/// socket and serves until shutdown.
pub async fn serve(addr: SocketAddr) -> Result<(), Box<dyn std::error::Error>> {
    let config = ChatConfig::from_env();

    let app_config = AppConfig {
        max_page_size:               config.max_page_size,
        hot_tail_cache_size:         config.hot_tail_cache_size,
        message_bucket_hours:        config.message_bucket_hours,
        member_stream_buffer_size:   config.member_stream_buffer_size,
        audience_stream_buffer_size: config.audience_stream_buffer_size,
        audience_shard_count:        config.audience_shard_count,
        presence_ttl_secs:           config.presence_ttl_secs,
        typing_ttl_secs:             config.typing_ttl_secs,
        // Production reuses the presence TTL for the Audience Plane.
        audience_ttl_secs:           config.presence_ttl_secs,
        visibility_consumer_group:   "chat-visibility-consumer".to_owned(),
        presence_settings_consumer_group: "chat-presence-settings".to_owned(),
        inbox_consumer_group:        "chat-inbox".to_owned(),
    };

    let backends = Backends {
        scylla:           ScyllaConfig::from_env(),
        redis:            RedisConfig::from_env(),
        kafka:            Some(KafkaClientConfig::from_env()),
        interaction_gate: crate::service::interaction_gate_from_env().map_err(|e| e.to_string())?,
    };

    let app = App::build(&app_config, backends).await?;

    // ── Serve ─────────────────────────────────────────────────────────────────

    let (health_reporter, health_service) = health_reporter();
    health_reporter
        .set_serving::<ChatServiceServer<ChatServiceHandler<InMemoryCommandBus, InMemoryQueryBus>>>()
        .await;

    let reflection = ReflectionBuilder::configure()
        .register_encoded_file_descriptor_set(FILE_DESCRIPTOR_SET)
        .build_v1()?;

    let svc = ChatServiceServer::new(app.handler);

    tracing::info!(addr = %addr, "chat gRPC server listening");

    Server::builder()
        .add_service(health_service)
        .add_service(reflection)
        .add_service(svc)
        .serve(addr)
        .await?;

    Ok(())
}
