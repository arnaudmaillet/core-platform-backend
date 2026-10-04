//! Adapts the notification composition root to the fleet
//! [`service_runtime::Service`] contract.
//!
//! The handler is generic over its broadcast registry (for the streaming RPC) in
//! addition to the command/query buses; all three are the live instances `App`
//! exposes. Kafka workers are spawned inside [`App::build`].

use std::sync::Arc;

use async_trait::async_trait;
use cqrs::command::InMemoryCommandBus;
use cqrs::query::InMemoryQueryBus;
use redis_storage::RedisConfig;
use scylla_storage::ScyllaConfig;
use service_runtime::{HealthProbe, InfraRegistry, Service};
use service_runtime::edge::authenticated;
use service_runtime::EdgePolicy;
use tonic::service::RoutesBuilder;
use tonic_reflection::server::Builder as ReflectionBuilder;
use transport::kafka::config::KafkaClientConfig;

use crate::app::{App, Backends};
use crate::config::NotificationConfig;
use crate::infrastructure::grpc::handler::{NotificationServiceHandler, NotificationServiceServer};
use crate::infrastructure::grpc::server::FILE_DESCRIPTOR_SET;
use crate::infrastructure::streaming::BroadcastRegistry;

type NotificationServer = NotificationServiceServer<
    NotificationServiceHandler<Arc<InMemoryCommandBus>, Arc<InMemoryQueryBus>, BroadcastRegistry>,
>;

/// The notification service as hosted by [`service_runtime`].
pub struct NotificationService {
    app: App,
}

#[async_trait]
impl Service for NotificationService {
    const NAME: &'static str = "notification";
    const VERSION: &'static str = env!("CARGO_PKG_VERSION");
    const GRPC_SERVICE_NAME: &'static str =
        <NotificationServer as tonic::server::NamedService>::NAME;

    /// The RPCs exposed on the client edge listener (`GRPC_EDGE_ADDR`); anything
    /// else on this service is mesh-only. See `transport::grpc::edge`.
    const EDGE_POLICY: EdgePolicy = &[
        authenticated("/notification.v1.NotificationService/ListNotifications"),
        authenticated("/notification.v1.NotificationService/GetUnreadCount"),
        authenticated("/notification.v1.NotificationService/MarkRead"),
        authenticated("/notification.v1.NotificationService/MarkAllRead"),
        authenticated("/notification.v1.NotificationService/StreamNotifications"),
        // Push devices and preferences (#654), bound to profile_id.
        authenticated("/notification.v1.NotificationService/RegisterDevice"),
        authenticated("/notification.v1.NotificationService/UnregisterDevice"),
        authenticated("/notification.v1.NotificationService/GetNotificationPreferences"),
        authenticated("/notification.v1.NotificationService/UpdateNotificationPreferences"),
    ];

    async fn build(_infra: Arc<InfraRegistry>) -> anyhow::Result<Self> {
        let config = Arc::new(NotificationConfig::from_env());
        let backends = Backends {
            scylla: ScyllaConfig::from_env(),
            redis:  RedisConfig::from_env(),
            kafka:  Some(KafkaClientConfig::from_env()),
        };

        let app = App::build(config, backends)
            .await
            .map_err(|e| anyhow::anyhow!("notification app build: {e}"))?;

        Ok(Self { app })
    }

    fn health_probes(&self) -> Vec<Arc<dyn HealthProbe>> {
        vec![
            scylla_storage::health::probe(Arc::clone(&self.app.scylla)),
            redis_storage::health::probe(self.app.redis.clone()),
        ]
    }

    fn register(self, routes: &mut RoutesBuilder) -> anyhow::Result<()> {
        let handler = NotificationServiceHandler::new(
            Arc::clone(&self.app.command_bus),
            Arc::clone(&self.app.query_bus),
            Arc::clone(&self.app.stream_registry),
        );
        let reflection = ReflectionBuilder::configure()
            .register_encoded_file_descriptor_set(FILE_DESCRIPTOR_SET)
            .build_v1()?;

        routes.add_service(reflection);
        routes.add_service(NotificationServiceServer::new(handler));
        Ok(())
    }
}
