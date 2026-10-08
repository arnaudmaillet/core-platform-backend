//! Adapts the engagement composition root to the fleet
//! [`service_runtime::Service`] contract.
//!
//! Engagement is Redis-primary (the always-on hot path) with ScyllaDB durable
//! copies written by Kafka workers spawned inside [`App::build`].
//! Readiness therefore gates on Redis only.

use std::sync::Arc;

use async_trait::async_trait;
use cqrs::command::InMemoryCommandBus;
use cqrs::query::InMemoryQueryBus;
use redis_storage::RedisConfig;
use scylla_storage::ScyllaConfig;
use service_runtime::{HealthProbe, InfraRegistry, Service};
use service_runtime::edge::public_read;
use service_runtime::EdgePolicy;
use tonic::service::RoutesBuilder;
use tonic_reflection::server::Builder as ReflectionBuilder;
use transport::kafka::config::KafkaClientConfig;

use crate::app::{App, Backends};
use crate::infrastructure::grpc::handler::engagement_handler::EngagementServiceServer;
use crate::infrastructure::grpc::handler::EngagementServiceHandler;
use crate::infrastructure::grpc::server::FILE_DESCRIPTOR_SET;

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
        public_read("/engagement.v1.EngagementService/RecordView"),
        public_read("/engagement.v1.EngagementService/RecordShare"),
        public_read("/engagement.v1.EngagementService/GetPostEngagement"),
        // Likes (#665): viewer-aware (hidden counts, the reader's own).
        public_read("/engagement.v1.EngagementService/BatchGetLikes"),
    ];

    async fn build(_infra: Arc<InfraRegistry>) -> anyhow::Result<Self> {
        let backends = Backends {
            scylla: ScyllaConfig::from_env(),
            redis:  RedisConfig::from_env(),
            kafka:  Some(KafkaClientConfig::from_env()),
        };

        let app = App::build(backends, like_visibility_from_env()?)
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

/// Like counts withheld per their author's setting (#809), asked of post at
/// `ENGAGEMENT_POST_GRPC_ENDPOINT` (lazily connected; request / connect
/// deadlines `ENGAGEMENT_POST_RPC_TIMEOUT_MS` / `ENGAGEMENT_POST_CONNECT_TIMEOUT_MS`,
/// 500 ms / 1 s). Unset: nothing is withheld (until the mesh route exists).
pub(crate) fn like_visibility_from_env() -> anyhow::Result<Option<Arc<dyn crate::application::port::LikeVisibility>>> {
    let Some(endpoint) = std::env::var("ENGAGEMENT_POST_GRPC_ENDPOINT").ok().filter(|v| !v.trim().is_empty()) else {
        tracing::warn!("ENGAGEMENT_POST_GRPC_ENDPOINT unset: hidden like counts are not withheld");
        return Ok(None);
    };
    let ms = |key: &str, default: u64| {
        std::time::Duration::from_millis(std::env::var(key).ok().and_then(|v| v.parse().ok()).unwrap_or(default))
    };
    let channel = tonic::transport::Channel::from_shared(endpoint)
        .map_err(|e| anyhow::anyhow!("invalid ENGAGEMENT_POST_GRPC_ENDPOINT: {e}"))?
        .timeout(ms("ENGAGEMENT_POST_RPC_TIMEOUT_MS", 500))
        .connect_timeout(ms("ENGAGEMENT_POST_CONNECT_TIMEOUT_MS", 1_000))
        .connect_lazy();
    Ok(Some(Arc::new(crate::infrastructure::client::GrpcLikeVisibility::new(channel))))
}

#[cfg(test)]
mod tests {
    use super::*;

    /// An account's likes are the GDPR export's (#653, #665), its positions
    /// the settlement's: never on the edge.
    #[test]
    fn listing_by_account_is_mesh_only() {
        for method in [
            "/engagement.v1.EngagementService/ListLikesByAccount",
            "/engagement.v1.EngagementService/GetLikePositions",
        ] {
            assert!(EngagementService::EDGE_POLICY.iter().all(|rule| rule.method != method), "{method}");
        }
    }
}
