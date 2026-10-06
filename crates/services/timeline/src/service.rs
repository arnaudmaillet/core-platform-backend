//! Adapts the timeline composition root to the fleet [`service_runtime::Service`]
//! contract.
//!
//! Timeline depends on social-graph over gRPC: the concrete
//! [`SocialGraphGrpcClient`] is built here from the configured endpoint (lazily
//! connected, so boot doesn't block on the dependency) and injected into the
//! otherwise client-generic [`App::build`]. The channel is wrapped in the shared
//! resilience stack (timeout + circuit breaker) resolved from the `social-graph`
//! binding in `infrastructure.toml` via [`InfraRegistry::resilience`], so it
//! hot-reloads with the fleet config. The gRPC surface is query-only; ingestion
//! runs via Kafka workers spawned inside `App::build`.

use std::sync::Arc;

use async_trait::async_trait;
use cqrs::query::InMemoryQueryBus;
use redis_storage::RedisConfig;
use scylla_storage::ScyllaConfig;
use service_runtime::{HealthProbe, InfraRegistry, Service};
use service_runtime::edge::{authenticated, public_read};
use service_runtime::EdgePolicy;
use tonic::service::RoutesBuilder;
use tonic_reflection::server::Builder as ReflectionBuilder;
use transport::grpc::client::{GrpcClientBuilder, GrpcClientConfig};
use transport::kafka::config::KafkaClientConfig;

use crate::app::{App, AppConfig, Backends};
use crate::config::TimelineConfig;
use crate::infrastructure::client::{GrpcNearbyPosts, SocialGraphGrpcClient};
use crate::infrastructure::grpc::handler::{TimelineServiceHandler, TimelineServiceServer};
use crate::infrastructure::grpc::server::FILE_DESCRIPTOR_SET;

type TimelineServer = TimelineServiceServer<TimelineServiceHandler<Arc<InMemoryQueryBus>>>;

/// Logical dependency name for the outbound social-graph channel — the key its
/// resilience profile is bound to under `[resilience.bindings]` in `infrastructure.toml`
/// (falls back to the default profile when unbound).
const SOCIAL_GRAPH_DEPENDENCY: &str = "social-graph";

/// Logical dependency name for the outbound geo-discovery channel (NEARBY).
const GEO_DISCOVERY_DEPENDENCY: &str = "geo-discovery";

/// The timeline service as hosted by [`service_runtime`].
pub struct TimelineService {
    app: App,
}

#[async_trait]
impl Service for TimelineService {
    const NAME: &'static str = "timeline";
    const VERSION: &'static str = env!("CARGO_PKG_VERSION");
    const GRPC_SERVICE_NAME: &'static str = <TimelineServer as tonic::server::NamedService>::NAME;

    /// The RPCs exposed on the client edge listener (`GRPC_EDGE_ADDR`); anything
    /// else on this service is mesh-only. See `transport::grpc::edge`.
    const EDGE_POLICY: EdgePolicy = &[
        authenticated("/timeline.v1.TimelineService/GetFollowingFeed"),
        // Not viewer-aware yet (lists posts/authors unfiltered): members only until
        // it filters on the audience (CheckAccess). Guest access: later.
        authenticated("/timeline.v1.TimelineService/GetAudioFeed"),
        // Viewer-aware (audience via CheckAccess, fail closed): members and
        // guests; a guest always gets the RESTRICTED content level.
        public_read("/timeline.v1.TimelineService/GetDiscoveryFeed"),
        // Interest tags (#662): owner only (`require_profile`).
        authenticated("/timeline.v1.TimelineService/ListInterests"),
        authenticated("/timeline.v1.TimelineService/RemoveInterest"),
        authenticated("/timeline.v1.TimelineService/ResetInterests"),
    ];

    async fn build(infra: Arc<InfraRegistry>) -> anyhow::Result<Self> {
        let cfg = TimelineConfig::from_env();

        let app_config = AppConfig {
            feed_cap:               cfg.feed_cap,
            audio_feed_cap:         cfg.audio_feed_cap,
            vip_registry_cap:       cfg.vip_registry_cap,
            backfill_limit:         cfg.backfill_limit,
            warm_ttl_secs:          cfg.warm_ttl_secs,
            tier_cache_ttl_secs:    cfg.tier_cache_ttl_secs,
            vip_registry_ttl_secs:  cfg.vip_registry_ttl_secs,
            max_page_size:          cfg.max_page_size,
            max_vip_merge_sources:  cfg.max_vip_merge_sources,
            warm_max_concurrency:   cfg.warm_max_concurrency,
            social_graph_page_size: cfg.social_graph_page_size,
            kafka_group_post_published: cfg.kafka_group_post_published.clone(),
            kafka_group_post_deleted:   cfg.kafka_group_post_deleted.clone(),
            kafka_group_sg_followed:    cfg.kafka_group_sg_followed.clone(),
            kafka_group_sg_unfollowed:  cfg.kafka_group_sg_unfollowed.clone(),
            discovery_window_secs:      cfg.discovery_window_secs,
            discovery_pool_cap:         cfg.discovery_pool_cap,
            discovery_hot_gravity_secs: cfg.discovery_hot_gravity_secs,
            nearby_candidates:          cfg.nearby_candidates,
            kafka_group_discovery:      cfg.kafka_group_discovery.clone(),
        };

        let backends = Backends {
            scylla: ScyllaConfig::from_env(),
            redis:  RedisConfig::from_env(),
            kafka:  Some(KafkaClientConfig::from_env()),
        };

        // Lazily-connected, resilience-wrapped channel: timeline boots even if
        // social-graph is not yet reachable (the connection opens on first RPC),
        // and the timeout + circuit-breaker stack — resolved from the
        // `social-graph` binding and shared across the fleet — wraps every call.
        let channel = GrpcClientBuilder::new(
            GrpcClientConfig::new(cfg.social_graph_endpoint.clone())
                .with_dependency(SOCIAL_GRAPH_DEPENDENCY),
        )
        .build_from_registry_lazy(&infra.resilience())
        .map_err(|e| anyhow::anyhow!("build social-graph client: {e}"))?;
        let social_graph = Arc::new(SocialGraphGrpcClient::new(channel));

        let geo_channel = GrpcClientBuilder::new(
            GrpcClientConfig::new(cfg.geo_discovery_endpoint.clone())
                .with_dependency(GEO_DISCOVERY_DEPENDENCY),
        )
        .build_from_registry_lazy(&infra.resilience())
        .map_err(|e| anyhow::anyhow!("build geo-discovery client: {e}"))?;
        let nearby = Arc::new(GrpcNearbyPosts::new(
            geo_channel,
            cfg.nearby_radius_km,
            cfg.nearby_close_radius_km,
        ));

        let app = App::build(&app_config, backends, social_graph, nearby)
            .await
            .map_err(|e| anyhow::anyhow!("timeline app build: {e}"))?;

        Ok(Self { app })
    }

    fn health_probes(&self) -> Vec<Arc<dyn HealthProbe>> {
        vec![
            scylla_storage::health::probe(Arc::clone(&self.app.scylla)),
            redis_storage::health::probe(self.app.redis.clone()),
        ]
    }

    fn register(self, routes: &mut RoutesBuilder) -> anyhow::Result<()> {
        let handler = TimelineServiceHandler::new(Arc::clone(&self.app.query_bus))
            .with_command_bus(Arc::clone(&self.app.command_bus));
        let reflection = ReflectionBuilder::configure()
            .register_encoded_file_descriptor_set(FILE_DESCRIPTOR_SET)
            .build_v1()?;

        routes.add_service(reflection);
        routes.add_service(TimelineServiceServer::new(handler));
        Ok(())
    }
}
