//! Adapts the geo-discovery composition root to the fleet
//! [`service_runtime::Service`] contract.
//!
//! Geo-discovery's gRPC surface is query-only (writes arrive via Kafka workers
//! spawned inside [`App::build`]), so the handler is constructed from the query
//! bus alone.

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
use transport::kafka::config::KafkaClientConfig;

use crate::app::{App, Backends};
use crate::config::GeoDiscoveryConfig;
use crate::infrastructure::grpc::handler::{GeoDiscoveryHandler, GeoDiscoveryServiceServer};
use crate::infrastructure::grpc::server::FILE_DESCRIPTOR_SET;

type GeoServer = GeoDiscoveryServiceServer<GeoDiscoveryHandler<Arc<InMemoryQueryBus>>>;

/// The geo-discovery service as hosted by [`service_runtime`].
pub struct GeoDiscoveryService {
    app: App,
}

#[async_trait]
impl Service for GeoDiscoveryService {
    const NAME: &'static str = "geo-discovery";
    const VERSION: &'static str = env!("CARGO_PKG_VERSION");
    const GRPC_SERVICE_NAME: &'static str = <GeoServer as tonic::server::NamedService>::NAME;

    /// The RPCs exposed on the client edge listener (`GRPC_EDGE_ADDR`); anything
    /// else on this service is mesh-only. See `transport::grpc::edge`.
    const EDGE_POLICY: EdgePolicy = &[
        public_read("/geo_discovery.v1.GeoDiscoveryService/QueryTile"),
        public_read("/geo_discovery.v1.GeoDiscoveryService/GetGeoTimeline"),
        // Members and guests; the principal is the token's.
        public_read("/geo_discovery.v1.GeoDiscoveryService/GetCountryAccess"),
        // The country ladder: aggregates, the same for every reader (#665).
        public_read("/geo_discovery.v1.GeoDiscoveryService/GetCountryStandings"),
        // A member's countries and unlocks: the caller's account (#665).
        authenticated("/geo_discovery.v1.GeoDiscoveryService/GetCountryUnlocks"),
        authenticated("/geo_discovery.v1.GeoDiscoveryService/UnlockCountry"),
    ];

    async fn build(_infra: Arc<InfraRegistry>) -> anyhow::Result<Self> {
        let cfg = GeoDiscoveryConfig::from_env();
        let backends = Backends {
            scylla: ScyllaConfig::from_env(),
            redis:  RedisConfig::from_env(),
            kafka:  Some(KafkaClientConfig::from_env()),
            audience: audience_gate_from_env()?,
            geo_ip:   geo_ip_from_env(),
            wallet:    Arc::new(crate::infrastructure::client::GrpcGemWallet::new(mesh_channel("GEO_WALLET_GRPC_ENDPOINT", "http://localhost:50072")?)),
            residence: Arc::new(crate::infrastructure::client::GrpcResidenceDirectory::new(mesh_channel(
                "GEO_ACCOUNT_GRPC_ENDPOINT",
                "http://localhost:50059",
            )?)),
        };

        let app = App::build(cfg, backends)
            .await
            .map_err(|e| anyhow::anyhow!("geo-discovery app build: {e}"))?;

        Ok(Self { app })
    }

    fn health_probes(&self) -> Vec<Arc<dyn HealthProbe>> {
        vec![
            scylla_storage::health::probe(Arc::clone(&self.app.scylla)),
            redis_storage::health::probe(self.app.redis.clone()),
        ]
    }

    fn register(self, routes: &mut RoutesBuilder) -> anyhow::Result<()> {
        let handler = GeoDiscoveryHandler::new(
            Arc::clone(&self.app.query_bus),
            Arc::clone(&self.app.country_access),
            self.app.trusted_proxy_hops,
        )
        .with_standings(Arc::clone(&self.app.standings))
        .with_unlocking(Arc::clone(&self.app.unlocking));
        let reflection = ReflectionBuilder::configure()
            .register_encoded_file_descriptor_set(FILE_DESCRIPTOR_SET)
            .build_v1()?;

        routes.add_service(reflection);
        routes.add_service(GeoDiscoveryServiceServer::new(handler));
        Ok(())
    }
}

/// The map's audience check: social-graph `CheckAccess` over a lazily-connected
/// channel (a cold start needs social-graph down or up alike). Both deadlines
/// are mandatory (tonic has none); client reads fail closed without an answer.
pub(crate) fn audience_gate_from_env() -> anyhow::Result<Arc<dyn crate::application::port::AudienceGate>> {
    let ms = |key: &str, default: u64| {
        std::time::Duration::from_millis(
            std::env::var(key).ok().and_then(|v| v.parse().ok()).unwrap_or(default),
        )
    };
    let endpoint = std::env::var("GEO_SOCIAL_GRAPH_GRPC_ENDPOINT")
        .unwrap_or_else(|_| "http://localhost:50053".to_owned());
    let channel = tonic::transport::Channel::from_shared(endpoint)
        .map_err(|e| anyhow::anyhow!("invalid GEO_SOCIAL_GRAPH_GRPC_ENDPOINT: {e}"))?
        .timeout(ms("GEO_AUDIENCE_RPC_TIMEOUT_MS", 500))
        .connect_timeout(ms("GEO_AUDIENCE_CONNECT_TIMEOUT_MS", 500))
        .connect_lazy();
    Ok(Arc::new(crate::infrastructure::client::GrpcAudienceGate::new(channel)))
}

/// A lazily-connected mesh channel to `env_key` (default `default`), with the
/// unlock peers' deadlines (`GEO_UNLOCK_RPC_TIMEOUT_MS`, 1000; connect 500).
pub(crate) fn mesh_channel(env_key: &str, default: &str) -> anyhow::Result<tonic::transport::Channel> {
    let ms = |key: &str, default: u64| {
        std::time::Duration::from_millis(std::env::var(key).ok().and_then(|v| v.parse().ok()).unwrap_or(default))
    };
    let endpoint = std::env::var(env_key).ok().filter(|v| !v.trim().is_empty()).unwrap_or_else(|| default.to_owned());
    Ok(tonic::transport::Channel::from_shared(endpoint)
        .map_err(|e| anyhow::anyhow!("invalid {env_key}: {e}"))?
        .timeout(ms("GEO_UNLOCK_RPC_TIMEOUT_MS", 1000))
        .connect_timeout(ms("GEO_UNLOCK_CONNECT_TIMEOUT_MS", 500))
        .connect_lazy())
}

/// GeoIP for country access: the MaxMind DB file at `GEO_GEOIP_MMDB_PATH`
/// (none = nothing is granted), and what a private-network address resolves to
/// (`GEO_GEOIP_PRIVATE_NETWORK_COUNTRY`: an ISO code, or `*` = the device's
/// claim — local fleet only).
pub(crate) fn geo_ip_from_env() -> Arc<dyn crate::application::port::GeoIp> {
    use crate::infrastructure::geoip::{MmdbGeoIp, PrivateNetworkCountry};
    let path = std::env::var("GEO_GEOIP_MMDB_PATH").ok();
    let private = PrivateNetworkCountry::parse(std::env::var("GEO_GEOIP_PRIVATE_NETWORK_COUNTRY").ok().as_deref());
    Arc::new(MmdbGeoIp::load(path.as_deref(), private))
}
