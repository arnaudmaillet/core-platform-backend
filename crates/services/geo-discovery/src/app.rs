//! The geo-discovery service's composition root.
//!
//! [`App::build`] is *pure composition*: storage configs and the service config
//! in, a fully-wired CQRS graph out. It binds no socket and reads no environment,
//! so the production entrypoint ([`crate::infrastructure::grpc::server::serve`])
//! and the live integration harness assemble the exact same graph.
//!
//! In production, indexing is driven by the Kafka workers (which invoke the
//! [`IndexPostHandler`] et al. directly). The composition root *also* registers
//! those handlers on a command bus so the integration harness can drive indexing
//! deterministically without a broker; the workers — and the broker — are derived
//! from [`Backends::kafka`].

use std::sync::Arc;

use cqrs::command::{CommandBusBuilder, InMemoryCommandBus};
use cqrs::query::{InMemoryQueryBus, QueryBusBuilder};
use redis_storage::{RedisClient, RedisClientBuilder, RedisConfig};
use scylla_storage::{ScyllaClient, ScyllaConfig, ScyllaSessionBuilder};
use transport::kafka::config::client::KafkaClientConfig;

use crate::application::command::{
    ApplyMapVisibilityCommand, ApplyMapVisibilityHandler, IndexPostCommand, IndexPostHandler,
    UpdateViralityWithTilesCommand, UpdateViralityWithTilesHandler,
};
use crate::application::port::AudienceGate;
use crate::application::query::get_geo_timeline::{GetGeoTimelineHandler, GetGeoTimelineQuery};
use crate::application::query::query_tile::{QueryTileHandler, QueryTileQuery};
use crate::config::GeoDiscoveryConfig;
use crate::infrastructure::cache::{RedisCardStore, RedisGeoSpatialIndex, RedisPinStore};
use crate::infrastructure::persistence::ScyllaTileRepository;
use crate::infrastructure::worker::{
    PostIndexerWorker, ScoreUpdaterWorker, TilePrunerWorker, VisibilityWorker,
};

/// Storage/transport endpoints the graph is wired against.
///
/// `kafka` is optional: `Some` spawns the indexer/score/pruner workers;
/// `None` leaves the index command handlers driveable directly via the command
/// bus.
pub struct Backends {
    pub scylla: ScyllaConfig,
    pub redis:  RedisConfig,
    pub kafka:  Option<KafkaClientConfig>,
    /// The audience check (social-graph `CheckAccess`) the read paths apply to
    /// clients. Injected so the harness can script it.
    pub audience: Arc<dyn AudienceGate>,
}

/// A fully-wired geo-discovery service bound to its backends. The buses exposed
/// here are the *same* instances the handlers are registered into; the
/// `QueryTile` query reads the spatial index, card cache, and tile repository, so
/// the query bus proves the end-to-end index→query round-trip.
pub struct App {
    pub command_bus: Arc<InMemoryCommandBus>,
    pub query_bus:   Arc<InMemoryQueryBus>,
    /// Live storage clients, retained so the runtime's readiness loop can probe
    /// their liveness (see [`crate::service`]).
    pub scylla:      Arc<ScyllaClient>,
    pub redis:       RedisClient,
}

impl App {
    /// Builds storage clients from `backends`, assembles the spatial index, card
    /// store, and tile repository, registers the index commands and the tile
    /// query, and — when Kafka is configured — spawns the background workers.
    pub async fn build(
        cfg:      GeoDiscoveryConfig,
        backends: Backends,
    ) -> Result<Self, Box<dyn std::error::Error>> {
        let Backends { scylla, redis, kafka, audience } = backends;

        let scylla_client = Arc::new(ScyllaSessionBuilder::new(scylla).build().await?);
        let redis_client = RedisClientBuilder::new(redis).build().await?;

        let spatial_index = Arc::new(RedisGeoSpatialIndex::new(redis_client.clone()));
        let card_store = Arc::new(RedisCardStore::new(redis_client.clone()));
        let pin_store = Arc::new(RedisPinStore::new(redis_client.clone()));
        let tile_repository = Arc::new(ScyllaTileRepository::new(Arc::clone(&scylla_client)));

        let command_bus = Arc::new(
            CommandBusBuilder::new()
                .register::<IndexPostCommand, _>(IndexPostHandler {
                    spatial_index:        Arc::clone(&spatial_index),
                    card_store:           Arc::clone(&card_store),
                    tile_repository:      Arc::clone(&tile_repository),
                    pin_store:            Arc::clone(&pin_store),
                    card_cache_threshold: cfg.card_cache_threshold,
                })?
                .register::<UpdateViralityWithTilesCommand, _>(UpdateViralityWithTilesHandler {
                    spatial_index:   Arc::clone(&spatial_index),
                    tile_repository: Arc::clone(&tile_repository),
                })?
                .register::<ApplyMapVisibilityCommand, _>(ApplyMapVisibilityHandler {
                    card_store:      Arc::clone(&card_store),
                    tile_repository: Arc::clone(&tile_repository),
                    pin_store:       Arc::clone(&pin_store),
                })?
                .build(),
        );

        let query_bus = Arc::new(
            QueryBusBuilder::new()
                // Radar (pan): Redis-only, returns lightweight pins.
                .register::<QueryTileQuery, _>(QueryTileHandler {
                    spatial_index: Arc::clone(&spatial_index),
                    pin_store:     Arc::clone(&pin_store),
                    audience:      Arc::clone(&audience),
                })?
                // Focus (tap): hydrates full cards, Redis + ScyllaDB fallback.
                .register::<GetGeoTimelineQuery, _>(GetGeoTimelineHandler {
                    card_store:      Arc::clone(&card_store),
                    tile_repository: Arc::clone(&tile_repository),
                    audience,
                })?
                .build(),
        );

        // ── Background workers (Kafka path) ──────────────────────────────────
        if let Some(kafka_config) = kafka {
            tokio::spawn(
                PostIndexerWorker::new(
                    kafka_config.clone(),
                    Arc::clone(&spatial_index),
                    Arc::clone(&card_store),
                    Arc::clone(&tile_repository),
                    Arc::clone(&pin_store),
                    cfg.post_indexer_group_id.clone(),
                    cfg.card_cache_threshold,
                )
                .run(),
            );
            tokio::spawn(
                ScoreUpdaterWorker::new(
                    kafka_config.clone(),
                    Arc::clone(&spatial_index),
                    Arc::clone(&tile_repository),
                    cfg.score_updater_group_id.clone(),
                )
                .run(),
            );
            tokio::spawn(
                VisibilityWorker::new(
                    kafka_config.clone(),
                    Arc::clone(&card_store),
                    Arc::clone(&tile_repository),
                    Arc::clone(&pin_store),
                    cfg.visibility_group_id.clone(),
                )
                .run(),
            );
            tokio::spawn(
                TilePrunerWorker::new(
                    redis_client.clone(),
                    cfg.tile_pruner_interval,
                    cfg.tile_cold_threshold,
                    500,
                )
                .run(),
            );
        }

        Ok(Self { command_bus, query_bus, scylla: scylla_client, redis: redis_client })
    }
}
