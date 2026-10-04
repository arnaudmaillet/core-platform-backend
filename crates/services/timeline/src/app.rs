//! The timeline service's composition root.
//!
//! [`App::build`] is *pure composition*: storage configs and a
//! [`SocialGraphClient`] in, a fully-wired service graph out. It binds no socket
//! and reads no environment, so the production entrypoint
//! ([`crate::service::TimelineService`], hosted by the fleet runtime) and the live
//! integration harness drive the exact same assembly.
//!
//! Two deliberate seams make the read-heavy timeline testable:
//!
//! - **`SocialGraphClient` is a generic parameter**, not a hard-wired gRPC
//!   client. Production passes the real
//!   [`SocialGraphGrpcClient`](crate::infrastructure::client::SocialGraphGrpcClient);
//!   the harness passes an in-process fake that *is* the follow graph and counts
//!   calls — so fan-out and cold-start rebuilds are deterministic. The
//!   discovery feed's [`NearbyPosts`] (geo-discovery) is injected the same way.
//! - **The Kafka workers are derived from [`Backends::kafka`].** When it is
//!   `Some`, the five consumers are spawned; when `None`, the harness drives the
//!   same command handlers directly through [`App::command_bus`], so the
//!   concurrency/temporal scenarios need no broker.

use std::collections::HashSet;
use std::sync::{Arc, Mutex};

use cqrs::command::{CommandBusBuilder, InMemoryCommandBus};
use cqrs::query::{InMemoryQueryBus, QueryBusBuilder};
use redis_storage::{RedisClient, RedisClientBuilder, RedisConfig};
use scylla_storage::{ScyllaClient, ScyllaConfig, ScyllaSessionBuilder};
use tokio::sync::Semaphore;
use transport::kafka::config::client::KafkaClientConfig;

use crate::application::command::apply_discovery_signal::{
    ApplyDiscoverySignalCommand, ApplyDiscoverySignalHandler,
};
use crate::application::command::backfill_follow::{BackfillFollowCommand, BackfillFollowHandler};
use crate::application::command::ingest_audio_index::{
    IngestAudioIndexCommand, IngestAudioIndexHandler,
};
use crate::application::command::ingest_post_published::{
    IngestPostPublishedCommand, IngestPostPublishedHandler,
};
use crate::application::command::prune_follow::{PruneFollowCommand, PruneFollowHandler};
use crate::application::command::remove_post::{RemovePostCommand, RemovePostHandler};
use crate::application::port::{
    AuthorPostRepository, DiscoveryPool, FeedRepository, FeedStore, FollowingStore, NearbyPosts,
    SocialGraphClient, TierCache, VipRegistry,
};
use crate::application::query::get_audio_feed::{GetAudioFeedHandler, GetAudioFeedQuery};
use crate::application::query::get_discovery_feed::{GetDiscoveryFeedHandler, GetDiscoveryFeedQuery};
use crate::application::query::get_following_feed::{GetFollowingFeedHandler, GetFollowingFeedQuery};
use crate::infrastructure::cache::{
    RedisAudioFeedStore, RedisDiscoveryPool, RedisFeedStore, RedisFollowingStore, RedisTierCache, RedisVipRegistry,
};
use crate::infrastructure::persistence::{
    ScyllaAudioFeedRepository, ScyllaAuthorPostRepository, ScyllaFeedRepository,
};
use crate::infrastructure::worker::{
    discovery_worker::DiscoveryWorker, follow_created_worker::FollowCreatedWorker, follow_deleted_worker::FollowDeletedWorker,
    post_deleted_worker::PostDeletedWorker, post_published_worker::PostPublishedWorker,
};

/// Storage/transport endpoints the graph is wired against.
///
/// `kafka` is optional: `Some` spawns the five ingestion workers; `None` leaves
/// the command handlers driveable directly via [`App::command_bus`].
pub struct Backends {
    pub scylla: ScyllaConfig,
    pub redis:  RedisConfig,
    pub kafka:  Option<KafkaClientConfig>,
}

/// The tuning surface threaded through the graph. Production fills this from
/// [`TimelineConfig`](crate::config::TimelineConfig); scenarios shrink the caps
/// and TTLs to force eviction and warm-flag expiry in seconds.
#[derive(Debug, Clone)]
pub struct AppConfig {
    pub feed_cap:               u16,
    pub audio_feed_cap:         u16,
    pub vip_registry_cap:       u16,
    pub backfill_limit:         i32,
    pub warm_ttl_secs:          u64,
    pub tier_cache_ttl_secs:    u64,
    pub vip_registry_ttl_secs:  u64,
    pub max_page_size:          i32,
    pub max_vip_merge_sources:  usize,
    pub warm_max_concurrency:   usize,
    pub social_graph_page_size: i32,
    /// Kafka consumer-group ids for the ingestion workers (only used when
    /// [`Backends::kafka`] is `Some`).
    pub kafka_group_post_published: String,
    pub kafka_group_post_deleted:   String,
    pub kafka_group_sg_followed:     String,
    pub kafka_group_sg_unfollowed:   String,
    /// Discovery pool: window, cap, hot-score gravity; NEARBY candidate cap;
    /// the discovery worker's consumer group.
    pub discovery_window_secs:      u64,
    pub discovery_pool_cap:         usize,
    pub discovery_hot_gravity_secs: f64,
    pub nearby_candidates:          usize,
    pub kafka_group_discovery:      String,
}

/// A fully-wired timeline service bound to its backends, plus the shared `Arc`
/// handles a scenario asserts against. The buses, cache adapters, and
/// repositories exposed here are the *same* instances the handlers hold.
pub struct App {
    pub command_bus:      Arc<InMemoryCommandBus>,
    pub query_bus:        Arc<InMemoryQueryBus>,
    pub feed_store:       Arc<dyn FeedStore>,
    pub vip_registry:     Arc<dyn VipRegistry>,
    pub tier_cache:       Arc<dyn TierCache>,
    pub following_store:  Arc<dyn FollowingStore>,
    pub feed_repository:  Arc<dyn FeedRepository>,
    pub author_post_repo: Arc<dyn AuthorPostRepository>,
    // The audio ports use RPITIT (not `#[async_trait]`) and so are not
    // dyn-compatible; exposed as their concrete adapters.
    pub audio_feed_store: Arc<RedisAudioFeedStore>,
    pub audio_feed_repo:  Arc<ScyllaAudioFeedRepository>,
    pub discovery_pool:   Arc<dyn DiscoveryPool>,
    /// Live storage clients, retained so the runtime's readiness loop can probe
    /// their liveness (see [`crate::service`]).
    pub scylla:           Arc<ScyllaClient>,
    pub redis:            RedisClient,
}

impl App {
    /// Builds storage clients from `backends`, assembles the cache/persistence
    /// adapters, the CQRS buses, and — when Kafka is configured — spawns the five
    /// ingestion workers against the same `social_graph` and command bus.
    pub async fn build<SG: SocialGraphClient>(
        config:       &AppConfig,
        backends:     Backends,
        social_graph: Arc<SG>,
        nearby:       Arc<dyn NearbyPosts>,
    ) -> Result<Self, Box<dyn std::error::Error>> {
        let Backends { scylla, redis, kafka } = backends;

        // ── Storage clients ──────────────────────────────────────────────────
        let scylla_client = Arc::new(ScyllaSessionBuilder::new(scylla).build().await?);
        let redis_client = RedisClientBuilder::new(redis).build().await?;

        // ── Cache adapters ───────────────────────────────────────────────────
        let feed_store = Arc::new(RedisFeedStore::new(redis_client.clone()));
        let vip_registry = Arc::new(RedisVipRegistry::new(redis_client.clone()));
        let tier_cache = Arc::new(RedisTierCache::new(redis_client.clone()));
        let following_store = Arc::new(RedisFollowingStore::new(redis_client.clone()));
        let audio_feed_store = Arc::new(RedisAudioFeedStore::new(redis_client.clone()));
        let discovery_pool: Arc<dyn DiscoveryPool> = Arc::new(RedisDiscoveryPool::new(
            redis_client.clone(),
            config.discovery_window_secs,
            config.discovery_pool_cap,
            config.discovery_hot_gravity_secs,
        ));

        // ── Persistence adapters ─────────────────────────────────────────────
        let feed_repository = Arc::new(ScyllaFeedRepository::new(Arc::clone(&scylla_client)));
        let author_post_repo = Arc::new(ScyllaAuthorPostRepository::new(Arc::clone(&scylla_client)));
        let audio_feed_repo = Arc::new(ScyllaAudioFeedRepository::new(Arc::clone(&scylla_client)));

        // ── Command bus ──────────────────────────────────────────────────────
        let command_bus = Arc::new(
            CommandBusBuilder::new()
                .register::<IngestPostPublishedCommand, _>(IngestPostPublishedHandler {
                    feed_store:             Arc::clone(&feed_store),
                    vip_registry:           Arc::clone(&vip_registry),
                    feed_repository:        Arc::clone(&feed_repository),
                    author_post_repo:       Arc::clone(&author_post_repo),
                    tier_cache:             Arc::clone(&tier_cache),
                    social_graph:           Arc::clone(&social_graph),
                    audio_feed_repo:        Arc::clone(&audio_feed_repo),
                    audio_feed_store:       Arc::clone(&audio_feed_store),
                    feed_cap:               config.feed_cap,
                    vip_registry_cap:       config.vip_registry_cap,
                    vip_registry_ttl_secs:  config.vip_registry_ttl_secs,
                    tier_cache_ttl_secs:    config.tier_cache_ttl_secs,
                    social_graph_page_size: config.social_graph_page_size,
                    audio_feed_cap:         config.audio_feed_cap,
                })?
                .register::<RemovePostCommand, _>(RemovePostHandler {
                    feed_store:       Arc::clone(&feed_store),
                    vip_registry:     Arc::clone(&vip_registry),
                    feed_repository:  Arc::clone(&feed_repository),
                    author_post_repo: Arc::clone(&author_post_repo),
                    tier_cache:       Arc::clone(&tier_cache),
                })?
                .register::<BackfillFollowCommand, _>(BackfillFollowHandler {
                    feed_store:       Arc::clone(&feed_store),
                    feed_repository:  Arc::clone(&feed_repository),
                    author_post_repo: Arc::clone(&author_post_repo),
                    tier_cache:       Arc::clone(&tier_cache),
                    following_store:  Arc::clone(&following_store),
                    feed_cap:         config.feed_cap,
                    backfill_limit:   config.backfill_limit,
                })?
                .register::<PruneFollowCommand, _>(PruneFollowHandler {
                    feed_store:       Arc::clone(&feed_store),
                    feed_repository:  Arc::clone(&feed_repository),
                    tier_cache:       Arc::clone(&tier_cache),
                    following_store:  Arc::clone(&following_store),
                })?
                .register::<IngestAudioIndexCommand, _>(IngestAudioIndexHandler {
                    audio_feed_repo:  Arc::clone(&audio_feed_repo),
                    audio_feed_store: Arc::clone(&audio_feed_store),
                    audio_feed_cap:   config.audio_feed_cap,
                })?
                .register::<ApplyDiscoverySignalCommand, _>(ApplyDiscoverySignalHandler {
                    pool: Arc::clone(&discovery_pool),
                })?
                .build(),
        );

        // ── Query bus ────────────────────────────────────────────────────────
        let query_bus = Arc::new(
            QueryBusBuilder::new()
                .register::<GetFollowingFeedQuery, _>(GetFollowingFeedHandler {
                    feed_store:             Arc::clone(&feed_store),
                    vip_registry:           Arc::clone(&vip_registry),
                    feed_repository:        Arc::clone(&feed_repository),
                    author_post_repo:       Arc::clone(&author_post_repo),
                    tier_cache:             Arc::clone(&tier_cache),
                    following_store:        Arc::clone(&following_store),
                    social_graph:           Arc::clone(&social_graph),
                    max_page_size:          config.max_page_size,
                    feed_cap:               config.feed_cap,
                    vip_registry_cap:       config.vip_registry_cap,
                    vip_registry_ttl_secs:  config.vip_registry_ttl_secs,
                    warm_ttl_secs:          config.warm_ttl_secs,
                    social_graph_page_size: config.social_graph_page_size,
                    max_vip_merge_sources:  config.max_vip_merge_sources,
                    warm_semaphore:         Arc::new(Semaphore::new(config.warm_max_concurrency)),
                    warming:                Arc::new(Mutex::new(HashSet::new())),
                })?
                .register::<GetAudioFeedQuery, _>(GetAudioFeedHandler {
                    audio_feed_store: Arc::clone(&audio_feed_store),
                    audio_feed_repo:  Arc::clone(&audio_feed_repo),
                    max_page_size:    config.max_page_size,
                })?
                .register::<GetDiscoveryFeedQuery, _>(GetDiscoveryFeedHandler {
                    pool:              Arc::clone(&discovery_pool),
                    social_graph:      Arc::clone(&social_graph),
                    nearby,
                    max_page_size:     config.max_page_size,
                    nearby_candidates: config.nearby_candidates,
                    hot_gravity_secs:  config.discovery_hot_gravity_secs,
                })?
                .build(),
        );

        // ── Ingestion workers (Kafka path) ───────────────────────────────────
        if let Some(kafka_config) = kafka {
            tokio::spawn(
                PostPublishedWorker::new(
                    kafka_config.clone(),
                    Arc::clone(&command_bus),
                    config.kafka_group_post_published.clone(),
                )
                .run(),
            );
            tokio::spawn(
                PostDeletedWorker::new(
                    kafka_config.clone(),
                    Arc::clone(&command_bus),
                    config.kafka_group_post_deleted.clone(),
                )
                .run(),
            );
            tokio::spawn(
                FollowCreatedWorker::new(
                    kafka_config.clone(),
                    Arc::clone(&command_bus),
                    config.kafka_group_sg_followed.clone(),
                )
                .run(),
            );
            tokio::spawn(
                DiscoveryWorker::new(
                    kafka_config.clone(),
                    Arc::clone(&command_bus),
                    config.kafka_group_discovery.clone(),
                )
                .run(),
            );
            tokio::spawn(
                FollowDeletedWorker::new(
                    kafka_config,
                    Arc::clone(&command_bus),
                    config.kafka_group_sg_unfollowed.clone(),
                )
                .run(),
            );
        }

        Ok(Self {
            command_bus,
            query_bus,
            feed_store:       feed_store as Arc<dyn FeedStore>,
            vip_registry:     vip_registry as Arc<dyn VipRegistry>,
            tier_cache:       tier_cache as Arc<dyn TierCache>,
            following_store:  following_store as Arc<dyn FollowingStore>,
            feed_repository:  feed_repository as Arc<dyn FeedRepository>,
            author_post_repo: author_post_repo as Arc<dyn AuthorPostRepository>,
            audio_feed_store,
            audio_feed_repo,
            discovery_pool,
            scylla: scylla_client,
            redis: redis_client,
        })
    }
}
