//! Integration harness: boots the shared infra, wires a real timeline graph
//! against it through the production composition root, and exposes the buses,
//! cache/persistence handles, and the social-graph fake for assertions.
//!
//! Kafka is never booted: scenarios drive the ingestion command handlers
//! directly through [`command_bus`](TestHarness::command_bus) and read through
//! [`query_bus`](TestHarness::query_bus), which is faster and fully deterministic.
#![allow(dead_code)]

use std::sync::Arc;
use std::time::Duration;

use uuid::Uuid;

use cqrs::command::InMemoryCommandBus;
use cqrs::query::InMemoryQueryBus;
use cqrs::{CommandBus, Envelope, QueryBus};
use redis_storage::RedisConfig;
use scylla_storage::ScyllaConfig;

use timeline::app::{App, AppConfig, Backends};
use timeline::application::command::ingest_post_published::IngestPostPublishedCommand;
use timeline::application::port::{DiscoveryPool, FeedStore, FollowingStore, TierCache, VipRegistry};
use timeline::application::query::get_following_feed::{FollowingFeedPage, GetFollowingFeedQuery};

pub use timeline::domain::value_object::{AuthorId, ProfileId};
pub use test_support::await_until;

use crate::timeline_it::fakes::{FakeNearby, FakeSocialGraph};

/// Generous default patience for a cross-component assertion (Redis round-trip,
/// async warm-up task completion).
pub const DEADLINE: Duration = Duration::from_secs(10);

/// ScyllaDB keyspace the migrations provision.
const KEYSPACE: &str = "timeline";
/// On-disk migration assets, resolved against *this* crate's manifest.
const MIGRATIONS_DIR: &str = concat!(env!("CARGO_MANIFEST_DIR"), "/migrations");

/// Per-scenario knobs. Defaults mirror production; scenarios shrink the cap or
/// the warm TTL to force eviction / re-cold in a single test.
#[derive(Debug, Clone)]
pub struct HarnessOptions {
    pub feed_cap:      u16,
    pub warm_ttl_secs: u64,
    pub max_page_size: i32,
}

impl Default for HarnessOptions {
    fn default() -> Self {
        Self { feed_cap: 500, warm_ttl_secs: 86_400, max_page_size: 50 }
    }
}

/// A fully-wired timeline service bound to ephemeral infra, plus assertion handles.
pub struct TestHarness {
    pub command_bus:     Arc<InMemoryCommandBus>,
    pub query_bus:       Arc<InMemoryQueryBus>,
    pub feed_store:      Arc<dyn FeedStore>,
    pub vip_registry:    Arc<dyn VipRegistry>,
    pub tier_cache:      Arc<dyn TierCache>,
    pub following_store: Arc<dyn FollowingStore>,
    pub social_graph:    Arc<FakeSocialGraph>,
    pub discovery_pool:  Arc<dyn DiscoveryPool>,
    pub nearby:          Arc<FakeNearby>,
}

impl TestHarness {
    /// Boots/reuses the shared containers, applies migrations, and assembles the
    /// service graph with an in-process social-graph fake.
    pub async fn start(opts: HarnessOptions) -> Self {
        let scylla_cp = test_support::containers::scylla_ready(KEYSPACE, MIGRATIONS_DIR).await;
        let redis_endpoint = test_support::containers::redis_endpoint().await;

        let backends = Backends {
            scylla: ScyllaConfig {
                contact_points: vec![scylla_cp],
                keyspace:       None,
                ..ScyllaConfig::default()
            },
            redis: RedisConfig { hosts: vec![redis_endpoint], ..RedisConfig::default() },
            kafka: None,
        };

        let config = AppConfig {
            feed_cap:                   opts.feed_cap,
            audio_feed_cap:             1_000,
            vip_registry_cap:           200,
            backfill_limit:             100,
            warm_ttl_secs:              opts.warm_ttl_secs,
            tier_cache_ttl_secs:        3_600,
            vip_registry_ttl_secs:      604_800,
            max_page_size:              opts.max_page_size,
            max_vip_merge_sources:      50,
            warm_max_concurrency:       64,
            social_graph_page_size:     500,
            kafka_group_post_published: "timeline-it-post-published".to_owned(),
            kafka_group_post_deleted:   "timeline-it-post-deleted".to_owned(),
            kafka_group_sg_followed:    "timeline-it-sg-followed".to_owned(),
            kafka_group_sg_unfollowed:  "timeline-it-sg-unfollowed".to_owned(),
            discovery_window_secs:      259_200,
            discovery_pool_cap:         10_000,
            discovery_hot_gravity_secs: 45_000.0,
            nearby_candidates:          300,
            kafka_group_discovery:      "timeline-it-discovery".to_owned(),
        };

        let social_graph = Arc::new(FakeSocialGraph::new());
        let nearby = Arc::new(FakeNearby::default());
        let app = App::build(&config, backends, Arc::clone(&social_graph), Arc::clone(&nearby) as _)
            .await
            .expect("integration: build timeline app");

        Self {
            command_bus:     app.command_bus,
            query_bus:       app.query_bus,
            feed_store:      app.feed_store,
            vip_registry:    app.vip_registry,
            tier_cache:      app.tier_cache,
            following_store: app.following_store,
            social_graph,
            discovery_pool:  app.discovery_pool,
            nearby,
        }
    }

    /// Fan-out-on-write a single published post for `author` (server-minted id).
    pub async fn ingest_post(&self, author: &AuthorId, tier: u8, published_at_ms: i64) -> String {
        let post_id = Uuid::now_v7().to_string();
        let cmd = IngestPostPublishedCommand {
            post_id:         post_id.clone(),
            author_id:       author.as_uuid().to_string(),
            author_tier:     tier,
            published_at_ms,
            audio_id:        None,
        };
        self.command_bus
            .dispatch(Envelope::new(Uuid::now_v7(), cmd))
            .await
            .expect("ingest_post_published");
        post_id
    }

    /// Reads the first page of `profile`'s following feed.
    pub async fn get_following_feed(&self, profile: &ProfileId) -> FollowingFeedPage {
        dispatch_following(Arc::clone(&self.query_bus), profile.as_uuid().to_string())
            .await
            .expect("get_following_feed")
    }
}

/// Dispatches a following-feed read on a shared bus — a free function so scenarios
/// can fire many concurrently from spawned tasks.
pub async fn dispatch_following(
    query_bus:  Arc<InMemoryQueryBus>,
    profile_id: String,
) -> Result<FollowingFeedPage, cqrs::CqrsError> {
    let query = GetFollowingFeedQuery { profile_id, limit: 50, page_token: None };
    query_bus.dispatch(Envelope::new(Uuid::now_v7(), query)).await
}

/// A fresh random profile id (a feed reader).
pub fn random_profile() -> ProfileId {
    ProfileId::from_uuid(Uuid::now_v7())
}

/// A fresh random author id (a content producer).
pub fn random_author() -> AuthorId {
    AuthorId::from_uuid(Uuid::now_v7())
}

/// Author-tier discriminants as the ingestion command expects them.
pub const TIER_STANDARD: u8 = 0;
pub const TIER_VIP: u8 = 2;
