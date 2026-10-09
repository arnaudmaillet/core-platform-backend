//! Integration harness: boots an ephemeral Redis container, wires a real
//! engagement graph against it through the production composition root, and
//! exposes the buses for assertions.
//!
//! Engagement's hot path is Redis-only, so — uniquely — this harness boots no
//! ScyllaDB and no Kafka: `Backends.kafka = None` skips the workers
//! (and the ScyllaDB client they need), and the placeholder ScyllaDB config is
//! never dialled.
#![allow(dead_code)]

use std::sync::Arc;
use std::time::Duration;

use uuid::Uuid;

use cqrs::command::InMemoryCommandBus;
use cqrs::query::InMemoryQueryBus;
use cqrs::{CommandBus, CqrsError, Envelope, QueryBus};
use redis_storage::RedisConfig;
use scylla_storage::ScyllaConfig;

use engagement::app::{App, Backends};
use engagement::application::command::record_view::RecordViewCommand;
use engagement::application::query::get_post_engagement::GetPostEngagementQuery;

pub use engagement::application::port::PostEngagementSnapshot;
pub use engagement::domain::value_object::PostId;
pub use test_support::await_until;

/// Generous default patience for a cross-component assertion (Redis round-trip).
pub const DEADLINE: Duration = Duration::from_secs(10);

/// A fully-wired engagement service bound to ephemeral Redis, plus the buses.
pub struct TestHarness {
    pub command_bus: Arc<InMemoryCommandBus>,
    pub query_bus:   Arc<InMemoryQueryBus>,
    /// The likes (#665) over the live Redis.
    pub like_store:  Arc<dyn engagement::application::port::LikeStore>,
    /// The live Redis itself (to expire a key as time would).
    pub redis:       redis_storage::RedisClient,
}

impl TestHarness {
    /// Boots/reuses the shared Redis container and assembles the service graph
    /// (no ScyllaDB, no Kafka, no workers).
    pub async fn start() -> Self {
        let redis_endpoint = test_support::containers::redis_endpoint().await;

        let backends = Backends {
            // Never dialled: with `kafka = None` the ScyllaDB client is not built.
            scylla: ScyllaConfig::default(),
            redis:  RedisConfig { hosts: vec![redis_endpoint], ..RedisConfig::default() },
            kafka:  None,
        };

        let app = App::build(backends, None, None)
            .await
            .expect("integration: build engagement app");

        Self { command_bus: app.command_bus, query_bus: app.query_bus, like_store: app.like_store, redis: app.redis }
    }

    /// Records a single view for `post`.
    pub async fn record_view(&self, post: &PostId) {
        dispatch_view(Arc::clone(&self.command_bus), post.as_str())
            .await
            .expect("record_view");
    }

    /// Current engagement snapshot for `post`.
    pub async fn snapshot(&self, post: &PostId) -> PostEngagementSnapshot {
        let engagement: engagement::application::query::get_post_engagement::PostEngagement = self
            .query_bus
            .dispatch(Envelope::new(Uuid::now_v7(), GetPostEngagementQuery {
                post_id: post.as_str(),
                reader:  engagement::application::query::get_post_engagement::EngagementReader::Internal,
                account: None,
            }))
            .await
            .expect("get_post_engagement");
        engagement.snapshot
    }
}

/// Dispatches a view record on a shared bus.
pub async fn dispatch_view(command_bus: Arc<InMemoryCommandBus>, post_id: String) -> Result<(), CqrsError> {
    command_bus
        .dispatch(Envelope::new(Uuid::now_v7(), RecordViewCommand { post_id }))
        .await
}

/// A fresh random post id.
pub fn random_post() -> PostId {
    PostId::from_uuid(Uuid::now_v7())
}
