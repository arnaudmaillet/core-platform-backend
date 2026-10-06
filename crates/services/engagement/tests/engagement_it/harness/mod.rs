//! Integration harness: boots an ephemeral Redis container, wires a real
//! engagement graph against it through the production composition root, and
//! exposes the buses for assertions.
//!
//! Engagement's hot path is Redis-only, so — uniquely — this harness boots no
//! ScyllaDB and no Kafka: `Backends.kafka = None` skips the write-behind workers
//! (and the ScyllaDB client they need), and the placeholder ScyllaDB config is
//! never dialled.
#![allow(dead_code)]

use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use uuid::Uuid;

use cqrs::command::InMemoryCommandBus;
use cqrs::query::InMemoryQueryBus;
use cqrs::{CommandBus, CqrsError, Envelope, QueryBus};
use redis_storage::RedisConfig;
use scylla_storage::ScyllaConfig;

use engagement::app::{App, Backends};
use engagement::application::command::record_view::RecordViewCommand;
use engagement::application::command::remove_reaction::RemoveReactionCommand;
use engagement::application::command::upsert_reaction::UpsertReactionCommand;
use engagement::application::port::EngagementEventPublisher;
use engagement::application::query::get_post_engagement::GetPostEngagementQuery;
use engagement::config::ReactionWeightsConfig;
use engagement::domain::event::reaction_event::ReactionKafkaEvent;
use engagement::error::EngagementError;

pub use engagement::application::port::PostEngagementSnapshot;
pub use engagement::domain::value_object::{PostId, ProfileId};
pub use test_support::await_until;

/// Generous default patience for a cross-component assertion (Redis round-trip).
pub const DEADLINE: Duration = Duration::from_secs(10);

/// `ReactionKind::Heart` — a valid 1-based proto ordinal.
pub const KIND_HEART: i32 = 1;

/// A no-op event publisher: the Redis-primary scenarios assert on the score
/// store, not on the write-behind Kafka contract.
struct NoopPublisher;

#[async_trait]
impl EngagementEventPublisher for NoopPublisher {
    async fn publish_reaction_event(&self, _event: &ReactionKafkaEvent) -> Result<(), EngagementError> {
        Ok(())
    }
}

/// A fully-wired engagement service bound to ephemeral Redis, plus the buses.
pub struct TestHarness {
    pub command_bus: Arc<InMemoryCommandBus>,
    pub query_bus:   Arc<InMemoryQueryBus>,
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

        let weights = Arc::new(ReactionWeightsConfig::from_env().expect("reaction weights"));
        let app = App::build(backends, weights, Arc::new(NoopPublisher), None)
            .await
            .expect("integration: build engagement app");

        Self { command_bus: app.command_bus, query_bus: app.query_bus }
    }

    /// Upserts a reaction for `(post, profile)`.
    pub async fn upsert(&self, post: &PostId, profile: &ProfileId, kind: i32) {
        dispatch_upsert(Arc::clone(&self.command_bus), post.as_str(), profile.as_str(), kind)
            .await
            .expect("upsert_reaction");
    }

    /// Removes `profile`'s reaction from `post`.
    pub async fn remove(&self, post: &PostId, profile: &ProfileId) {
        let cmd = RemoveReactionCommand { post_id: post.as_str(), profile_id: profile.as_str() };
        self.command_bus
            .dispatch(Envelope::new(Uuid::now_v7(), cmd))
            .await
            .expect("remove_reaction");
    }

    /// Records a single view for `post`.
    pub async fn record_view(&self, post: &PostId) {
        dispatch_view(Arc::clone(&self.command_bus), post.as_str())
            .await
            .expect("record_view");
    }

    /// Current engagement snapshot for `post`.
    pub async fn snapshot(&self, post: &PostId) -> PostEngagementSnapshot {
        self.query_bus
            .dispatch(Envelope::new(Uuid::now_v7(), GetPostEngagementQuery {
                post_id: post.as_str(),
                reader:  engagement::application::query::get_post_engagement::EngagementReader::Internal,
            }))
            .await
            .expect("get_post_engagement")
    }
}

/// Dispatches an upsert on a shared bus — a free function so scenarios can fire
/// many concurrently from spawned tasks.
pub async fn dispatch_upsert(
    command_bus: Arc<InMemoryCommandBus>,
    post_id:     String,
    profile_id:  String,
    kind:        i32,
) -> Result<(), CqrsError> {
    let cmd = UpsertReactionCommand { post_id, profile_id, kind };
    command_bus.dispatch(Envelope::new(Uuid::now_v7(), cmd)).await
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

/// A fresh random profile id.
pub fn random_profile() -> ProfileId {
    ProfileId::from_uuid(Uuid::now_v7())
}

/// The stored score for the heart reaction in a snapshot (0 when absent).
pub fn heart_score(snapshot: &PostEngagementSnapshot) -> i64 {
    snapshot.reaction_scores.get("heart").copied().unwrap_or(0)
}
