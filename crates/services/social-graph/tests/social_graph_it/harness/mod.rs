//! Integration harness: boots the shared infra, wires a real social-graph graph
//! against it through the production composition root, and exposes the buses for
//! assertions. The event publisher is an in-process no-op (these scenarios assert
//! on the persisted adjacency tables, not on emitted events).
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

use social_graph::app::{App, Backends};
use social_graph::application::command::{
    AudienceFact, BlockProfileCommand, FollowProfileCommand, RecordProfileAudienceCommand,
};
use social_graph::application::port::EventPublisher;
use social_graph::application::query::{CheckAccessQuery, ListFollowersQuery, ListFollowingQuery};
use social_graph::domain::event::DomainEvent;
use social_graph::error::SocialGraphError;

pub use social_graph::domain::access::{ContentAccess, Viewer};
pub use social_graph::domain::value_object::ProfileId;
pub use test_support::await_until;

/// Generous default patience for a cross-component assertion (ScyllaDB adjacency
/// write visibility, async block-sever fan-out).
pub const DEADLINE: Duration = Duration::from_secs(10);

/// ScyllaDB keyspace the migrations provision.
const KEYSPACE: &str = "social_graph";
/// On-disk migration assets, resolved against *this* crate's manifest.
const MIGRATIONS_DIR: &str = concat!(env!("CARGO_MANIFEST_DIR"), "/migrations");

/// A no-op event publisher: the graph-consistency scenarios assert on ScyllaDB,
/// not on the Kafka contract.
struct NoopPublisher;

#[async_trait]
impl EventPublisher for NoopPublisher {
    async fn publish(&self, _event: &DomainEvent) -> Result<(), SocialGraphError> {
        Ok(())
    }
}

/// A fully-wired social-graph service bound to ephemeral infra, plus the buses.
pub struct TestHarness {
    pub command_bus: Arc<InMemoryCommandBus>,
    pub query_bus:   Arc<InMemoryQueryBus>,
}

impl TestHarness {
    /// Boots/reuses the shared containers, applies migrations, and assembles the
    /// service graph with a no-op event publisher.
    pub async fn start() -> Self {
        let scylla_cp = test_support::containers::scylla_ready(KEYSPACE, MIGRATIONS_DIR).await;
        let redis_endpoint = test_support::containers::redis_endpoint().await;

        let backends = Backends {
            scylla: ScyllaConfig {
                contact_points: vec![scylla_cp],
                keyspace:       None,
                ..ScyllaConfig::default()
            },
            redis: RedisConfig { hosts: vec![redis_endpoint], ..RedisConfig::default() },
        };

        let app = App::build(
            backends,
            Arc::new(NoopPublisher) as Arc<dyn EventPublisher>,
            social_graph::domain::value_object::TierThresholds::new(10_000, 1_000_000),
        )
        .await
        .expect("integration: build social-graph app");

        Self { command_bus: app.command_bus, query_bus: app.query_bus }
    }

    /// `actor` follows `target`, expecting success.
    pub async fn follow(&self, actor: &ProfileId, target: &ProfileId) {
        dispatch_follow(Arc::clone(&self.command_bus), actor.as_str(), target.as_str())
            .await
            .expect("follow_profile");
    }

    /// `actor` blocks `target`.
    pub async fn block(&self, actor: &ProfileId, target: &ProfileId) {
        let cmd = BlockProfileCommand { actor_id: actor.as_str(), target_id: target.as_str() };
        self.command_bus
            .dispatch(Envelope::new(Uuid::now_v7(), cmd))
            .await
            .expect("block_profile");
    }

    /// Returns the follower profile ids of `target` (from the `followers` table).
    pub async fn followers(&self, target: &ProfileId) -> Vec<ProfileId> {
        let page = self
            .query_bus
            .dispatch(Envelope::new(
                Uuid::now_v7(),
                ListFollowersQuery {
                    followee_id: target.as_str(),
                    limit:       1000,
                    page_token:  None,
                    viewer:      Viewer::Internal,
                },
            ))
            .await
            .expect("list_followers");
        page.edges.into_iter().map(|e| e.profile_id).collect()
    }

    /// Records an audience fact, as the `profile.v1.events` consumer does.
    pub async fn audience(&self, profile: &ProfileId, fact: AudienceFact) {
        let cmd = RecordProfileAudienceCommand { profile_id: profile.as_str(), fact };
        self.command_bus.dispatch(Envelope::new(Uuid::now_v7(), cmd)).await.expect("audience");
    }

    /// What `viewers` may see of `target`, through `CheckAccess`.
    pub async fn access(&self, viewers: &[ProfileId], target: &ProfileId) -> ContentAccess {
        let query = CheckAccessQuery {
            viewer_profile_ids: viewers.iter().map(ProfileId::as_str).collect(),
            target_profile_ids: vec![target.as_str()],
        };
        let result: Vec<(ProfileId, ContentAccess)> = self
            .query_bus
            .dispatch(Envelope::new(Uuid::now_v7(), query))
            .await
            .expect("check_access");
        assert_eq!(result.len(), 1);
        result[0].1
    }

    /// Returns the follower ids of `target` as `viewer` sees them.
    pub async fn followers_as(&self, target: &ProfileId, viewer: Viewer) -> Vec<ProfileId> {
        let page = self
            .query_bus
            .dispatch(Envelope::new(
                Uuid::now_v7(),
                ListFollowersQuery { followee_id: target.as_str(), limit: 1000, page_token: None, viewer },
            ))
            .await
            .expect("list_followers");
        page.edges.into_iter().map(|e| e.profile_id).collect()
    }

    /// Returns the followee profile ids of `actor` (from the `following` table).
    pub async fn following(&self, actor: &ProfileId) -> Vec<ProfileId> {
        let page = self
            .query_bus
            .dispatch(Envelope::new(
                Uuid::now_v7(),
                ListFollowingQuery {
                    follower_id: actor.as_str(),
                    limit:       1000,
                    page_token:  None,
                    viewer:      Viewer::Internal,
                },
            ))
            .await
            .expect("list_following");
        page.edges.into_iter().map(|e| e.profile_id).collect()
    }
}

/// Dispatches a follow on a shared bus — a free function so scenarios can fire
/// many concurrently from spawned tasks. Returns the dispatch result so a
/// scenario can assert a block-gated re-follow is rejected.
pub async fn dispatch_follow(
    command_bus: Arc<InMemoryCommandBus>,
    actor_id:    String,
    target_id:   String,
) -> Result<(), CqrsError> {
    let cmd = FollowProfileCommand { actor_id, target_id };
    command_bus.dispatch(Envelope::new(Uuid::now_v7(), cmd)).await
}

/// A fresh random profile id.
pub fn random_profile() -> ProfileId {
    ProfileId::from_uuid(Uuid::now_v7())
}

/// Whether `set` contains `id` (by uuid).
pub fn contains(set: &[ProfileId], id: &ProfileId) -> bool {
    set.iter().any(|p| p.as_str() == id.as_str())
}
