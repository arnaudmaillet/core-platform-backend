//! The social-graph service's composition root.
//!
//! [`App::build`] is *pure composition*: storage configs and an [`EventPublisher`]
//! in, a fully-wired CQRS graph out. It binds no socket and reads no environment,
//! so a binary entrypoint and the live integration harness assemble the exact
//! same graph.
//!
//! The event publisher is injected as a trait object (the handlers already hold
//! `Arc<dyn EventPublisher>`): production passes the Kafka publisher; the
//! integration harness passes an in-process no-op, so the adjacency-consistency
//! and block-override scenarios run without a broker.

use std::sync::Arc;

use cqrs::command::{CommandBusBuilder, InMemoryCommandBus};
use cqrs::query::{InMemoryQueryBus, QueryBusBuilder};
use redis_storage::{RedisClient, RedisClientBuilder, RedisConfig};
use scylla_storage::{ScyllaClient, ScyllaConfig, ScyllaSessionBuilder};

use crate::application::command::{
    ApproveFollowRequestCommand, ApproveFollowRequestHandler, BlockProfileCommand,
    BlockProfileHandler, FollowProfileCommand, FollowProfileHandler, WithdrawFollowRequestCommand,
    WithdrawFollowRequestHandler,
    RecordProfileAudienceCommand, RecordProfileAudienceHandler, UnblockProfileCommand,
    UnblockProfileHandler, UnfollowProfileCommand, UnfollowProfileHandler,
};
use crate::application::port::{EventPublisher, SocialGraphCache, SocialGraphRepository};
use crate::application::query::{
    CheckAccessHandler, CheckAccessQuery, CheckInteractionHandler, CheckInteractionQuery, GetRelationStatusHandler, GetRelationStatusQuery,
    ListBlocksHandler, ListBlocksQuery, ListFollowRequestsHandler, ListFollowRequestsQuery,
    ListFollowersHandler, ListFollowersQuery, ListFollowingHandler, ListFollowingQuery,
};
use crate::domain::value_object::TierThresholds;
use crate::infrastructure::cache::RedisSocialGraphCache;
use crate::infrastructure::persistence::ScyllaSocialGraphRepository;

/// Storage endpoints the graph is wired against.
pub struct Backends {
    pub scylla: ScyllaConfig,
    pub redis:  RedisConfig,
}

/// A fully-wired social-graph service bound to its backends. The buses exposed
/// here are the *same* instances the handlers are registered into; the
/// `ListFollowers`/`ListFollowing` queries read the separate adjacency tables, so
/// the query bus alone proves their consistency.
pub struct App {
    pub command_bus: Arc<InMemoryCommandBus>,
    pub query_bus:   Arc<InMemoryQueryBus>,
    /// Live storage clients, retained so the runtime's readiness loop can probe
    /// their liveness (see [`crate::service`]).
    pub scylla:      Arc<ScyllaClient>,
    pub redis:       Arc<RedisClient>,
}

impl App {
    /// Builds storage clients from `backends`, assembles the ScyllaDB repository
    /// and Redis cache, and registers every social-graph command and query
    /// against the supplied `publisher`.
    pub async fn build(
        backends:        Backends,
        publisher:       Arc<dyn EventPublisher>,
        tier_thresholds: TierThresholds,
    ) -> Result<Self, Box<dyn std::error::Error>> {
        let Backends { scylla, redis } = backends;

        let scylla_client = Arc::new(ScyllaSessionBuilder::new(scylla).build().await?);
        let redis_client = Arc::new(RedisClientBuilder::new(redis).build().await?);

        let repo: Arc<dyn SocialGraphRepository> =
            Arc::new(ScyllaSocialGraphRepository::new(Arc::clone(&scylla_client)));
        let cache: Arc<dyn SocialGraphCache> =
            Arc::new(RedisSocialGraphCache::new(Arc::clone(&redis_client)));

        let command_bus = Arc::new(
            CommandBusBuilder::new()
                .register::<FollowProfileCommand, _>(FollowProfileHandler::new(
                    Arc::clone(&repo),
                    Arc::clone(&cache),
                    Arc::clone(&publisher),
                    tier_thresholds,
                ))?
                .register::<UnfollowProfileCommand, _>(UnfollowProfileHandler::new(
                    Arc::clone(&repo),
                    Arc::clone(&cache),
                    Arc::clone(&publisher),
                    tier_thresholds,
                ))?
                .register::<BlockProfileCommand, _>(BlockProfileHandler::new(
                    Arc::clone(&repo),
                    Arc::clone(&cache),
                    Arc::clone(&publisher),
                ))?
                .register::<UnblockProfileCommand, _>(UnblockProfileHandler::new(
                    Arc::clone(&repo),
                    Arc::clone(&cache),
                    Arc::clone(&publisher),
                ))?
                .register::<RecordProfileAudienceCommand, _>(RecordProfileAudienceHandler::new(
                    Arc::clone(&repo),
                ))?
                .register::<ApproveFollowRequestCommand, _>(ApproveFollowRequestHandler::new(
                    Arc::clone(&repo),
                    Arc::clone(&cache),
                    Arc::clone(&publisher),
                    tier_thresholds,
                ))?
                .register::<WithdrawFollowRequestCommand, _>(WithdrawFollowRequestHandler::new(
                    Arc::clone(&repo),
                ))?
                .build(),
        );

        let query_bus = Arc::new(
            QueryBusBuilder::new()
                .register::<GetRelationStatusQuery, _>(GetRelationStatusHandler::new(
                    Arc::clone(&repo),
                    Arc::clone(&cache),
                ))?
                .register::<ListFollowersQuery, _>(ListFollowersHandler::new(Arc::clone(&repo)))?
                .register::<ListFollowingQuery, _>(ListFollowingHandler::new(Arc::clone(&repo)))?
                .register::<ListBlocksQuery, _>(ListBlocksHandler::new(Arc::clone(&repo)))?
                .register::<ListFollowRequestsQuery, _>(ListFollowRequestsHandler::new(Arc::clone(&repo)))?
                .register::<CheckAccessQuery, _>(CheckAccessHandler::new(Arc::clone(&repo)))?
                .register::<CheckInteractionQuery, _>(CheckInteractionHandler::new(Arc::clone(&repo)))?
                .build(),
        );

        Ok(Self { command_bus, query_bus, scylla: scylla_client, redis: redis_client })
    }
}
