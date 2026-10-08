//! The engagement service's composition root.
//!
//! [`App::build`] is *pure composition*: storage configs, the reaction weights,
//! and an [`EngagementEventPublisher`] in, a fully-wired CQRS graph out. It binds
//! no socket and reads no environment, so the production entrypoint
//! ([`crate::infrastructure::grpc::server::serve`]) and the live integration
//! harness assemble the exact same graph.
//!
//! Engagement is Redis-primary: the reaction/view/share hot path is a single
//! atomic Redis (Lua) round-trip, and durability to ScyllaDB is a *write-behind*
//! handled by the background workers. Those workers — and the ScyllaDB client
//! they need — are derived from [`Backends::kafka`]: when it is `Some` they are
//! spawned; when `None` the harness drives the Redis hot path directly and never
//! touches ScyllaDB or a broker.

use std::sync::Arc;
use std::time::Duration;

use cqrs::command::{CommandBusBuilder, InMemoryCommandBus};
use cqrs::query::{InMemoryQueryBus, QueryBusBuilder};
use redis_storage::{RedisClient, RedisClientBuilder, RedisConfig};
use scylla_storage::{ScyllaConfig, ScyllaSessionBuilder};
use transport::kafka::config::client::KafkaClientConfig;

use crate::application::command::record_share::{RecordShareCommand, RecordShareHandler};
use crate::application::command::record_view::{RecordViewCommand, RecordViewHandler};
use crate::application::command::remove_reaction::{RemoveReactionCommand, RemoveReactionHandler};
use crate::application::command::upsert_reaction::{UpsertReactionCommand, UpsertReactionHandler};
use crate::application::port::{EngagementEventPublisher, LikeLedger, LikeStore, LikeVisibility, ReactionLedger, ScoreStore};
use crate::application::query::batch_get_likes::{BatchGetLikesHandler, BatchGetLikesQuery};
use crate::application::query::list_reactions_by_profile::{ListReactionsByProfileHandler, ListReactionsByProfileQuery};
use crate::application::query::get_post_engagement::{
    GetPostEngagementHandler, GetPostEngagementQuery,
};
use crate::config::ReactionWeightsConfig;
use crate::infrastructure::persistence::{ScyllaLikeLedger, ScyllaReactionLedger};
use crate::infrastructure::scoring::redis_like_store::RedisLikeStore;
use crate::infrastructure::scoring::redis_score_store::{DirtyPostTracker, RedisScoreStore};
use crate::infrastructure::worker::{
    comment_consumer::CommentEventConsumer, counter_flush::CounterFlushWorker,
    reaction_write_behind::ReactionWriteBehindWorker, stake_consumer::StakeConsumer,
};

/// Storage/transport endpoints the graph is wired against.
///
/// `kafka` is optional: `Some` builds the ScyllaDB ledger and spawns the
/// write-behind, counter-flush, and comment-consumer workers; `None` leaves the
/// Redis hot path driveable directly with no ScyllaDB or broker.
pub struct Backends {
    pub scylla: ScyllaConfig,
    pub redis:  RedisConfig,
    pub kafka:  Option<KafkaClientConfig>,
}

/// A fully-wired engagement service bound to its backends. The buses and the
/// Redis score store exposed here are the *same* instances the handlers hold.
pub struct App {
    pub command_bus: Arc<InMemoryCommandBus>,
    pub query_bus:   Arc<InMemoryQueryBus>,
    pub score_store: Arc<dyn ScoreStore>,
    /// The likes (#665): each account's total per post and comment.
    pub like_store:  Arc<dyn LikeStore>,
    /// Live Redis client (the always-on hot path), retained so the runtime's
    /// readiness loop can probe it (see [`crate::service`]). ScyllaDB is only the
    /// async write-behind ledger and is not part of the serving readiness gate.
    pub redis:       RedisClient,
    /// The durable ledger when the write-behind path is on, for the opt-in
    /// profile-index backfill (#653).
    pub ledger:      Option<Arc<ScyllaReactionLedger>>,
}

impl App {
    /// Builds the Redis score store and CQRS buses; when Kafka is configured,
    /// also builds the ScyllaDB ledger and spawns the write-behind workers.
    /// `likes`: who hides like counts (#809, post); `None` withholds nothing.
    pub async fn build<P: EngagementEventPublisher>(
        backends:  Backends,
        weights:   Arc<ReactionWeightsConfig>,
        publisher: Arc<P>,
        likes:     Option<Arc<dyn LikeVisibility>>,
    ) -> Result<Self, Box<dyn std::error::Error>> {
        let Backends { scylla, redis, kafka } = backends;

        // ── Redis hot path (always) ──────────────────────────────────────────
        let redis_client = RedisClientBuilder::new(redis).build().await?;
        let dirty_tracker = DirtyPostTracker::new();
        let score_store =
            Arc::new(RedisScoreStore::new(redis_client.clone(), dirty_tracker.clone()));
        let like_store: Arc<dyn LikeStore> = Arc::new(RedisLikeStore::new(redis_client.clone()));

        // ── Durable ledger (with the write-behind path only) ─────────────────
        let (ledger, like_ledger) = match &kafka {
            Some(_) => {
                let scylla_client = Arc::new(ScyllaSessionBuilder::new(scylla).build().await?);
                let likes: Arc<dyn LikeLedger> = Arc::new(ScyllaLikeLedger::new(Arc::clone(&scylla_client)));
                (Some(Arc::new(ScyllaReactionLedger::new(scylla_client))), Some(likes))
            }
            None => (None, None),
        };

        // ── CQRS buses ───────────────────────────────────────────────────────
        let command_bus = Arc::new(
            CommandBusBuilder::new()
                .register::<UpsertReactionCommand, _>(UpsertReactionHandler {
                    score_store: Arc::clone(&score_store),
                    publisher:   Arc::clone(&publisher),
                    weights:     Arc::clone(&weights),
                })?
                .register::<RemoveReactionCommand, _>(RemoveReactionHandler {
                    score_store: Arc::clone(&score_store),
                    publisher:   Arc::clone(&publisher),
                })?
                .register::<RecordViewCommand, _>(RecordViewHandler {
                    score_store: Arc::clone(&score_store),
                })?
                .register::<RecordShareCommand, _>(RecordShareHandler {
                    score_store: Arc::clone(&score_store),
                })?
                .build(),
        );

        let query_bus = Arc::new(
            QueryBusBuilder::new()
                .register::<ListReactionsByProfileQuery, _>(ListReactionsByProfileHandler {
                    ledger: ledger.clone().map(|l| l as Arc<dyn ReactionLedger>),
                })?
                .register::<BatchGetLikesQuery, _>(BatchGetLikesHandler {
                    like_store: Arc::clone(&like_store),
                    likes:      likes.clone(),
                })?
                .register::<GetPostEngagementQuery, _>(GetPostEngagementHandler {
                    score_store: Arc::clone(&score_store),
                    likes,
                    like_store:  Arc::clone(&like_store),
                })?
                .build(),
        );

        // ── Write-behind workers (Kafka path) ────────────────────────────────
        if let (Some(kafka_client), Some(ledger), Some(like_ledger)) = (kafka, ledger.clone(), like_ledger) {
            // Likes are points (#665): the wallet's stakes become the likes.
            tokio::spawn(
                StakeConsumer::new(kafka_client.clone(), Arc::clone(&like_store), like_ledger, "engagement-stakes").run(),
            );

            tokio::spawn(
                ReactionWriteBehindWorker::new(
                    kafka_client.clone(),
                    Arc::clone(&ledger),
                    "engagement-reaction-write-behind",
                )
                .run(),
            );
            tokio::spawn(
                CounterFlushWorker::new(
                    Arc::clone(&score_store),
                    Arc::clone(&ledger),
                    dirty_tracker,
                    Duration::from_secs(5),
                )
                .run(),
            );
            tokio::spawn(
                CommentEventConsumer::new(
                    kafka_client,
                    Arc::clone(&score_store),
                    Arc::clone(&ledger),
                    "engagement-comment-consumer",
                )
                .run(),
            );
        }

        Ok(Self {
            command_bus,
            query_bus,
            score_store: score_store as Arc<dyn ScoreStore>,
            like_store,
            redis: redis_client,
            ledger,
        })
    }
}
