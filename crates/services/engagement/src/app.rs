//! The engagement service's composition root.
//!
//! [`App::build`] is *pure composition*: storage configs in, a fully-wired CQRS
//! graph out. It binds no socket and reads no environment, so the production
//! entrypoint ([`crate::service`]) and the live integration harness assemble
//! the exact same graph.
//!
//! Engagement is Redis-primary: likes (#665: a like is a point, staked in the
//! wallet) and the view/share/comment counters are read from Redis, and their
//! durable copies are written to ScyllaDB by the background workers. Those
//! workers — and the ScyllaDB client they need — are derived from
//! [`Backends::kafka`]: when it is `Some` they are spawned; when `None` the
//! harness drives the Redis hot path directly and never touches ScyllaDB or a
//! broker.

use std::sync::Arc;
use std::time::Duration;

use cqrs::command::{CommandBusBuilder, InMemoryCommandBus};
use cqrs::query::{InMemoryQueryBus, QueryBusBuilder};
use redis_storage::{RedisClient, RedisClientBuilder, RedisConfig};
use scylla_storage::{ScyllaConfig, ScyllaSessionBuilder};
use transport::kafka::config::client::KafkaClientConfig;

use crate::application::command::record_share::{RecordShareCommand, RecordShareHandler};
use crate::application::command::record_view::{RecordViewCommand, RecordViewHandler};
use crate::application::erasure::LikeEraser;
use crate::application::port::{LikeLedger, LikeStore, LikeVisibility, ProfileAccess, ProfileTabs, ScoreStore};
use crate::application::query::batch_get_likes::{BatchGetLikesHandler, BatchGetLikesQuery};
use crate::application::query::get_like_positions::{GetLikePositionsHandler, GetLikePositionsQuery};
use crate::application::query::get_post_engagement::{GetPostEngagementHandler, GetPostEngagementQuery};
use crate::application::query::list_likes_by_account::{ListLikesByAccountHandler, ListLikesByAccountQuery};
use crate::application::query::list_likes_by_profile::{ListLikesByProfileHandler, ListLikesByProfileQuery};
use crate::infrastructure::persistence::{ScyllaCounterLedger, ScyllaLikeLedger, ScyllaProfileTabs};
use crate::infrastructure::scoring::redis_like_store::RedisLikeStore;
use crate::infrastructure::scoring::redis_score_store::{DirtyPostTracker, RedisScoreStore};
use crate::infrastructure::worker::{
    account_consumer::AccountConsumer, comment_consumer::CommentEventConsumer, counter_flush::CounterFlushWorker,
    profile_consumer::ProfileConsumer, stake_consumer::StakeConsumer,
};

/// Storage/transport endpoints the graph is wired against.
///
/// `kafka` is optional: `Some` builds the ScyllaDB ledgers and spawns the
/// stake, counter-flush and comment workers; `None` leaves the Redis hot path
/// driveable directly with no ScyllaDB or broker.
pub struct Backends {
    pub scylla: ScyllaConfig,
    pub redis:  RedisConfig,
    pub kafka:  Option<KafkaClientConfig>,
}

/// A fully-wired engagement service bound to its backends. The buses and the
/// Redis stores exposed here are the *same* instances the handlers hold.
pub struct App {
    pub command_bus: Arc<InMemoryCommandBus>,
    pub query_bus:   Arc<InMemoryQueryBus>,
    pub score_store: Arc<dyn ScoreStore>,
    /// The likes (#665): each account's total per post and comment.
    pub like_store:  Arc<dyn LikeStore>,
    /// Live Redis client (the always-on hot path), retained so the runtime's
    /// readiness loop can probe it (see [`crate::service`]).
    pub redis:       RedisClient,
}

impl App {
    /// Builds the Redis stores and CQRS buses; when Kafka is configured, also
    /// builds the ScyllaDB ledgers and spawns the workers. `likes`: who hides
    /// like counts (#809, post) and whose each post is; `None` withholds
    /// nothing, and a Likes tab is then the owner's only. `access`: who may
    /// see a profile (#829, social-graph); `None`: likewise.
    pub async fn build(
        backends: Backends,
        likes:    Option<Arc<dyn LikeVisibility>>,
        access:   Option<Arc<dyn ProfileAccess>>,
    ) -> Result<Self, Box<dyn std::error::Error>> {
        let Backends { scylla, redis, kafka } = backends;

        // ── Redis hot path (always) ──────────────────────────────────────────
        let redis_client = RedisClientBuilder::new(redis).build().await?;
        let dirty_tracker = DirtyPostTracker::new();
        let score_store = Arc::new(RedisScoreStore::new(redis_client.clone(), dirty_tracker.clone()));
        let like_store: Arc<dyn LikeStore> = Arc::new(RedisLikeStore::new(redis_client.clone()));

        // ── Durable ledgers (with the Kafka path only) ───────────────────────
        let ledgers = match &kafka {
            Some(_) => {
                let scylla_client = Arc::new(ScyllaSessionBuilder::new(scylla).build().await?);
                let like_ledger: Arc<dyn LikeLedger> = Arc::new(ScyllaLikeLedger::new(Arc::clone(&scylla_client)));
                let tabs: Arc<dyn ProfileTabs> = Arc::new(ScyllaProfileTabs::new(Arc::clone(&scylla_client)));
                Some((Arc::new(ScyllaCounterLedger::new(scylla_client)), like_ledger, tabs))
            }
            None => None,
        };

        // ── CQRS buses ───────────────────────────────────────────────────────
        let command_bus = Arc::new(
            CommandBusBuilder::new()
                .register::<RecordViewCommand, _>(RecordViewHandler { score_store: Arc::clone(&score_store) })?
                .register::<RecordShareCommand, _>(RecordShareHandler { score_store: Arc::clone(&score_store) })?
                .build(),
        );

        let query_bus = Arc::new(
            QueryBusBuilder::new()
                .register::<ListLikesByAccountQuery, _>(ListLikesByAccountHandler {
                    ledger: ledgers.as_ref().map(|(_, likes, _)| Arc::clone(likes)),
                })?
                .register::<ListLikesByProfileQuery, _>(ListLikesByProfileHandler {
                    ledger: ledgers.as_ref().map(|(_, likes, _)| Arc::clone(likes)),
                    tabs:   ledgers.as_ref().map(|(_, _, tabs)| Arc::clone(tabs)),
                    access,
                    posts:  likes.clone(),
                })?
                .register::<GetLikePositionsQuery, _>(GetLikePositionsHandler {
                    like_store:  Arc::clone(&like_store),
                    like_ledger: ledgers.as_ref().map(|(_, l, _)| Arc::clone(l)),
                })?
                .register::<BatchGetLikesQuery, _>(BatchGetLikesHandler {
                    like_store:  Arc::clone(&like_store),
                    like_ledger: ledgers.as_ref().map(|(_, l, _)| Arc::clone(l)),
                    likes:       likes.clone(),
                })?
                .register::<GetPostEngagementQuery, _>(GetPostEngagementHandler {
                    score_store: Arc::clone(&score_store),
                    likes,
                    like_store:  Arc::clone(&like_store),
                    like_ledger: ledgers.as_ref().map(|(_, l, _)| Arc::clone(l)),
                })?
                .build(),
        );

        // ── Workers (Kafka path) ─────────────────────────────────────────────
        if let (Some(kafka_client), Some((counters, like_ledger, tabs))) = (kafka, ledgers) {
            // Which profiles show their Likes tab (#829).
            tokio::spawn(ProfileConsumer::new(kafka_client.clone(), tabs, "engagement-profile-tabs").run());
            // Likes are points (#665): the wallet's stakes become the likes.
            tokio::spawn(
                StakeConsumer::new(kafka_client.clone(), Arc::clone(&like_store), Arc::clone(&like_ledger), "engagement-stakes")
                    .run(),
            );
            // A deleted account's likes: who liked goes, the counts stay.
            let eraser = LikeEraser { store: Arc::clone(&like_store), ledger: like_ledger };
            tokio::spawn(AccountConsumer::new(kafka_client.clone(), eraser, "engagement-account-erasure").run());
            tokio::spawn(
                CounterFlushWorker::new(Arc::clone(&score_store), Arc::clone(&counters), dirty_tracker, Duration::from_secs(5))
                    .run(),
            );
            tokio::spawn(
                CommentEventConsumer::new(kafka_client, Arc::clone(&score_store), counters, "engagement-comment-consumer")
                    .run(),
            );
        }

        Ok(Self {
            command_bus,
            query_bus,
            score_store: score_store as Arc<dyn ScoreStore>,
            like_store,
            redis: redis_client,
        })
    }
}
