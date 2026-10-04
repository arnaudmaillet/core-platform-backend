//! The notification service's composition root.
//!
//! [`App::build`] is *pure composition*: a [`NotificationConfig`] and storage
//! configs in, a fully-wired service graph out. It binds no socket and reads no
//! environment, so the production entrypoint
//! ([`crate::infrastructure::grpc::server::serve`]) and the live integration
//! harness drive the exact same assembly.
//!
//! The cache adapters take the whole `Arc<NotificationConfig>` (they read several
//! knobs each), so — unlike chat/timeline — there is no separate `AppConfig`; the
//! domain config *is* the tuning surface.
//!
//! The four Kafka workers are derived from [`Backends::kafka`]: when it is `Some`
//! they are spawned; when `None` the harness drives [`CreateNotificationCommand`]
//! and the gRPC handler directly against [`App::command_bus`] and
//! [`App::stream_registry`], so the stream-lifetime and counter scenarios need no
//! broker.

use std::sync::Arc;
use std::time::Duration;

use cqrs::command::{CommandBusBuilder, InMemoryCommandBus};
use cqrs::query::{InMemoryQueryBus, QueryBusBuilder};
use redis_storage::{RedisClient, RedisClientBuilder, RedisConfig};
use scylla_storage::{ScyllaClient, ScyllaConfig, ScyllaSessionBuilder};
use transport::kafka::config::client::KafkaClientConfig;
use transport::kafka::config::ProducerConfig;
use transport::kafka::producer::KafkaProducerBuilder;

use crate::application::command::create_notification::{
    CreateNotificationCommand, CreateNotificationHandler,
};
use crate::application::command::mark_read::{
    MarkAllReadCommand, MarkAllReadHandler, MarkReadCommand, MarkReadHandler,
};
use crate::application::command::push_settings::{
    RegisterDeviceCommand, RegisterDeviceHandler, UnregisterDeviceCommand, UnregisterDeviceHandler,
    UpdatePreferencesCommand, UpdatePreferencesHandler,
};
use crate::application::port::{
    BlockCache, DeviceRegistry, NotificationEventPublisher, NotificationRepository, PreferenceStore,
    UnreadCounter,
};
use crate::application::query::push_settings::{
    GetPreferencesHandler, GetPreferencesQuery, ResolvePushTargetsHandler, ResolvePushTargetsQuery,
};
use crate::application::query::get_unread_count::{GetUnreadCountHandler, GetUnreadCountQuery};
use crate::application::query::list_notifications::{
    ListNotificationsHandler, ListNotificationsQuery,
};
use crate::config::NotificationConfig;
use crate::infrastructure::cache::{RedisBlockCache, RedisUnreadCounter};
use crate::infrastructure::persistence::{ScyllaNotificationRepository, ScyllaPushSettings};
use crate::infrastructure::publisher::{KafkaNotificationPublisher, NoopNotificationPublisher};
use crate::infrastructure::streaming::BroadcastRegistry;
use crate::infrastructure::worker::{
    collapse_flush_worker::CollapseFlushWorker, comment_worker::CommentNotificationWorker,
    mention_worker::MentionNotificationWorker, reaction_worker::ReactionNotificationWorker,
};

/// Storage/transport endpoints the graph is wired against.
///
/// `kafka` is optional: `Some` spawns the three ingestion workers plus the
/// collapse-flush worker; `None` leaves the command handlers driveable directly.
pub struct Backends {
    pub scylla: ScyllaConfig,
    pub redis:  RedisConfig,
    pub kafka:  Option<KafkaClientConfig>,
}

/// A fully-wired notification service bound to its backends, plus the shared
/// `Arc` handles a scenario asserts against. The buses, broadcast registry, and
/// counter exposed here are the *same* instances the handlers and workers hold.
pub struct App {
    pub command_bus:     Arc<InMemoryCommandBus>,
    pub query_bus:       Arc<InMemoryQueryBus>,
    pub stream_registry: Arc<BroadcastRegistry>,
    pub counter:         Arc<dyn UnreadCounter>,
    pub repository:      Arc<dyn NotificationRepository>,
    pub block_cache:     Arc<dyn BlockCache>,
    /// Live storage clients, retained so the runtime's readiness loop can probe
    /// their liveness (see [`crate::service`]).
    pub scylla:          Arc<ScyllaClient>,
    pub redis:           RedisClient,
}

impl App {
    /// Builds storage clients from `backends`, assembles the repository, cache,
    /// broadcast registry, and CQRS buses, spawns the broadcast-registry reaper,
    /// and — when Kafka is configured — the four background workers.
    pub async fn build(
        config:   Arc<NotificationConfig>,
        backends: Backends,
    ) -> Result<Self, Box<dyn std::error::Error>> {
        let Backends { scylla, redis, kafka } = backends;

        // ── Storage clients ──────────────────────────────────────────────────
        let scylla_client = Arc::new(ScyllaSessionBuilder::new(scylla).build().await?);
        let redis_client = RedisClientBuilder::new(redis).build().await?;

        // ── Infrastructure objects ───────────────────────────────────────────
        let repository = Arc::new(ScyllaNotificationRepository::new(Arc::clone(&scylla_client)));
        let block_cache = Arc::new(RedisBlockCache::new(redis_client.clone(), Arc::clone(&config)));
        let counter = Arc::new(RedisUnreadCounter::new(
            redis_client.clone(),
            Arc::clone(&repository),
            Arc::clone(&config),
        ));
        let stream_registry = Arc::new(BroadcastRegistry::new(config.stream_buffer_size));
        let push_settings = Arc::new(ScyllaPushSettings::new(Arc::clone(&scylla_client)));
        let devices: Arc<dyn DeviceRegistry> = Arc::clone(&push_settings) as _;
        let preferences: Arc<dyn PreferenceStore> = push_settings;

        // ── Realtime push publisher (notification.v1.events) ─────────────────
        // Kafka-backed when a broker is configured; a no-op otherwise so the
        // harness drives the command path without a broker. Built before the bus
        // because the create handler holds it.
        let publisher: Arc<dyn NotificationEventPublisher> = match kafka.as_ref() {
            Some(cfg) => {
                let producer =
                    KafkaProducerBuilder::new(ProducerConfig::new(cfg.clone())).build()?;
                Arc::new(KafkaNotificationPublisher::new(producer))
            }
            None => Arc::new(NoopNotificationPublisher),
        };

        // ── CQRS buses ───────────────────────────────────────────────────────
        let command_bus = Arc::new(
            CommandBusBuilder::new()
                .register::<CreateNotificationCommand, _>(CreateNotificationHandler {
                    repository:      Arc::clone(&repository),
                    block_cache:     Arc::clone(&block_cache),
                    counter:         Arc::clone(&counter),
                    stream_registry: Arc::clone(&stream_registry),
                    publisher:       Arc::clone(&publisher),
                })?
                .register::<MarkReadCommand, _>(MarkReadHandler {
                    repository: Arc::clone(&repository),
                    counter:    Arc::clone(&counter),
                })?
                .register::<MarkAllReadCommand, _>(MarkAllReadHandler {
                    repository: Arc::clone(&repository),
                    counter:    Arc::clone(&counter),
                })?
                .register::<RegisterDeviceCommand, _>(RegisterDeviceHandler {
                    devices:     Arc::clone(&devices),
                    preferences: Arc::clone(&preferences),
                })?
                .register::<UnregisterDeviceCommand, _>(UnregisterDeviceHandler {
                    devices: Arc::clone(&devices),
                })?
                .register::<UpdatePreferencesCommand, _>(UpdatePreferencesHandler {
                    preferences: Arc::clone(&preferences),
                })?
                .build(),
        );

        let query_bus = Arc::new(
            QueryBusBuilder::new()
                .register::<ListNotificationsQuery, _>(ListNotificationsHandler {
                    repository:    Arc::clone(&repository),
                    counter:       Arc::clone(&counter),
                    max_page_size: config.max_page_size,
                })?
                .register::<GetUnreadCountQuery, _>(GetUnreadCountHandler {
                    counter: Arc::clone(&counter),
                })?
                .register::<GetPreferencesQuery, _>(GetPreferencesHandler {
                    preferences: Arc::clone(&preferences),
                })?
                .register::<ResolvePushTargetsQuery, _>(ResolvePushTargetsHandler {
                    devices:     Arc::clone(&devices),
                    preferences: Arc::clone(&preferences),
                })?
                .build(),
        );

        // ── Background workers (Kafka path) ──────────────────────────────────
        if let Some(kafka_config) = kafka {
            tokio::spawn(
                ReactionNotificationWorker::new(
                    kafka_config.clone(),
                    redis_client.clone(),
                    Arc::clone(&repository),
                    Arc::clone(&block_cache),
                    Arc::clone(&counter),
                    Arc::clone(&stream_registry),
                    Arc::clone(&config),
                    "notification-reaction-consumer",
                )
                .run(),
            );
            tokio::spawn(
                CommentNotificationWorker::new(
                    kafka_config.clone(),
                    redis_client.clone(),
                    Arc::clone(&repository),
                    Arc::clone(&block_cache),
                    Arc::clone(&counter),
                    Arc::clone(&stream_registry),
                    Arc::clone(&config),
                    "notification-comment-consumer",
                )
                .run(),
            );
            tokio::spawn(
                MentionNotificationWorker::new(
                    kafka_config.clone(),
                    redis_client.clone(),
                    Arc::clone(&repository),
                    Arc::clone(&block_cache),
                    Arc::clone(&counter),
                    Arc::clone(&stream_registry),
                    Arc::clone(&config),
                    "notification-mention-consumer",
                )
                .run(),
            );
            tokio::spawn(
                CollapseFlushWorker::new(
                    redis_client.clone(),
                    Arc::clone(&repository),
                    Arc::clone(&counter),
                    Arc::clone(&stream_registry),
                    Arc::clone(&config),
                    Duration::from_secs(config.collapse_flush_interval_secs),
                )
                .run(),
            );
        }

        // Periodic reaper for the broadcast registry (production parity; 60 s
        // cadence — tests reap by hand via `stream_registry.reap()`).
        tokio::spawn(Arc::clone(&stream_registry).run_reaper());

        Ok(Self {
            command_bus,
            query_bus,
            stream_registry,
            counter:     counter as Arc<dyn UnreadCounter>,
            repository:  repository as Arc<dyn NotificationRepository>,
            block_cache: block_cache as Arc<dyn BlockCache>,
            scylla:      scylla_client,
            redis:       redis_client,
        })
    }
}
