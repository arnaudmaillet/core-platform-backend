//! The chat service's composition root.
//!
//! [`App::build`] is *pure composition*: storage configs in, a fully-wired
//! service graph out. It binds no socket and reads no environment — both the
//! production entrypoint ([`crate::infrastructure::grpc::server::serve`]) and the
//! live integration harness drive the exact same assembly, so the suite tests the
//! graph that ships rather than a parallel re-wiring.
//!
//! The event-publisher choice is *derived*, not injected: when [`Backends::kafka`]
//! is `Some`, the durable [`KafkaEventPublisher`] is used and the per-pod
//! [`VisibilityWorker`] is spawned; when it is `None`, the in-process
//! [`LogEventPublisher`] is used and no broker is required. This is what lets the
//! integration scenarios that don't exercise Kafka boot without one.

use std::sync::Arc;

use cqrs::command::{CommandBusBuilder, InMemoryCommandBus};
use cqrs::query::{InMemoryQueryBus, QueryBusBuilder};
use redis_storage::{RedisClient, RedisClientBuilder, RedisConfig, RedisSubscriberBuilder};
use scylla_storage::{ScyllaClient, ScyllaConfig, ScyllaSessionBuilder};
use transport::kafka::config::client::KafkaClientConfig;
use transport::kafka::config::producer::ProducerConfig;
use transport::kafka::producer::KafkaProducerBuilder;

use crate::application::command::{
    CreateConversationCommand, CreateConversationHandler, JoinAsMemberCommand, JoinAsMemberHandler,
    MarkReadCommand, MarkReadHandler, SendMessageCommand, SendMessageHandler, SubscribeCommand,
    SubscribeHandler, ToggleVisibilityCommand, ToggleVisibilityHandler, UnsubscribeCommand,
    UnsubscribeHandler,
};
use crate::application::port::{
    ConversationRepository, EventPublisher, HotTailCache, MemberRepository, PresenceSettingsStore,
    PresenceStore, ReceiptStore, RoutingRegistry,
};
use crate::application::query::{
    GetHistoryHandler, GetHistoryQuery, ListMembersHandler, ListMembersQuery,
    ListSubscriptionsHandler, ListSubscriptionsQuery,
};
use crate::infrastructure::cache::{
    RedisHotTailCache, RedisPresenceStore, RedisReceiptStore, RedisRoutingRegistry,
};
use crate::infrastructure::event::{KafkaEventPublisher, LogEventPublisher};
use crate::infrastructure::grpc::handler::chat_handler::StreamingParams;
use crate::infrastructure::grpc::handler::ChatServiceHandler;
use crate::infrastructure::persistence::{
    ScyllaConversationRepository, ScyllaMemberRepository, ScyllaMessageRepository,
    ScyllaPresenceSettingsStore, ScyllaSubscriptionRepository,
};
use crate::infrastructure::routing::{
    Fanout, MessageFanout, PlaneAttach, PlaneSubscriber, RedisPlaneBroadcaster,
};
use crate::infrastructure::streaming::{ConversationBroadcastRegistry, PlaneFanoutSink};
use crate::infrastructure::worker::{PresenceSettingsWorker, VisibilityWorker};

/// Storage/transport endpoints the graph is wired against.
///
/// `kafka` is optional: its presence selects the durable publisher and enables
/// the [`VisibilityWorker`]; its absence selects the in-process log publisher.
pub struct Backends {
    pub scylla: ScyllaConfig,
    pub redis:  RedisConfig,
    pub kafka:  Option<KafkaClientConfig>,
}

/// The tuning surface threaded through the graph. Production fills this from
/// [`ChatConfig`](crate::config::ChatConfig); integration scenarios shrink the
/// buffers/TTLs to make overflow and liveness assertions complete in seconds.
#[derive(Debug, Clone)]
pub struct AppConfig {
    pub max_page_size:              i32,
    pub hot_tail_cache_size:        u16,
    pub message_bucket_hours:       u32,
    pub member_stream_buffer_size:  usize,
    pub audience_stream_buffer_size: usize,
    pub audience_shard_count:       u16,
    pub presence_ttl_secs:          u64,
    pub typing_ttl_secs:            u64,
    /// TTL (seconds) for the Audience-Plane shard activation and shadow fan-out.
    /// Production reuses `presence_ttl_secs`; scenarios set it independently.
    pub audience_ttl_secs:          u64,
    /// Kafka consumer-group id for the per-pod [`VisibilityWorker`]. Production
    /// uses a stable id; scenarios suffix a UUID for isolation.
    pub visibility_consumer_group:  String,
    /// Kafka consumer-group id for the [`PresenceSettingsWorker`] (stable in
    /// production; scenarios suffix a UUID).
    pub presence_settings_consumer_group: String,
}

/// A fully-wired chat service bound to its backends, plus the shared `Arc`
/// handles a test asserts against. The handler holds the *same* `Arc`s exposed
/// here, so a scenario reads the live state the handler mutates.
pub struct App {
    pub handler:           ChatServiceHandler<InMemoryCommandBus, InMemoryQueryBus>,
    /// Live storage clients, retained so the runtime's readiness loop can probe
    /// their liveness (see [`crate::service`]).
    pub scylla:            Arc<ScyllaClient>,
    pub redis:             RedisClient,
    pub presence:          Arc<dyn PresenceStore>,
    /// The members' presence settings (projected from `profile.v1.events`).
    pub presence_settings: Arc<dyn PresenceSettingsStore>,
    pub routing:           Arc<dyn RoutingRegistry>,
    pub hot_tail:          Arc<dyn HotTailCache>,
    pub member_registry:   Arc<ConversationBroadcastRegistry>,
    pub audience_registry: Arc<ConversationBroadcastRegistry>,
    pub params:            StreamingParams,
}

impl App {
    /// Builds storage clients from `backends`, assembles the repositories, cache,
    /// routing, CQRS buses, and streaming registries, and spawns the per-pod
    /// background tasks (plane subscriber, registry reapers, and — when Kafka is
    /// configured — the visibility worker).
    pub async fn build(
        config:   &AppConfig,
        backends: Backends,
    ) -> Result<Self, Box<dyn std::error::Error>> {
        let Backends { scylla, redis, kafka } = backends;

        // ── Storage clients ──────────────────────────────────────────────────
        let scylla_client = Arc::new(ScyllaSessionBuilder::new(scylla).build().await?);
        let redis_client = RedisClientBuilder::new(redis.clone()).build().await?;
        let redis_subscriber = RedisSubscriberBuilder::new(redis).build().await?;

        // ── Repositories ─────────────────────────────────────────────────────
        let conversation_repo =
            Arc::new(ScyllaConversationRepository::new(Arc::clone(&scylla_client)));
        let message_repo = Arc::new(ScyllaMessageRepository::new(
            Arc::clone(&scylla_client),
            config.message_bucket_hours,
        ));
        let member_repo = Arc::new(ScyllaMemberRepository::new(Arc::clone(&scylla_client)));
        let subscription_repo =
            Arc::new(ScyllaSubscriptionRepository::new(Arc::clone(&scylla_client)));

        // ── Cache / routing adapters ─────────────────────────────────────────
        let hot_tail = Arc::new(RedisHotTailCache::new(redis_client.clone()));
        let presence = Arc::new(RedisPresenceStore::new(redis_client.clone()));
        let receipt = Arc::new(RedisReceiptStore::new(redis_client.clone()));
        let routing = Arc::new(RedisRoutingRegistry::new(redis_client.clone()));
        let presence_settings: Arc<dyn PresenceSettingsStore> =
            Arc::new(ScyllaPresenceSettingsStore::new(Arc::clone(&scylla_client)));
        let broadcaster = Arc::new(RedisPlaneBroadcaster::new(redis_client.clone()));

        // ── In-process fan-out registries + per-pod subscriber ───────────────
        let member_registry =
            Arc::new(ConversationBroadcastRegistry::new(config.member_stream_buffer_size));
        let audience_registry =
            Arc::new(ConversationBroadcastRegistry::new(config.audience_stream_buffer_size));
        let sink = Arc::new(PlaneFanoutSink::new(
            Arc::clone(&member_registry),
            Arc::clone(&audience_registry),
        ));
        let plane_subscriber = Arc::new(PlaneSubscriber::new(redis_subscriber, sink));
        Arc::clone(&plane_subscriber).spawn();

        // ── Message-fork orchestrator ────────────────────────────────────────
        let fanout = Arc::new(MessageFanout::new(
            Arc::clone(&broadcaster),
            Arc::clone(&routing),
            Arc::clone(&hot_tail),
            config.hot_tail_cache_size,
            config.audience_ttl_secs,
        ));

        // ── CQRS buses (publisher derived from Kafka presence) ───────────────
        let command_bus = match &kafka {
            Some(cfg) => {
                let producer = KafkaProducerBuilder::new(ProducerConfig::new(cfg.clone())).build()?;
                build_command_bus(
                    Arc::new(KafkaEventPublisher::new(producer)),
                    &conversation_repo,
                    &message_repo,
                    &member_repo,
                    &subscription_repo,
                )?
            }
            None => build_command_bus(
                Arc::new(LogEventPublisher),
                &conversation_repo,
                &message_repo,
                &member_repo,
                &subscription_repo,
            )?,
        };

        let query_bus = QueryBusBuilder::new()
            .register::<GetHistoryQuery, _>(GetHistoryHandler {
                conversation_repo: Arc::clone(&conversation_repo),
                member_repo:       Arc::clone(&member_repo),
                message_repo:      Arc::clone(&message_repo),
                max_page_size:     config.max_page_size,
            })?
            .register::<ListMembersQuery, _>(ListMembersHandler {
                member_repo: Arc::clone(&member_repo),
            })?
            .register::<ListSubscriptionsQuery, _>(ListSubscriptionsHandler {
                subscription_repo: Arc::clone(&subscription_repo),
                max_page_size:     config.max_page_size,
            })?
            .build();

        // ── VisibilityWorker (Kafka path): cluster-wide Audience-Plane teardown
        if let Some(cfg) = &kafka {
            let worker = VisibilityWorker::new(
                cfg.clone(),
                Arc::clone(&audience_registry),
                Arc::clone(&routing) as Arc<dyn RoutingRegistry>,
                config.visibility_consumer_group.clone(),
            );
            tokio::spawn(worker.run());
            tokio::spawn(
                PresenceSettingsWorker::new(
                    cfg.clone(),
                    Arc::clone(&presence_settings),
                    config.presence_settings_consumer_group.clone(),
                )
                .run(),
            );
        }

        // ── Registry reapers ─────────────────────────────────────────────────
        tokio::spawn(Arc::clone(&member_registry).run_reaper());
        tokio::spawn(Arc::clone(&audience_registry).run_reaper());

        let params = StreamingParams {
            presence_ttl_secs:    config.presence_ttl_secs,
            typing_ttl_secs:      config.typing_ttl_secs,
            audience_shard_count: config.audience_shard_count,
            audience_ttl_secs:    config.audience_ttl_secs,
        };

        let handler = ChatServiceHandler::new(
            command_bus,
            query_bus,
            Arc::clone(&fanout) as Arc<dyn Fanout>,
            Arc::clone(&plane_subscriber) as Arc<dyn PlaneAttach>,
            Arc::clone(&member_registry),
            Arc::clone(&audience_registry),
            Arc::clone(&presence) as Arc<dyn PresenceStore>,
            Arc::clone(&receipt) as Arc<dyn ReceiptStore>,
            Arc::clone(&routing) as Arc<dyn RoutingRegistry>,
            Arc::clone(&conversation_repo) as Arc<dyn ConversationRepository>,
            Arc::clone(&member_repo) as Arc<dyn MemberRepository>,
            Arc::clone(&presence_settings),
            params,
        );

        Ok(Self {
            handler,
            scylla: scylla_client,
            redis: redis_client,
            presence: presence as Arc<dyn PresenceStore>,
            presence_settings,
            routing: routing as Arc<dyn RoutingRegistry>,
            hot_tail: hot_tail as Arc<dyn HotTailCache>,
            member_registry,
            audience_registry,
            params,
        })
    }
}

/// Builds the command bus generically over the event publisher so the same wiring
/// serves both the durable Kafka-backed run and the log-backed run.
fn build_command_bus<EP: EventPublisher>(
    publisher:         Arc<EP>,
    conversation_repo: &Arc<ScyllaConversationRepository>,
    message_repo:      &Arc<ScyllaMessageRepository>,
    member_repo:       &Arc<ScyllaMemberRepository>,
    subscription_repo: &Arc<ScyllaSubscriptionRepository>,
) -> Result<InMemoryCommandBus, Box<dyn std::error::Error>> {
    Ok(CommandBusBuilder::new()
        .register::<CreateConversationCommand, _>(CreateConversationHandler {
            conversation_repo: Arc::clone(conversation_repo),
            member_repo:       Arc::clone(member_repo),
            publisher:         Arc::clone(&publisher),
        })?
        .register::<SendMessageCommand, _>(SendMessageHandler {
            member_repo:  Arc::clone(member_repo),
            message_repo: Arc::clone(message_repo),
            publisher:    Arc::clone(&publisher),
        })?
        .register::<ToggleVisibilityCommand, _>(ToggleVisibilityHandler {
            conversation_repo: Arc::clone(conversation_repo),
            member_repo:       Arc::clone(member_repo),
            publisher:         Arc::clone(&publisher),
        })?
        .register::<JoinAsMemberCommand, _>(JoinAsMemberHandler {
            conversation_repo: Arc::clone(conversation_repo),
            member_repo:       Arc::clone(member_repo),
            publisher:         Arc::clone(&publisher),
        })?
        .register::<SubscribeCommand, _>(SubscribeHandler {
            conversation_repo: Arc::clone(conversation_repo),
            subscription_repo: Arc::clone(subscription_repo),
        })?
        .register::<UnsubscribeCommand, _>(UnsubscribeHandler {
            subscription_repo: Arc::clone(subscription_repo),
        })?
        .register::<MarkReadCommand, _>(MarkReadHandler { member_repo: Arc::clone(member_repo) })?
        .build())
}
