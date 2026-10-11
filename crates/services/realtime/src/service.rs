//! Adapts the realtime composition roots to the fleet [`service_runtime::Service`]
//! contract — **twice**, because realtime is two deployables that share a domain
//! but no process or failure domain:
//!
//! * [`RealtimeGatewayService`] (`realtime-gateway`) — the stateful edge. Builds
//!   the adapters, opens the node-channel subscriber + the public WS server as
//!   background tasks, and serves only health + reflection on its internal gRPC
//!   port (:50066). The connections live in the spawned WS server.
//! * [`RealtimeDispatcherService`] (`realtime-dispatcher`) — the stateless fan-out
//!   worker. Builds the routing fabric, spawns the supervised `run_consumer`
//!   loops that decode upstream events and fan them out, and serves only health +
//!   reflection on its port (:50067). Exposes no domain RPC.

use std::sync::Arc;
use std::time::Duration;

use anyhow::Context;
use async_trait::async_trait;
use redis_storage::RedisClient;
use service_runtime::{FnProbe, HealthProbe, InfraRegistry, Service};
use tonic::service::RoutesBuilder;
use tonic_reflection::server::Builder as ReflectionBuilder;
use transport::kafka::config::{ConsumerConfig, KafkaClientConfig, ProducerConfig};
use transport::kafka::consumer::{KafkaConsumerBuilder, KafkaConsumerHandle};
use transport::kafka::producer::{KafkaProducerBuilder, KafkaProducerHandle};

use crate::app::Adapters;
use crate::application::FanOutHandler;
use crate::application::handshake::HandshakeHandler;
use crate::application::lifecycle::ReapHandler;
use crate::config::RealtimeConfig;
use crate::domain::NodeId;
use crate::infrastructure::decode::{
    NotificationWire, PopularityWire, PostWire, map_counter_popularity, map_notification, map_post,
};
use crate::infrastructure::runtime::{
    ConnectionTable, GatewayState, run_fanout_consumer, serve_ws, spawn_node_subscriber,
};

/// Health-reporting key for both binaries (they serve no domain RPC; this is the
/// readiness key the runtime marks `SERVING`).
const HEALTH_KEY: &str = "realtime.v1.RealtimeDispatchService";

const NOTIFICATION_TOPIC: &str = "notification.v1.events";
const NOTIFICATION_GROUP: &str = "realtime-notif-fanout";
const POPULARITY_TOPIC: &str = "counter.v1.popularity";
const POPULARITY_GROUP: &str = "realtime-counter-fanout";
const POST_TOPIC: &str = "post.v1.events";
const POST_GROUP: &str = "realtime-post-fanout";

/// Backoff before respawning a consumer after its runner returns.
const CONSUMER_RESPAWN_BACKOFF: Duration = Duration::from_secs(5);

// ── Gateway ───────────────────────────────────────────────────────────────────

pub struct RealtimeGatewayService {
    redis: RedisClient,
}

#[async_trait]
impl Service for RealtimeGatewayService {
    const NAME: &'static str = "realtime-gateway";
    const VERSION: &'static str = env!("CARGO_PKG_VERSION");
    const GRPC_SERVICE_NAME: &'static str = HEALTH_KEY;

    async fn build(_infra: Arc<InfraRegistry>) -> anyhow::Result<Self> {
        let config = RealtimeConfig::from_env();
        let adapters = Adapters::build(&config).await.context("build adapters")?;

        let node_id = NodeId::new(config.node_id.clone())
            .map_err(|e| anyhow::anyhow!("invalid REALTIME_NODE_ID: {e}"))?;
        let table = Arc::new(ConnectionTable::new());

        // The node-channel subscriber turns inbound deliveries into socket frames.
        spawn_node_subscriber(adapters.subscriber, config.node_id.clone(), Arc::clone(&table));

        let handshake = Arc::new(HandshakeHandler::new(
            Arc::clone(&adapters.verifier),
            Arc::clone(&adapters.registry),
            node_id,
            config.subscription_cap,
        ));
        let reap = Arc::new(ReapHandler::new(Arc::clone(&adapters.registry)));

        let state = GatewayState {
            handshake,
            reap,
            table,
            send_queue_cap: config.send_queue_cap,
            heartbeat_interval: config.heartbeat_interval,
            heartbeat_timeout: config.heartbeat_timeout,
            revocations: adapters.revocations.clone(),
        };

        let ws_addr = config.ws_addr.clone();
        tokio::spawn(async move {
            if let Err(error) = serve_ws(state, ws_addr).await {
                tracing::error!(%error, "realtime gateway WS server stopped");
            }
        });

        Ok(Self {
            redis: adapters.redis,
        })
    }

    fn health_probes(&self) -> Vec<Arc<dyn HealthProbe>> {
        vec![redis_probe(self.redis.clone())]
    }

    fn register(self, routes: &mut RoutesBuilder) -> anyhow::Result<()> {
        let reflection = ReflectionBuilder::configure()
            .register_encoded_file_descriptor_set(realtime_api::FILE_DESCRIPTOR_SET)
            .build_v1()?;
        routes.add_service(reflection);
        Ok(())
    }
}

// ── Dispatcher ────────────────────────────────────────────────────────────────

pub struct RealtimeDispatcherService {
    redis: RedisClient,
}

#[async_trait]
impl Service for RealtimeDispatcherService {
    const NAME: &'static str = "realtime-dispatcher";
    const VERSION: &'static str = env!("CARGO_PKG_VERSION");
    const GRPC_SERVICE_NAME: &'static str = HEALTH_KEY;

    async fn build(_infra: Arc<InfraRegistry>) -> anyhow::Result<Self> {
        let config = RealtimeConfig::from_env();
        let adapters = Adapters::build(&config).await.context("build adapters")?;

        let handler = Arc::new(FanOutHandler::new(
            Arc::clone(&adapters.registry),
            Arc::clone(&adapters.node_channel),
        ));

        // Supervised fan-out consumers: notification (targeted), counter popularity
        // and post (public broadcast). chat stays on its own live plane (coexist).
        spawn_fanout_consumer::<NotificationWire, _>(
            NOTIFICATION_TOPIC,
            NOTIFICATION_GROUP,
            "notification",
            &handler,
            map_notification,
        );
        spawn_fanout_consumer::<PopularityWire, _>(
            POPULARITY_TOPIC,
            POPULARITY_GROUP,
            "popularity",
            &handler,
            map_counter_popularity,
        );
        spawn_fanout_consumer::<PostWire, _>(
            POST_TOPIC,
            POST_GROUP,
            "post",
            &handler,
            map_post,
        );

        Ok(Self {
            redis: adapters.redis,
        })
    }

    fn health_probes(&self) -> Vec<Arc<dyn HealthProbe>> {
        vec![redis_probe(self.redis.clone())]
    }

    fn register(self, routes: &mut RoutesBuilder) -> anyhow::Result<()> {
        let reflection = ReflectionBuilder::configure()
            .register_encoded_file_descriptor_set(realtime_api::FILE_DESCRIPTOR_SET)
            .build_v1()?;
        routes.add_service(reflection);
        Ok(())
    }
}

// ── Shared wiring helpers ─────────────────────────────────────────────────────

/// A hot-tier liveness probe: a trivial `EVAL 'return 1'` round-trip to Redis.
fn redis_probe(redis: RedisClient) -> Arc<dyn HealthProbe> {
    use fred::interfaces::LuaInterface;
    Arc::new(FnProbe::new("redis", move || {
        let redis = redis.clone();
        async move {
            let _: i64 = redis
                .inner
                .eval("return 1", Vec::<String>::new(), Vec::<String>::new())
                .await
                .map_err(|e: fred::error::Error| anyhow::anyhow!(e.to_string()))?;
            Ok(())
        }
    }))
}

/// Spawns a supervised fan-out consumer that rebuilds its Kafka handles and
/// restarts after a backoff whenever the runner returns.
fn spawn_fanout_consumer<T, M>(
    topic: &'static str,
    group: &'static str,
    label: &'static str,
    handler: &Arc<FanOutHandler>,
    map: M,
) where
    T: transport::kafka::envelope::ConsumablePayload + Clone,
    M: Fn(T) -> Result<Option<crate::application::DeliverableEvent>, crate::error::RealtimeError>
        + Copy
        + Send
        + Sync
        + 'static,
{
    let handler = Arc::clone(handler);
    tokio::spawn(async move {
        loop {
            match build_consumer(topic, group) {
                Ok((consumer, producer)) => {
                    run_fanout_consumer::<T, _>(label, consumer, producer, Arc::clone(&handler), map)
                        .await;
                    tracing::warn!(label, "consumer exited; respawning after backoff");
                }
                Err(error) => tracing::error!(label, %error, "failed to build consumer; retrying"),
            }
            tokio::time::sleep(CONSUMER_RESPAWN_BACKOFF).await;
        }
    });
}

/// Builds a manual-commit consumer (subscribed to `topic`) and the dead-letter
/// producer the runner needs. Kafka config is read fresh per respawn.
fn build_consumer(
    topic: &str,
    group: &str,
) -> anyhow::Result<(KafkaConsumerHandle, KafkaProducerHandle)> {
    let kafka = KafkaClientConfig::from_env();
    let consumer = KafkaConsumerBuilder::new(ConsumerConfig::new(kafka.clone(), group))
        .subscribe(topic)
        .build()
        .with_context(|| format!("build consumer for {topic}"))?;
    let producer = KafkaProducerBuilder::new(ProducerConfig::new(kafka))
        .build()
        .with_context(|| format!("build dead-letter producer for {topic}"))?;
    Ok((consumer, producer))
}
