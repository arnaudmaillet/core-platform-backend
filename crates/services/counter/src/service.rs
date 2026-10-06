//! Adapts the counter composition roots to the fleet [`service_runtime::Service`]
//! contract — **twice**, because counter-analytics is two deployables that share a
//! domain but no process or failure domain:
//!
//! * [`CounterReadService`] (`counter-server`) — the low-latency read API. Builds
//!   the storage ports, composes the gRPC read handler, serves `counter.v1` on
//!   :50064, and reports Redis liveness. No consumers, no aggregation.
//! * [`CounterWorkerService`] (`counter-worker`) — the stream processor. Builds the
//!   ports + the Kafka signal producer, spawns the five supervised firehose/domain
//!   consumers folding into a shared [`WindowAggregator`], and runs the drain/flush
//!   loop that fans windows out across the tiers and publishes popularity. Exposes
//!   no domain RPC (only health + reflection on its port).

use std::sync::Arc;
use std::time::Duration;

use anyhow::Context;
use async_trait::async_trait;
use fred::interfaces::LuaInterface;
use redis_storage::RedisClient;
use service_runtime::{FnProbe, HealthProbe, InfraRegistry, Service};
use service_runtime::edge::{authenticated, public_read};
use service_runtime::EdgePolicy;
use tokio::sync::Mutex;
use tonic::service::RoutesBuilder;
use tonic_reflection::server::Builder as ReflectionBuilder;
use transport::kafka::config::{ConsumerConfig, KafkaClientConfig, ProducerConfig};
use transport::kafka::consumer::{KafkaConsumerBuilder, KafkaConsumerHandle};
use transport::kafka::producer::{KafkaProducerBuilder, KafkaProducerHandle};

use tonic::transport::Channel;

use crate::app::{Ports, compose_read};
use crate::application::command::{DeltaFlusher, PopularityPublisher, Reconciler};
use crate::application::port::{ReconciliationSource, SignalPublisher};
use crate::config::CounterConfig;
use crate::domain::{Observation, WindowAggregator};
use crate::error::CounterError;
use crate::infrastructure::consumer::{run_flush_loop, run_fold_consumer};
use crate::infrastructure::reconcile::{GrpcReconciliationSource, run_reconcile_loop};
use crate::infrastructure::decode::{
    FollowWire, HitWire, ReactionWire, map_click, map_follow, map_impression, map_reaction, map_view,
};
use crate::infrastructure::grpc::{
    CounterServiceHandler, CounterServiceServer, FILE_DESCRIPTOR_SET,
};
use crate::infrastructure::kafka_signal_publisher::KafkaSignalPublisher;
use transport::kafka::envelope::ConsumablePayload;

const VIEW_TOPIC: &str = "view.v1.events";
const IMPRESSION_TOPIC: &str = "impression.v1.events";
const CLICK_TOPIC: &str = "click.v1.events";
const REACTION_TOPIC: &str = "engagement.reactions";
const FOLLOW_TOPIC: &str = "social-graph.follows";

const VIEW_GROUP: &str = "counter-view-aggregator";
const IMPRESSION_GROUP: &str = "counter-impression-aggregator";
const CLICK_GROUP: &str = "counter-click-aggregator";
const REACTION_GROUP: &str = "counter-reaction-aggregator";
const FOLLOW_GROUP: &str = "counter-follow-aggregator";

/// Backoff before respawning a consumer after its runner returns.
const CONSUMER_RESPAWN_BACKOFF: Duration = Duration::from_secs(5);

type CounterServer = CounterServiceServer<CounterServiceHandler>;

// ── Read server ───────────────────────────────────────────────────────────────

/// The counter read API as hosted by [`service_runtime`].
pub struct CounterReadService {
    handler: CounterServiceHandler,
    redis: RedisClient,
}

#[async_trait]
impl Service for CounterReadService {
    const NAME: &'static str = "counter";
    const VERSION: &'static str = env!("CARGO_PKG_VERSION");
    const GRPC_SERVICE_NAME: &'static str = <CounterServer as tonic::server::NamedService>::NAME;

    /// The RPCs exposed on the client edge listener (`GRPC_EDGE_ADDR`); anything
    /// else on this service is mesh-only. See `transport::grpc::edge`.
    // GetTimeSeries is an analytics surface and stays mesh-only.
    const EDGE_POLICY: EdgePolicy = &[
        public_read("/counter.v1.CounterService/BatchGetCounters"),
        // Not viewer-aware yet (lists posts/authors unfiltered): members only until
        // it filters on the audience (CheckAccess). Guests get trending through the discovery feed (B3, #673).
        authenticated("/counter.v1.CounterService/GetTrending"),
    ];

    async fn build(_infra: Arc<InfraRegistry>) -> anyhow::Result<Self> {
        let CounterConfig {
            postgres,
            redis,
            scylla,
            aggregation_window,
            read_timeout,
            ..
        } = CounterConfig::from_env();
        let ports = Ports::build(postgres, redis, scylla, aggregation_window)
            .await
            .map_err(|e| anyhow::anyhow!("counter read ports build: {e}"))?;
        let handler = compose_read(
            Arc::clone(&ports.store),
            Arc::clone(&ports.ledger),
            Arc::clone(&ports.series),
            read_timeout,
        )
        .with_like_guard(like_guard_from_env()?);
        Ok(Self {
            handler,
            redis: ports.redis,
        })
    }

    fn health_probes(&self) -> Vec<Arc<dyn HealthProbe>> {
        vec![redis_probe(self.redis.clone())]
    }

    fn register(self, routes: &mut RoutesBuilder) -> anyhow::Result<()> {
        let reflection = ReflectionBuilder::configure()
            .register_encoded_file_descriptor_set(FILE_DESCRIPTOR_SET)
            .build_v1()?;
        routes.add_service(reflection);
        routes.add_service(CounterServiceServer::new(self.handler));
        Ok(())
    }
}

// ── Stream worker ───────────────────────────────────────────────────────────--

/// The counter stream processor as hosted by [`service_runtime`]. Exposes no domain
/// RPC; it serves only health + reflection on its port while the supervised
/// consumers and the flush loop do the work.
pub struct CounterWorkerService {
    redis: RedisClient,
}

#[async_trait]
impl Service for CounterWorkerService {
    const NAME: &'static str = "counter-worker";
    const VERSION: &'static str = env!("CARGO_PKG_VERSION");
    const GRPC_SERVICE_NAME: &'static str = <CounterServer as tonic::server::NamedService>::NAME;

    async fn build(_infra: Arc<InfraRegistry>) -> anyhow::Result<Self> {
        let CounterConfig {
            postgres,
            redis,
            scylla,
            kafka,
            aggregation_window,
            flush_interval,
            reconcile_interval,
            drift_tolerance,
            social_graph_endpoint,
            social_graph_rpc_timeout,
            social_graph_connect_timeout,
            ..
        } = CounterConfig::from_env();

        let ports = Ports::build(postgres, redis, scylla, aggregation_window)
            .await
            .map_err(|e| anyhow::anyhow!("counter worker ports build: {e}"))?;

        // Outbound signal producer (built once; the popularity loop reuses it).
        let producer = KafkaProducerBuilder::new(ProducerConfig::new(kafka))
            .build()
            .context("build popularity producer")?;
        let publisher: Arc<dyn SignalPublisher> = Arc::new(KafkaSignalPublisher::new(producer));

        let flusher = Arc::new(DeltaFlusher::new(
            Arc::clone(&ports.store),
            Arc::clone(&ports.ledger),
            Arc::clone(&ports.series),
        ));
        let popularity = Arc::new(PopularityPublisher::new(Arc::clone(&ports.store), publisher));
        let aggregator = Arc::new(Mutex::new(WindowAggregator::new(aggregation_window)));

        // Five supervised consumers fold into the one shared aggregator.
        spawn_consumer::<HitWire, _>(VIEW_TOPIC, VIEW_GROUP, "view", &aggregator, map_view);
        spawn_consumer::<HitWire, _>(
            IMPRESSION_TOPIC,
            IMPRESSION_GROUP,
            "impression",
            &aggregator,
            map_impression,
        );
        spawn_consumer::<HitWire, _>(CLICK_TOPIC, CLICK_GROUP, "click", &aggregator, map_click);
        spawn_consumer::<ReactionWire, _>(
            REACTION_TOPIC,
            REACTION_GROUP,
            "reaction",
            &aggregator,
            map_reaction,
        );
        spawn_consumer::<FollowWire, _>(
            FOLLOW_TOPIC,
            FOLLOW_GROUP,
            "follow",
            &aggregator,
            map_follow,
        );

        // The drain/flush + popularity loop.
        tokio::spawn(run_flush_loop(
            Arc::clone(&aggregator),
            flusher,
            popularity,
            flush_interval,
        ));

        // The reconciliation sweep: heal exact-counter drift against social-graph.
        // Lazy connect — a cold start does not require the dependency to be up.
        // Both deadlines are mandatory: tonic has no default request timeout, and
        // a hung call would stall the reconcile loop forever.
        let social_graph = Channel::from_shared(social_graph_endpoint)
            .context("invalid social-graph endpoint")?
            .timeout(social_graph_rpc_timeout)
            .connect_timeout(social_graph_connect_timeout)
            .connect_lazy();
        let source: Arc<dyn ReconciliationSource> =
            Arc::new(GrpcReconciliationSource::new(social_graph));
        let reconciler = Arc::new(Reconciler::new(
            Arc::clone(&ports.store),
            Arc::clone(&ports.ledger),
            source,
            drift_tolerance,
        ));
        tokio::spawn(run_reconcile_loop(
            reconciler,
            Arc::clone(&ports.ledger),
            reconcile_interval,
        ));

        Ok(Self { redis: ports.redis })
    }

    fn health_probes(&self) -> Vec<Arc<dyn HealthProbe>> {
        vec![redis_probe(self.redis.clone())]
    }

    fn register(self, routes: &mut RoutesBuilder) -> anyhow::Result<()> {
        let reflection = ReflectionBuilder::configure()
            .register_encoded_file_descriptor_set(FILE_DESCRIPTOR_SET)
            .build_v1()?;
        routes.add_service(reflection);
        Ok(())
    }
}

// ── Shared wiring helpers ─────────────────────────────────────────────────────

/// A hot-tier liveness probe: a trivial `EVAL 'return 1'` round-trip to Redis.
fn redis_probe(redis: RedisClient) -> Arc<dyn HealthProbe> {
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

/// Spawns a supervised fold consumer: rebuilds its Kafka handles and restarts after
/// a backoff whenever the runner returns.
fn spawn_consumer<T, M>(
    topic: &'static str,
    group: &'static str,
    label: &'static str,
    aggregator: &Arc<Mutex<WindowAggregator>>,
    map: M,
) where
    T: ConsumablePayload + Clone,
    M: Fn(T) -> Result<Vec<Observation>, CounterError> + Copy + Send + Sync + 'static,
{
    let aggregator = Arc::clone(aggregator);
    tokio::spawn(async move {
        loop {
            match build_consumer(topic, group) {
                Ok((consumer, producer)) => {
                    run_fold_consumer::<T, _>(
                        label,
                        consumer,
                        producer,
                        Arc::clone(&aggregator),
                        map,
                    )
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

/// Like counts withheld per their author's setting (#809), asked of post at
/// `COUNTER_POST_GRPC_ENDPOINT` (lazily connected; request / connect deadlines
/// `COUNTER_POST_RPC_TIMEOUT_MS` / `COUNTER_POST_CONNECT_TIMEOUT_MS`, 500 ms /
/// 1 s). Unset: nothing is withheld (until the mesh route exists).
fn like_guard_from_env() -> anyhow::Result<crate::application::query::LikeGuard> {
    use crate::application::query::LikeGuard;
    let Some(endpoint) = std::env::var("COUNTER_POST_GRPC_ENDPOINT").ok().filter(|v| !v.trim().is_empty()) else {
        tracing::warn!("COUNTER_POST_GRPC_ENDPOINT unset: hidden like counts are not withheld");
        return Ok(LikeGuard::default());
    };
    let ms = |key: &str, default: u64| {
        Duration::from_millis(std::env::var(key).ok().and_then(|v| v.parse().ok()).unwrap_or(default))
    };
    let channel = Channel::from_shared(endpoint)
        .map_err(|e| anyhow::anyhow!("invalid COUNTER_POST_GRPC_ENDPOINT: {e}"))?
        .timeout(ms("COUNTER_POST_RPC_TIMEOUT_MS", 500))
        .connect_timeout(ms("COUNTER_POST_CONNECT_TIMEOUT_MS", 1_000))
        .connect_lazy();
    Ok(LikeGuard::new(Arc::new(crate::infrastructure::GrpcLikeVisibility::new(channel))))
}
