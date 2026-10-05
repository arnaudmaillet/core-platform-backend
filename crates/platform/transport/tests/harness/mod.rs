//! Reusable integration-test harness for the generic `run_consumer` runtime.
//!
//! This module owns all the broker plumbing so the individual scenarios (A–K, added in
//! later phases) stay declarative: each test asks the harness for an isolated
//! [`TestContext`], drives the runner, and asserts on durable broker state (committed
//! offsets and dead-letter records) via [`await_until`] — never via fixed sleeps.
//!
//! # Design pillars (mirroring the approved blueprint)
//!
//! - **One broker per test binary.** A single `apache/kafka-native` container is booted
//!   lazily through a [`OnceCell`] and shared by every test in the binary. The runner is
//!   stateless with respect to the broker, so sharing is safe and keeps the suite fast.
//! - **Isolation by namespacing.** Every [`TestContext`] mints a UUIDv7-suffixed topic,
//!   its `.dlq` sibling, and a consumer group, so tests never collide and can run in
//!   parallel without a shared-container teardown dance.
//! - **Explicit topic pre-creation.** Both topics are created synchronously (partitions=1,
//!   RF=1) via an [`AdminClient`] before the context is handed back, eliminating the
//!   metadata-auto-create race.
//! - **`auto.offset.reset = earliest`.** Consumers always read from offset 0, so a produce
//!   that races ahead of subscription is still observed — another flakiness source removed.
//!
//! Note on the broker image: `testcontainers-modules` 0.14 does not package a Redpanda
//! module, so this harness uses its `kafka` module (default image `apache/kafka-native`),
//! which provides the same fast-boot, KRaft single-node, no-ZooKeeper characteristics.

// Phase 1 ships the harness ahead of the scenarios that consume it. Until those land,
// some helpers have no in-tree caller; silence the lint rather than leave them unused.
#![allow(dead_code)]

use std::time::Duration;

use futures::StreamExt;
use rdkafka::admin::{AdminClient, AdminOptions, NewTopic, TopicReplication};
use rdkafka::client::DefaultClientContext;
use rdkafka::config::ClientConfig;
use rdkafka::consumer::{BaseConsumer, Consumer};
use rdkafka::{Offset, TopicPartitionList};
use testcontainers::runners::AsyncRunner;
use testcontainers::{ContainerAsync, ContainerRequest};
use testcontainers_modules::kafka::apache::{KAFKA_PORT, Kafka};
use tokio::sync::OnceCell;
use tokio::time::{Instant, sleep};
use uuid::Uuid;

use transport::kafka::config::client::KafkaClientConfig;
use transport::kafka::config::consumer::{AutoOffsetReset, ConsumerConfig};
use transport::kafka::config::producer::ProducerConfig;
use transport::kafka::consumer::{ConsumedMessage, KafkaConsumerBuilder, KafkaConsumerHandle};
use transport::kafka::producer::{KafkaProducerBuilder, KafkaProducerHandle};

/// Dead-letter topic suffix. Mirrors the (private) `DLQ_SUFFIX` in the runner so the
/// harness pre-creates exactly the topic the runner will publish evacuated records to.
const DLQ_SUFFIX: &str = ".dlq";

/// An address that accepts no connections, used to build the Scenario-J broken producer.
/// Port 1 is reserved and never bound, so produce attempts fail fast (bounded by the
/// producer's `message.timeout.ms`) rather than blocking.
const UNROUTABLE_BROKER: &str = "127.0.0.1:1";

/// Local produce deadline for the broken producer. Short enough that a Scenario-J
/// dead-letter publish fails within the test's patience, long enough to be unambiguous.
const BROKEN_PRODUCER_TIMEOUT_MS: u32 = 1_500;

/// The single Kafka broker shared by every test in this binary.
///
/// Held in a static so the container outlives all tests; cleanup is handled by the
/// testcontainers reaper when the process exits.
static BROKER: OnceCell<ContainerAsync<Kafka>> = OnceCell::const_new();

/// Boots the shared broker on first use and returns its `host:port` bootstrap string.
///
/// The `apache/kafka-native` image rewrites its advertised listener to `127.0.0.1:<mapped
/// port>`, so clients must dial that exact host — hence the hard-coded `127.0.0.1`.
async fn bootstrap_servers() -> String {
    let container = BROKER
        .get_or_init(|| async {
            let broker: ContainerRequest<Kafka> = Kafka::default().into();
            test_support::containers::ensure_image(&broker.descriptor()).await;
            broker
                .start()
                .await
                .expect("failed to start the Kafka test container")
        })
        .await;

    let port = container
        .get_host_port_ipv4(KAFKA_PORT)
        .await
        .expect("failed to resolve the mapped Kafka port");

    format!("127.0.0.1:{port}")
}

/// Synchronously creates each topic (partitions=1, RF=1) and waits for the broker to
/// confirm, so a freshly built consumer can subscribe without racing auto-creation.
async fn create_topics(brokers: &str, topics: &[&str]) {
    let admin: AdminClient<DefaultClientContext> = ClientConfig::new()
        .set("bootstrap.servers", brokers)
        .create()
        .expect("failed to build the Kafka AdminClient");

    let new_topics: Vec<NewTopic> = topics
        .iter()
        .map(|name| NewTopic::new(name, 1, TopicReplication::Fixed(1)))
        .collect();

    // operation_timeout makes the broker confirm creation before the future resolves,
    // turning this into a true synchronous barrier rather than a fire-and-forget request.
    let opts = AdminOptions::new().operation_timeout(Some(Duration::from_secs(10)));

    let results = admin
        .create_topics(&new_topics, &opts)
        .await
        .expect("the create_topics request failed");

    for result in results {
        if let Err((topic, code)) = result {
            panic!("failed to create topic '{topic}': {code}");
        }
    }
}

/// A fully isolated broker namespace for one test: a unique source topic, its `.dlq`
/// sibling, and a consumer group, all sharing a UUIDv7 suffix.
///
/// Build one with [`TestContext::new`]; both topics are already created by the time it
/// returns. The factory methods hand out cleanly configured transport handles wired to
/// this namespace.
pub struct TestContext {
    /// Bootstrap `host:port` of the shared broker.
    pub brokers: String,
    /// Source topic the consumer-under-test reads from.
    pub topic: String,
    /// Dead-letter topic (`{topic}.dlq`) the runner evacuates poison/rejected records to.
    pub dlq_topic: String,
    /// Consumer group the runner commits offsets under.
    pub group_id: String,
}

impl TestContext {
    /// Boots the shared broker (first call only), mints a unique namespace, and
    /// pre-creates both the source and dead-letter topics before returning.
    pub async fn new() -> Self {
        let brokers = bootstrap_servers().await;

        // v7 is time-ordered and collision-free, giving readable yet unique names.
        let suffix = Uuid::now_v7().simple().to_string();
        let topic = format!("it-{suffix}");
        let dlq_topic = format!("{topic}{DLQ_SUFFIX}");
        let group_id = format!("it-grp-{suffix}");

        create_topics(&brokers, &[&topic, &dlq_topic]).await;

        Self {
            brokers,
            topic,
            dlq_topic,
            group_id,
        }
    }

    /// A healthy producer aimed at this context's broker — used to seed the source topic.
    pub fn producer(&self) -> KafkaProducerHandle {
        let config = ProducerConfig::new(KafkaClientConfig::new(&self.brokers));
        KafkaProducerBuilder::new(config)
            .build()
            .expect("failed to build the Kafka producer")
    }

    /// A deliberately broken producer aimed at an unroutable broker with a short produce
    /// deadline. Hand this to `run_consumer` as the *dead-letter* producer in Scenario J:
    /// every `publish_raw` fails fast, so the runner must return `Err` and withhold the
    /// commit — the at-least-once "failed evacuation ⇒ no offset advance" guarantee.
    pub fn broken_producer(&self) -> KafkaProducerHandle {
        let mut config = ProducerConfig::new(KafkaClientConfig::new(UNROUTABLE_BROKER));
        config.message_timeout_ms = Some(BROKEN_PRODUCER_TIMEOUT_MS);
        KafkaProducerBuilder::new(config)
            .build()
            .expect("failed to build the broken Kafka producer")
    }

    /// The consumer-under-test: subscribed to the source topic, committing under this
    /// context's group, reading from `earliest`, with auto-commit disabled (the runner
    /// owns every commit). This is the handle a scenario passes to `run_consumer`.
    pub fn consumer(&self) -> KafkaConsumerHandle {
        let mut config = ConsumerConfig::new(KafkaClientConfig::new(&self.brokers), &self.group_id);
        config.auto_offset_reset = AutoOffsetReset::Earliest;
        KafkaConsumerBuilder::new(config)
            .subscribe(&self.topic)
            .build()
            .expect("failed to build the Kafka consumer")
    }

    /// A read-only consumer on the dead-letter topic, in its own group so each call reads
    /// the `.dlq` from the beginning. Used to assert evacuated records and their
    /// `x-dlq-*` diagnostic headers. See [`TestContext::next_dlq_record`].
    pub fn dlq_consumer(&self) -> KafkaConsumerHandle {
        let group = format!("{}-dlq-reader", self.group_id);
        let mut config = ConsumerConfig::new(KafkaClientConfig::new(&self.brokers), group);
        config.auto_offset_reset = AutoOffsetReset::Earliest;
        KafkaConsumerBuilder::new(config)
            .subscribe(&self.dlq_topic)
            .build()
            .expect("failed to build the DLQ consumer")
    }

    /// Awaits the next record on the dead-letter topic, up to `timeout`.
    ///
    /// Decoded as a JSON `Value` for convenience, but the typed `payload` may be `Err`
    /// for a non-JSON poison record — assertions should read `raw_payload`, `key`, and
    /// `headers` (which carry the `x-dlq-*` diagnostics), all of which are populated
    /// regardless of decode success. Returns `None` if nothing arrives in time.
    pub async fn next_dlq_record(
        &self,
        timeout: Duration,
    ) -> Option<ConsumedMessage<serde_json::Value>> {
        let handle = self.dlq_consumer();
        let mut stream = handle.stream::<serde_json::Value>();
        match tokio::time::timeout(timeout, stream.next()).await {
            Ok(Some(Ok(msg))) => Some(msg),
            // Timed out, stream ended, or a broker-level stream error — all "no record".
            _ => None,
        }
    }

    /// The committed offset for `partition` under this context's group, or `None` if the
    /// group has not yet committed anything there.
    ///
    /// Queries the group coordinator with a throwaway `BaseConsumer` (off the runtime via
    /// `spawn_blocking`, since the rdkafka call is blocking). Compose with [`await_until`]
    /// to assert "the offset advanced to N" — or, inverted, "the offset never reached N".
    pub async fn committed_offset(&self, partition: i32) -> Option<i64> {
        let brokers = self.brokers.clone();
        let group_id = self.group_id.clone();
        let topic = self.topic.clone();

        tokio::task::spawn_blocking(move || {
            let consumer: BaseConsumer = ClientConfig::new()
                .set("bootstrap.servers", &brokers)
                .set("group.id", &group_id)
                .set("enable.auto.commit", "false")
                .create()
                .expect("failed to build the offset-probe consumer");

            let mut tpl = TopicPartitionList::new();
            tpl.add_partition(&topic, partition);

            let committed = consumer
                .committed_offsets(tpl, Duration::from_secs(5))
                .ok()?;

            match committed.find_partition(&topic, partition)?.offset() {
                Offset::Offset(offset) => Some(offset),
                // Invalid / Beginning / End ⇒ no concrete commit recorded yet.
                _ => None,
            }
        })
        .await
        .expect("the offset-probe task panicked")
    }
}

/// Polls `check` until it returns `Some(value)` or `timeout` elapses, sleeping `interval`
/// between attempts. Returns the value, or `None` on timeout.
///
/// This is the harness's single anti-flakiness primitive. Every assertion about an
/// asynchronous side-effect — a committed offset advancing, a record landing on the DLQ
/// — is phrased as a predicate polled to satisfaction, never as a guessed `sleep`. The
/// predicate is evaluated at least once even when `timeout` is zero.
pub async fn await_until<F, Fut, T>(
    timeout: Duration,
    interval: Duration,
    mut check: F,
) -> Option<T>
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = Option<T>>,
{
    let deadline = Instant::now() + timeout;
    loop {
        if let Some(value) = check().await {
            return Some(value);
        }
        if Instant::now() >= deadline {
            return None;
        }
        sleep(interval).await;
    }
}
