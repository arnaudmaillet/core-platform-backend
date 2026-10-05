use std::sync::Arc;

use transport::kafka::consumer::builder::KafkaConsumerBuilder;
use transport::kafka::consumer::{run_consumer, ProcessOutcome, RetryPolicy};
use transport::kafka::config::client::KafkaClientConfig;
use transport::kafka::config::consumer::{AutoOffsetReset, ConsumerConfig};
use transport::kafka::producer::KafkaProducerHandle;

use crate::application::port::ReactionLedger;
use crate::domain::event::reaction_event::ReactionKafkaEvent;
use crate::domain::value_object::{PostId, ProfileId};
use crate::infrastructure::worker::build_dlq_producer;

const TOPIC: &str = "engagement.reactions";

/// Kafka consumer that durably applies reaction events to the ScyllaDB ledger.
///
/// This worker runs as a long-lived background task spawned at service startup.
/// It enforces at-least-once delivery semantics: Kafka offsets are committed
/// explicitly only after the ledger write succeeds (`enable_auto_commit = false`).
/// A failed write leaves the offset uncommitted so the message is redelivered.
///
/// The ledger UPSERT is idempotent (last-write-wins), making redelivery safe.
/// Removal operations are also safe to retry — deleting a non-existent row is
/// a no-op in ScyllaDB.
pub struct ReactionWriteBehindWorker<L> {
    kafka_config: KafkaClientConfig,
    ledger:       Arc<L>,
    group_id:     String,
}

impl<L: ReactionLedger> ReactionWriteBehindWorker<L> {
    pub fn new(kafka_config: KafkaClientConfig, ledger: Arc<L>, group_id: impl Into<String>) -> Self {
        Self {
            kafka_config,
            ledger,
            group_id: group_id.into(),
        }
    }

    /// Runs indefinitely, consuming `engagement.reactions` events and writing
    /// to ScyllaDB. Call this inside `tokio::spawn`.
    pub async fn run(self) {
        let producer = match build_dlq_producer(&self.kafka_config) {
            Ok(producer) => producer,
            Err(e) => {
                tracing::error!(error = %e, "failed to build DLQ producer — reaction write-behind consumer not started");
                return;
            }
        };

        let worker = Arc::new(self);
        loop {
            match worker.clone().run_once(&producer).await {
                Ok(()) => {
                    tracing::warn!("reaction write-behind consumer exited cleanly — restarting");
                }
                Err(e) => {
                    tracing::error!(error = %e, "reaction write-behind consumer error — restarting after 5 s");
                    tokio::time::sleep(tokio::time::Duration::from_secs(5)).await;
                }
            }
        }
    }

    async fn run_once(self: Arc<Self>, producer: &KafkaProducerHandle) -> Result<(), String> {
        let mut config = ConsumerConfig::new(self.kafka_config.clone(), &self.group_id);
        config.auto_offset_reset  = AutoOffsetReset::Earliest;
        config.enable_auto_commit = false;

        let handle = KafkaConsumerBuilder::new(config)
            .subscribe(TOPIC)
            .build()
            .map_err(|e| e.to_string())?;

        tracing::info!(topic = TOPIC, group = %self.group_id, "reaction write-behind consumer started");

        // The ledger UPSERT is idempotent (last-write-wins) and removals are no-ops,
        // so transient failures are safe to retry before dead-lettering.
        let policy = RetryPolicy::default();
        run_consumer::<ReactionKafkaEvent, _>(&handle, producer, &policy, move |event| {
            let worker = Arc::clone(&self);
            Box::pin(async move { ProcessOutcome::from_result(worker.process(event).await) })
        })
        .await
        .map_err(|e| e.to_string())
    }

    async fn process(&self, event: &ReactionKafkaEvent) -> Result<(), crate::error::EngagementError> {
        match event {
            ReactionKafkaEvent::Upserted(e) => {
                let post_id    = PostId::try_from(e.post_id.as_str())?;
                let profile_id = ProfileId::try_from(e.profile_id.as_str())?;

                self.ledger
                    .upsert(&post_id, &profile_id, e.new_kind, e.new_weight, e.event_at_ms)
                    .await?;

                tracing::debug!(
                    post_id    = %post_id,
                    profile_id = %profile_id,
                    kind       = e.new_kind.as_redis_key(),
                    "ledger upsert applied"
                );
            }

            ReactionKafkaEvent::Removed(e) => {
                let post_id    = PostId::try_from(e.post_id.as_str())?;
                let profile_id = ProfileId::try_from(e.profile_id.as_str())?;

                self.ledger.remove(&post_id, &profile_id, e.event_at_ms).await?;

                tracing::debug!(
                    post_id    = %post_id,
                    profile_id = %profile_id,
                    "ledger removal applied"
                );
            }
        }

        Ok(())
    }
}
