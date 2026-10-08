use std::sync::Arc;

use serde::Deserialize;
use transport::kafka::consumer::builder::KafkaConsumerBuilder;
use transport::kafka::consumer::{run_consumer, ProcessOutcome, RetryPolicy};
use transport::kafka::config::client::KafkaClientConfig;
use transport::kafka::config::consumer::{AutoOffsetReset, ConsumerConfig};
use transport::kafka::producer::KafkaProducerHandle;

use crate::application::port::{CounterLedger, ScoreStore};
use crate::domain::value_object::PostId;
use crate::infrastructure::worker::build_dlq_producer;

const TOPIC_CREATED: &str = "comment.created";
const TOPIC_DELETED: &str = "comment.deleted";

/// Minimal projection of comment service events consumed by the engagement service.
/// Unknown fields are silently ignored — the comment service schema is an external contract.
#[derive(Debug, Deserialize)]
pub struct CommentEngagementPayload {
    pub post_id: String,
}

/// Kafka consumer for comment lifecycle events from `services/comment`.
///
/// Drives `engagement:comments:{post_id}` Redis counter and the ScyllaDB
/// `post_interaction_counters.comment_count` column.
///
/// Unlike views/shares (which are batched), each comment event is individually
/// applied to ScyllaDB because comment frequency is orders of magnitude lower.
pub struct CommentEventConsumer<S, L> {
    kafka_config: KafkaClientConfig,
    score_store:  Arc<S>,
    ledger:       Arc<L>,
    group_id:     String,
}

impl<S: ScoreStore, L: CounterLedger> CommentEventConsumer<S, L> {
    pub fn new(
        kafka_config: KafkaClientConfig,
        score_store:  Arc<S>,
        ledger:       Arc<L>,
        group_id:     impl Into<String>,
    ) -> Self {
        Self {
            kafka_config,
            score_store,
            ledger,
            group_id: group_id.into(),
        }
    }

    pub async fn run(self) {
        let producer = match build_dlq_producer(&self.kafka_config) {
            Ok(producer) => producer,
            Err(e) => {
                tracing::error!(error = %e, "failed to build DLQ producer — comment event consumer not started");
                return;
            }
        };

        // The per-message runner is handed only the decoded payload, not the topic,
        // so created (+1) and deleted (-1) are consumed by two single-topic loops
        // with a constant delta each. They share one group id, so committed offsets
        // (and thus exactly-once-per-deploy continuity) are preserved.
        let worker = Arc::new(self);
        tokio::join!(
            worker.clone().run_topic(TOPIC_CREATED, 1, &producer),
            worker.clone().run_topic(TOPIC_DELETED, -1, &producer),
        );
    }

    async fn run_topic(self: Arc<Self>, topic: &'static str, delta: i64, producer: &KafkaProducerHandle) {
        loop {
            match self.clone().run_once(topic, delta, producer).await {
                Ok(()) => {
                    tracing::warn!(topic, "comment event consumer exited cleanly — restarting");
                }
                Err(e) => {
                    tracing::error!(topic, error = %e, "comment event consumer error — restarting after 5 s");
                    tokio::time::sleep(tokio::time::Duration::from_secs(5)).await;
                }
            }
        }
    }

    async fn run_once(
        self: Arc<Self>,
        topic:    &'static str,
        delta:    i64,
        producer: &KafkaProducerHandle,
    ) -> Result<(), String> {
        let mut config = ConsumerConfig::new(self.kafka_config.clone(), &self.group_id);
        config.auto_offset_reset  = AutoOffsetReset::Earliest;
        config.enable_auto_commit = false;

        let handle = KafkaConsumerBuilder::new(config)
            .subscribe(topic)
            .build()
            .map_err(|e| e.to_string())?;

        tracing::info!(topic, group = %self.group_id, "comment event consumer started");

        let policy = RetryPolicy::default();
        run_consumer::<CommentEngagementPayload, _>(&handle, producer, &policy, move |payload| {
            let worker = Arc::clone(&self);
            Box::pin(async move { ProcessOutcome::from_result(worker.apply(delta, payload).await) })
        })
        .await
        .map_err(|e| e.to_string())
    }

    async fn apply(
        &self,
        delta:   i64,
        payload: &CommentEngagementPayload,
    ) -> Result<(), crate::error::EngagementError> {
        let post_id = PostId::try_from(payload.post_id.as_str())?;

        if delta > 0 {
            self.score_store.incr_comment(&post_id).await?;
        } else {
            self.score_store.decr_comment(&post_id).await?;
        }

        self.ledger
            .apply_interaction_delta(&post_id, 0, 0, delta)
            .await?;

        tracing::debug!(post_id = %post_id, delta, "comment count applied");

        Ok(())
    }
}
