pub mod discovery_worker;
pub mod follow_created_worker;
pub mod follow_deleted_worker;
pub mod post_deleted_worker;
pub mod post_published_worker;

use cqrs::CqrsError;
use error::AppError;
use transport::kafka::config::client::KafkaClientConfig;
use transport::kafka::config::producer::ProducerConfig;
use transport::kafka::consumer::ProcessOutcome;
use transport::kafka::producer::{KafkaProducerBuilder, KafkaProducerHandle};

/// Builds the Kafka producer that consumer workers use to forward poison and
/// retry-exhausted records to their per-topic dead-letter topics.
pub(crate) fn build_dlq_producer(
    kafka_config: &KafkaClientConfig,
) -> Result<KafkaProducerHandle, String> {
    KafkaProducerBuilder::new(ProducerConfig::new(kafka_config.clone()))
        .build()
        .map_err(|e| e.to_string())
}

/// Maps a command-bus dispatch result to a runner outcome. The bus preserves the
/// handler's `AppError` metadata, so transient (storage) failures are retried and
/// permanent (validation / bad-data) failures are dead-lettered.
pub(crate) fn dispatch_outcome(result: Result<(), CqrsError>) -> ProcessOutcome {
    match result {
        Ok(())                     => ProcessOutcome::Done,
        Err(e) if e.is_retryable() => ProcessOutcome::Retry(e.to_string()),
        Err(e)                     => ProcessOutcome::Reject(e.to_string()),
    }
}
