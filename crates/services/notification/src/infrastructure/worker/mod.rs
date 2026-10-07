pub mod appeal_worker;
pub mod chat_push_worker;
pub mod collapse;
pub mod collapse_flush_worker;
pub mod comment_worker;
pub mod follow_worker;
pub mod mention_worker;
pub mod reaction_worker;

use transport::kafka::config::client::KafkaClientConfig;
use transport::kafka::config::producer::ProducerConfig;
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
