pub mod presence_settings_worker;
pub mod visibility_worker;

pub use presence_settings_worker::PresenceSettingsWorker;
pub use visibility_worker::VisibilityWorker;

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
