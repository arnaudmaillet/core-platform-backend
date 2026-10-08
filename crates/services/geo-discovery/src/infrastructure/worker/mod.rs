pub mod country_likes_worker;
pub mod location_settings_worker;
pub mod post_indexer;
pub mod score_updater;
pub mod tile_pruner;
pub mod visibility_worker;

pub use country_likes_worker::CountryLikesWorker;
pub use location_settings_worker::LocationSettingsWorker;
pub use post_indexer::PostIndexerWorker;
pub use score_updater::ScoreUpdaterWorker;
pub use tile_pruner::TilePrunerWorker;
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
