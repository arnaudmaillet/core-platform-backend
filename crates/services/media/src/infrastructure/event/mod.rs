//! Event-publication adapters for the `media.v1.events` topic.
//!
//! Every domain event is keyed by `asset_id` so an asset's lifecycle keeps
//! per-partition order — `AssetReady` can never be delivered ahead of the
//! `AssetUploaded` it follows, and `AssetDeleted` is always last. The `event_type`
//! header carries the dotted routing key.

pub mod audit_access_log;
pub mod kafka_event_publisher;
pub mod log_event_publisher;

pub use audit_access_log::{KafkaAccessLog, LogAccessLog};
pub use kafka_event_publisher::KafkaEventPublisher;
pub use log_event_publisher::LogEventPublisher;

/// The single Kafka topic every media domain event is published to.
pub const TOPIC_MEDIA_EVENTS: &str = "media.v1.events";
