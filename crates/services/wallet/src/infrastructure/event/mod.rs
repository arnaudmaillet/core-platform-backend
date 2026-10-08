//! `wallet.v1.events` publishers: Kafka, or a log line without a broker.

use async_trait::async_trait;
use transport::kafka::envelope::EventEnvelope;
use transport::kafka::producer::KafkaProducerHandle;

use crate::application::port::EventPublisher;
use crate::domain::event::WalletEvent;
use crate::error::WalletError;

pub const TOPIC_WALLET_EVENTS: &str = "wallet.v1.events";

/// Kafka-backed publisher, keyed by the target (its likes stay in order).
pub struct KafkaEventPublisher {
    producer: KafkaProducerHandle,
}

impl KafkaEventPublisher {
    pub fn new(producer: KafkaProducerHandle) -> Self {
        Self { producer }
    }
}

#[async_trait]
impl EventPublisher for KafkaEventPublisher {
    async fn publish(&self, event: &WalletEvent) -> Result<(), WalletError> {
        let envelope = EventEnvelope::new(TOPIC_WALLET_EVENTS, event.key(), event.clone())
            .with_header("event_type", event.event_type().to_owned());
        self.producer.publish(envelope).await.map_err(|e| WalletError::EventPublishFailed(e.to_string()))
    }
}

/// Logs instead of publishing (local runs without a broker).
pub struct LogEventPublisher;

#[async_trait]
impl EventPublisher for LogEventPublisher {
    async fn publish(&self, event: &WalletEvent) -> Result<(), WalletError> {
        tracing::info!(event_type = event.event_type(), key = %event.key(), "wallet event (no broker)");
        Ok(())
    }
}
