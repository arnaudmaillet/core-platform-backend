//! `account.v1.events` → engagement: `account_deleted` erases the account's
//! likes (#665, GDPR Art. 17) — who liked goes, the counts stay. Other
//! account events are skipped.

use std::sync::Arc;

use serde::Deserialize;
use transport::kafka::config::client::KafkaClientConfig;
use transport::kafka::config::consumer::{AutoOffsetReset, ConsumerConfig};
use transport::kafka::consumer::builder::KafkaConsumerBuilder;
use transport::kafka::consumer::{run_consumer, ProcessOutcome, RetryPolicy};
use transport::kafka::producer::KafkaProducerHandle;

use crate::application::erasure::LikeEraser;
use crate::infrastructure::worker::build_dlq_producer;

const TOPIC: &str = "account.v1.events";

/// account's `DomainEvent`, internally tagged on `type`, snake_case; only what
/// engagement acts on is read.
#[derive(Debug, Deserialize)]
pub struct AccountEvent {
    #[serde(rename = "type")]
    kind:       String,
    #[serde(default)]
    account_id: String,
}

/// What an event asks of the likes.
#[derive(Debug, PartialEq, Eq)]
enum Outcome {
    Skip,
    Erase(String),
    Poison(String),
}

fn outcome(event: &AccountEvent) -> Outcome {
    if event.kind != "account_deleted" {
        return Outcome::Skip;
    }
    match uuid::Uuid::parse_str(&event.account_id) {
        Ok(account) => Outcome::Erase(account.to_string()),
        Err(_) => Outcome::Poison(format!("account_deleted with a bad account_id {:?}", event.account_id)),
    }
}

pub struct AccountConsumer {
    kafka_config: KafkaClientConfig,
    eraser:       LikeEraser,
    group_id:     String,
}

impl AccountConsumer {
    pub fn new(kafka_config: KafkaClientConfig, eraser: LikeEraser, group_id: impl Into<String>) -> Self {
        Self { kafka_config, eraser, group_id: group_id.into() }
    }

    pub async fn run(self) {
        let producer = match build_dlq_producer(&self.kafka_config) {
            Ok(producer) => producer,
            Err(e) => {
                tracing::error!(error = %e, "failed to build DLQ producer — account consumer not started");
                return;
            }
        };
        let worker = Arc::new(self);
        loop {
            match worker.clone().run_once(&producer).await {
                Ok(()) => tracing::warn!("account consumer exited cleanly — restarting"),
                Err(e) => {
                    tracing::error!(error = %e, "account consumer error — restarting after 5 s");
                    tokio::time::sleep(tokio::time::Duration::from_secs(5)).await;
                }
            }
        }
    }

    async fn run_once(self: Arc<Self>, producer: &KafkaProducerHandle) -> Result<(), String> {
        let mut config = ConsumerConfig::new(self.kafka_config.clone(), &self.group_id);
        config.auto_offset_reset = AutoOffsetReset::Earliest;
        config.enable_auto_commit = false;
        let handle = KafkaConsumerBuilder::new(config).subscribe(TOPIC).build().map_err(|e| e.to_string())?;
        tracing::info!(topic = TOPIC, group = %self.group_id, "account consumer started");
        let policy = RetryPolicy::default();
        run_consumer::<AccountEvent, _>(&handle, producer, &policy, move |event| {
            let worker = Arc::clone(&self);
            Box::pin(async move {
                match outcome(event) {
                    Outcome::Skip => ProcessOutcome::Done,
                    Outcome::Poison(reason) => ProcessOutcome::Reject(reason),
                    Outcome::Erase(account) => {
                        let erased = worker.eraser.erase(&account, chrono::Utc::now().timestamp_micros()).await;
                        if let Ok(targets) = &erased {
                            tracing::info!(targets, "a deleted account's likes erased");
                        }
                        ProcessOutcome::from_result(erased.map(|_| ()))
                    }
                }
            })
        })
        .await
        .map_err(|e| e.to_string())
    }
}

#[cfg(test)]
mod tests {
    use account::domain::event::{AccountDeleted, AccountSuspended, DomainEvent as AccountDomainEvent};
    use chrono::Utc;
    use uuid::Uuid;

    use super::*;

    /// Serialized with account's own types, read back as the consumer does.
    fn wire(event: AccountDomainEvent) -> AccountEvent {
        serde_json::from_slice(&serde_json::to_vec(&event).unwrap()).unwrap()
    }

    #[test]
    fn account_deleted_erases_the_likes_other_events_skip() {
        let id = account::domain::value_object::AccountId::new();
        let deleted = wire(AccountDomainEvent::AccountDeleted(AccountDeleted {
            account_id: id,
            deleted_by: None,
            occurred_at: Utc::now(),
            correlation_id: Uuid::now_v7(),
        }));
        assert_eq!(outcome(&deleted), Outcome::Erase(id.to_string()));
        let suspended = wire(AccountDomainEvent::AccountSuspended(AccountSuspended {
            account_id: id,
            reason: "spam".into(),
            occurred_at: Utc::now(),
            correlation_id: Uuid::now_v7(),
        }));
        assert_eq!(outcome(&suspended), Outcome::Skip);
        let bad: AccountEvent = serde_json::from_str(r#"{"type":"account_deleted","account_id":"nope"}"#).unwrap();
        assert!(matches!(outcome(&bad), Outcome::Poison(_)));
    }
}
