//! `chat.message.push` → APNs (#654): a chat message's push, to the members
//! chat says it reaches (its inbox rules and mutes), each under their own
//! `messages` preference, pause and quiet hours. Nothing is written to the
//! activity feed: a message lives in chat's inbox.
//!
//! Sent **at most once** per message: a Redis claim on its id is taken before
//! sending, so a redelivery (or chat re-asking after a retried projection)
//! sends nothing; a crash mid-send loses the rest, which a push can afford.

use std::sync::Arc;

use chrono::Utc;
use fred::interfaces::KeysInterface;
use futures::StreamExt;
use redis_storage::RedisClient;
use serde::Deserialize;
use transport::kafka::config::client::KafkaClientConfig;
use transport::kafka::config::consumer::{AutoOffsetReset, ConsumerConfig};
use transport::kafka::consumer::builder::KafkaConsumerBuilder;
use transport::kafka::consumer::{run_consumer, ProcessOutcome, RetryPolicy};
use transport::kafka::producer::KafkaProducerHandle;

use crate::application::push_dispatcher::PushDispatcher;
use crate::domain::preferences::PushCategory;
use crate::domain::push_message::{ChatMessage, PushMessage};
use crate::domain::value_object::ProfileId;
use crate::error::NotificationError;
use crate::infrastructure::worker::build_dlq_producer;

pub const TOPIC_CHAT_MESSAGE_PUSH: &str = "chat.message.push";
/// A message older than this is not pushed (a backlog after an outage).
const MAX_AGE_MS: i64 = 24 * 3600 * 1000;
/// Recipients sent to at once (a group has up to 500).
const SENDS_IN_FLIGHT: usize = 16;

/// chat's `MessagePush`, read leniently.
#[derive(Debug, Clone, Deserialize)]
pub struct ChatPushPayload {
    pub message_id:      String,
    pub conversation_id: String,
    pub sender_id:       String,
    #[serde(default)]
    pub content_type:    String,
    #[serde(default)]
    pub preview:         String,
    #[serde(default)]
    pub recipients:      Vec<String>,
    #[serde(default)]
    pub created_at_ms:   i64,
}

pub struct ChatPushWorker {
    kafka_config: KafkaClientConfig,
    redis:        RedisClient,
    dispatcher:   Arc<PushDispatcher>,
    /// How long a message's claim is kept (the dedupe window).
    claim_ttl_secs: u64,
    group_id:     String,
}

impl ChatPushWorker {
    pub fn new(
        kafka_config: KafkaClientConfig,
        redis: RedisClient,
        dispatcher: Arc<PushDispatcher>,
        claim_ttl_secs: u64,
        group_id: impl Into<String>,
    ) -> Self {
        Self { kafka_config, redis, dispatcher, claim_ttl_secs, group_id: group_id.into() }
    }

    pub async fn run(self) {
        let producer = match build_dlq_producer(&self.kafka_config) {
            Ok(producer) => producer,
            Err(e) => {
                tracing::error!(error = %e, "failed to build DLQ producer — chat push consumer not started");
                return;
            }
        };
        let worker = Arc::new(self);
        loop {
            match worker.clone().run_once(&producer).await {
                Ok(()) => tracing::warn!("chat push consumer exited cleanly — restarting"),
                Err(e) => {
                    tracing::error!(error = %e, "chat push consumer error — restarting after 5 s");
                    tokio::time::sleep(tokio::time::Duration::from_secs(5)).await;
                }
            }
        }
    }

    async fn run_once(self: Arc<Self>, producer: &KafkaProducerHandle) -> Result<(), String> {
        let mut config = ConsumerConfig::new(self.kafka_config.clone(), &self.group_id);
        config.auto_offset_reset  = AutoOffsetReset::Latest;
        config.enable_auto_commit = false;

        let handle = KafkaConsumerBuilder::new(config)
            .subscribe_many([TOPIC_CHAT_MESSAGE_PUSH])
            .build()
            .map_err(|e| e.to_string())?;
        tracing::info!(group = %self.group_id, "chat push consumer started");

        let policy = RetryPolicy::default();
        run_consumer::<ChatPushPayload, _>(&handle, producer, &policy, move |event| {
            let worker = Arc::clone(&self);
            Box::pin(async move { ProcessOutcome::from_result(worker.process(event).await.map(|_| ())) })
        })
        .await
        .map_err(|e| e.to_string())
    }

    /// Handles one message's push; returns how many devices took it (public
    /// so the integration suite drives it without a broker).
    pub async fn process(&self, push: &ChatPushPayload) -> Result<usize, NotificationError> {
        if push.recipients.is_empty() || Utc::now().timestamp_millis() - push.created_at_ms > MAX_AGE_MS {
            return Ok(0);
        }
        // Malformed ids are dropped, not retried: nothing better comes of them.
        let Ok(sender) = ProfileId::try_from(push.sender_id.as_str()) else {
            tracing::warn!(message_id = %push.message_id, "chat push with a malformed sender: dropped");
            return Ok(0);
        };
        if !self.claim(&push.message_id).await? {
            return Ok(0);
        }

        let name = self.dispatcher.names.display_name(&sender).await;
        let message = PushMessage::chat_message(ChatMessage {
            message_id:      &push.message_id,
            conversation_id: &push.conversation_id,
            media:           push.content_type == "media",
            preview:         &push.preview,
            sender_name:     name.as_deref(),
        });
        let recipients: Vec<ProfileId> =
            push.recipients.iter().filter_map(|r| ProfileId::try_from(r.as_str()).ok()).collect();
        let message = &message;
        let delivered = futures::stream::iter(recipients)
            .map(|recipient| async move {
                match self.dispatcher.send_to(&recipient, PushCategory::Messages, message).await {
                    Ok(n) => n,
                    Err(error) => {
                        tracing::warn!(%error, recipient = %recipient, "chat push not sent");
                        0
                    }
                }
            })
            .buffer_unordered(SENDS_IN_FLIGHT)
            .fold(0, |sum, n| async move { sum + n })
            .await;
        Ok(delivered)
    }

    /// Takes the message's claim; `false` when it was already taken.
    async fn claim(&self, message_id: &str) -> Result<bool, NotificationError> {
        let key = format!("notification:chatpush:{message_id}");
        let taken: Option<String> = self
            .redis
            .inner
            .set(
                &key,
                "1",
                Some(fred::types::Expiration::EX(self.claim_ttl_secs as i64)),
                Some(fred::types::SetOptions::NX),
                false,
            )
            .await
            .map_err(|e| NotificationError::Redis(redis_storage::RedisStorageError::from(e)))?;
        Ok(taken.is_some())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// chat's own `MessagePush` decodes here.
    #[test]
    fn reads_chats_message_push() {
        let json = serde_json::json!({
            "message_id": "m", "conversation_id": "c", "conversation_kind": "group",
            "sender_id": "s", "content_type": "text", "preview": "salut",
            "recipients": ["a", "b"], "created_at_ms": 1
        });
        let push: ChatPushPayload = serde_json::from_value(json).unwrap();
        assert_eq!((push.recipients.len(), push.preview.as_str()), (2, "salut"));
    }
}
