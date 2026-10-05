use std::sync::Arc;

use serde::Deserialize;
use transport::kafka::config::client::KafkaClientConfig;
use transport::kafka::config::consumer::{AutoOffsetReset, ConsumerConfig};
use transport::kafka::consumer::builder::KafkaConsumerBuilder;
use transport::kafka::consumer::{run_consumer, ProcessOutcome, RetryPolicy};
use transport::kafka::producer::KafkaProducerHandle;

use crate::application::command::InboxProjector;
use crate::domain::event::{ConversationCreatedEvent, DomainEvent, MemberJoinedEvent, MemberLeftEvent, MessageSentEvent};
use crate::error::ChatError;
use crate::infrastructure::worker::build_dlq_producer;

/// The facts the inbox follows, one topic each.
pub const INBOX_TOPICS: [&str; 4] =
    ["chat.message.sent", "chat.conversation.created", "chat.member.joined", "chat.member.left"];

/// One of the inbox's facts, told apart by its fields (each topic carries
/// one shape; `message_id`, `left_at_ms`, `joined_at_ms`, `owner_id` are
/// each unique to theirs).
#[derive(Debug, Clone, Deserialize)]
#[serde(untagged)]
pub enum InboxFact {
    Message(MessageSentEvent),
    Left(MemberLeftEvent),
    Joined(MemberJoinedEvent),
    Created(ConversationCreatedEvent),
}

/// Keeps every member's inbox (#656) from chat's own topics. Keyed by
/// conversation, so one conversation's facts arrive in order; redelivery is
/// harmless (an entry never moves back). Mandatory consumer runtime: manual
/// commit after a terminal outcome, retry with backoff, DLQ.
pub struct InboxWorker {
    kafka_config: KafkaClientConfig,
    projector:    Arc<InboxProjector>,
    group_id:     String,
}

impl InboxWorker {
    pub fn new(kafka_config: KafkaClientConfig, projector: Arc<InboxProjector>, group_id: impl Into<String>) -> Self {
        Self { kafka_config, projector, group_id: group_id.into() }
    }

    pub async fn run(self) {
        let producer = match build_dlq_producer(&self.kafka_config) {
            Ok(producer) => producer,
            Err(e) => {
                tracing::error!(error = %e, "failed to build DLQ producer — inbox consumer not started");
                return;
            }
        };
        let worker = Arc::new(self);
        loop {
            match worker.clone().run_once(&producer).await {
                Ok(()) => tracing::warn!("inbox consumer exited cleanly — restarting"),
                Err(e) => {
                    tracing::error!(error = %e, "inbox consumer error — restarting after 5 s");
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
            .subscribe_many(INBOX_TOPICS)
            .build()
            .map_err(|e| e.to_string())?;
        tracing::info!(group = %self.group_id, "inbox consumer started");

        let policy = RetryPolicy::default();
        run_consumer::<InboxFact, _>(&handle, producer, &policy, move |fact| {
            let worker = Arc::clone(&self);
            Box::pin(async move { ProcessOutcome::from_result(worker.process(fact).await) })
        })
        .await
        .map_err(|e| e.to_string())
    }

    /// Applies one fact (public so the integration suite drives it without a
    /// broker).
    pub async fn process(&self, fact: &InboxFact) -> Result<(), ChatError> {
        match fact.clone() {
            InboxFact::Message(e) => self.projector.on_message(&e).await,
            InboxFact::Left(e) => self.projector.on_conversation(&DomainEvent::MemberLeft(e)).await,
            InboxFact::Joined(e) => self.projector.on_conversation(&DomainEvent::MemberJoined(e)).await,
            InboxFact::Created(e) => self.projector.on_conversation(&DomainEvent::ConversationCreated(e)).await,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Each topic's payload, as chat's publisher serializes it, decodes as its
    /// own fact — never as another's.
    #[test]
    fn each_topics_payload_decodes_as_its_own_fact() {
        let message = serde_json::to_value(MessageSentEvent {
            conversation_id: "c".into(),
            message_id:      "m".into(),
            sender_id:       "s".into(),
            content_type:    "text".into(),
            body:            "hi".into(),
            media_ref:       None,
            reply_to:        None,
            created_at_ms:   1,
            withheld:        true,
            request:         false,
        })
        .unwrap();
        let joined = serde_json::to_value(MemberJoinedEvent {
            conversation_id: "c".into(),
            profile_id:      "p".into(),
            role:            "member".into(),
            joined_at_ms:    1,
        })
        .unwrap();
        let left = serde_json::to_value(MemberLeftEvent { conversation_id: "c".into(), profile_id: "p".into(), left_at_ms: 1 }).unwrap();
        let created = serde_json::to_value(ConversationCreatedEvent {
            conversation_id: "c".into(),
            kind:            "group".into(),
            visibility:      "private".into(),
            owner_id:        "o".into(),
            created_at_ms:   1,
        })
        .unwrap();
        assert!(matches!(serde_json::from_value(message).unwrap(), InboxFact::Message(m) if m.withheld));
        assert!(matches!(serde_json::from_value(joined).unwrap(), InboxFact::Joined(_)));
        assert!(matches!(serde_json::from_value(left).unwrap(), InboxFact::Left(_)));
        assert!(matches!(serde_json::from_value(created).unwrap(), InboxFact::Created(_)));
    }
}
