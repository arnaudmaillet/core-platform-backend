use async_trait::async_trait;
use serde::Serialize;
use transport::error::TransportError;
use transport::kafka::envelope::EventEnvelope;
use transport::kafka::producer::handle::KafkaProducerHandle;

use crate::application::port::{EventPublisher, MessagePush, MessagePushes};
use crate::domain::event::{DomainEvent, MessageEvent};
use crate::error::ChatError;

const TOPIC_CREATED:      &str = "chat.conversation.created";
const TOPIC_PUBLISHED:    &str = "chat.conversation.published";
const TOPIC_UNPUBLISHED:  &str = "chat.conversation.unpublished";
const TOPIC_MEMBER_JOINED: &str = "chat.member.joined";
const TOPIC_MEMBER_LEFT:  &str = "chat.member.left";
const TOPIC_MESSAGE_SENT: &str = "chat.message.sent";
/// A message's push for the notification service (#654).
const TOPIC_MESSAGE_PUSH: &str = "chat.message.push";

/// Kafka-backed [`EventPublisher`]. Each event family is keyed by
/// `conversation_id` so all events for one conversation land on the same
/// partition and preserve per-conversation ordering. Downstream services
/// (notification fan-out to subscribers, analytics) consume these topics; the
/// chat service itself consumes `chat.conversation.unpublished` to tear down the
/// Audience Plane cluster-wide.
pub struct KafkaEventPublisher {
    producer: KafkaProducerHandle,
}

impl KafkaEventPublisher {
    pub fn new(producer: KafkaProducerHandle) -> Self {
        Self { producer }
    }

    async fn emit<T: Serialize + Send + Sync + 'static>(
        &self,
        topic:           &str,
        conversation_id: &str,
        event_type:      &str,
        payload:         T,
    ) -> Result<(), ChatError> {
        let envelope = EventEnvelope::new(topic, conversation_id.to_owned(), payload)
            .with_header("event_type", event_type.to_owned())
            .with_header("conversation_id", conversation_id.to_owned());
        self.producer.publish(envelope).await.map_err(publish_err)
    }
}

#[async_trait]
impl EventPublisher for KafkaEventPublisher {
    async fn publish_conversation(&self, event: &DomainEvent) -> Result<(), ChatError> {
        match event {
            DomainEvent::ConversationCreated(e) => {
                self.emit(TOPIC_CREATED, &e.conversation_id, "ConversationCreated", e.clone()).await
            }
            DomainEvent::ConversationPublished(e) => {
                self.emit(TOPIC_PUBLISHED, &e.conversation_id, "ConversationPublished", e.clone())
                    .await
            }
            DomainEvent::ConversationUnpublished(e) => {
                self.emit(
                    TOPIC_UNPUBLISHED,
                    &e.conversation_id,
                    "ConversationUnpublished",
                    e.clone(),
                )
                .await
            }
            DomainEvent::MemberJoined(e) => {
                self.emit(TOPIC_MEMBER_JOINED, &e.conversation_id, "MemberJoined", e.clone()).await
            }
            DomainEvent::MemberLeft(e) => {
                self.emit(TOPIC_MEMBER_LEFT, &e.conversation_id, "MemberLeft", e.clone()).await
            }
        }
    }

    async fn publish_message(&self, event: &MessageEvent) -> Result<(), ChatError> {
        match event {
            MessageEvent::Sent(e) => {
                self.emit(TOPIC_MESSAGE_SENT, &e.conversation_id, "MessageSent", e.clone()).await
            }
        }
    }
}

#[async_trait]
impl MessagePushes for KafkaEventPublisher {
    async fn request(&self, push: &MessagePush) -> Result<(), ChatError> {
        self.emit(TOPIC_MESSAGE_PUSH, &push.conversation_id, "MessagePush", push.clone()).await
    }
}

fn publish_err(e: TransportError) -> ChatError {
    ChatError::EventPublishFailed { message: e.to_string() }
}
