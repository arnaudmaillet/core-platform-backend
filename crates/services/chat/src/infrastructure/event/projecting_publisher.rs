use std::sync::Arc;

use async_trait::async_trait;

use crate::application::command::InboxProjector;
use crate::application::port::EventPublisher;
use crate::domain::event::{DomainEvent, MessageEvent};
use crate::error::ChatError;

/// Without a broker, chat's own facts reach the inbox inline (#656): the
/// [`InboxWorker`](crate::infrastructure::worker::InboxWorker) does it from
/// Kafka otherwise. The fact is already durable, so a projection failure is
/// logged, never returned.
pub struct ProjectingPublisher<P> {
    inner:     P,
    projector: Arc<InboxProjector>,
}

impl<P> ProjectingPublisher<P> {
    pub fn new(inner: P, projector: Arc<InboxProjector>) -> Self {
        Self { inner, projector }
    }
}

#[async_trait]
impl<P: EventPublisher> EventPublisher for ProjectingPublisher<P> {
    async fn publish_conversation(&self, event: &DomainEvent) -> Result<(), ChatError> {
        self.inner.publish_conversation(event).await?;
        if let Err(error) = self.projector.on_conversation(event).await {
            tracing::warn!(%error, "inbox projection failed (conversation event)");
        }
        Ok(())
    }

    async fn publish_message(&self, event: &MessageEvent) -> Result<(), ChatError> {
        self.inner.publish_message(event).await?;
        let MessageEvent::Sent(sent) = event;
        if let Err(error) = self.projector.on_message(sent).await {
            tracing::warn!(%error, "inbox projection failed (message)");
        }
        Ok(())
    }
}
