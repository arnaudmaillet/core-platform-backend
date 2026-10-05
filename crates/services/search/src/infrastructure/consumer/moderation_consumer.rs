use std::sync::Arc;

use chrono::Utc;
use cqrs::Envelope;
use tracing::{error, info};
use transport::kafka::consumer::{KafkaConsumerHandle, ProcessOutcome, RetryPolicy, run_consumer};
use transport::kafka::producer::KafkaProducerHandle;
use uuid::Uuid;

use crate::application::command::ProjectionHandler;
use crate::domain::{EntityKind, ModerationEvent, SourceEvent, VisibilityChange};
use crate::error::SearchError;
use crate::infrastructure::decode::{Decoded, ModerationWireEvent, map_moderation};
use crate::infrastructure::hydrate::SourceHydrator;

/// Runs the `moderation.v1.events` consumer. Visibility transitions are projected
/// directly; a lifted takedown on a post also re-hydrates it (the document may be
/// gone). Every other moderation event is a benign no-op that still commits the
/// offset.
pub async fn run_moderation_consumer(
    consumer: KafkaConsumerHandle,
    projection: Arc<ProjectionHandler>,
    hydrator: Arc<dyn SourceHydrator>,
    producer: KafkaProducerHandle,
) {
    info!("search moderation consumer started");
    let policy = RetryPolicy::default();
    let result =
        run_consumer::<ModerationWireEvent, _>(&consumer, &producer, &policy, move |event| {
            let projection = Arc::clone(&projection);
            let hydrator = Arc::clone(&hydrator);
            Box::pin(async move { ProcessOutcome::from_result(process(&projection, hydrator.as_ref(), event).await) })
        })
        .await;
    if let Err(e) = result {
        error!(error = %e, "search moderation consumer stopped");
    }
}

pub(crate) async fn process(
    projection: &ProjectionHandler,
    hydrator: &dyn SourceHydrator,
    event: &ModerationWireEvent,
) -> Result<(), SearchError> {
    let source_event = match map_moderation(event.clone()) {
        Decoded::Ready(source_event) => source_event,
        // `NeedsContent` never arises for moderation events; treat as a no-op.
        Decoded::NeedsContent(_) | Decoded::Ignore => return Ok(()),
    };
    let reinstated = match &source_event {
        SourceEvent::Moderation(ModerationEvent::VisibilityRestored(VisibilityChange {
            kind: Some(EntityKind::Post),
            id,
            ..
        })) => Some(id.clone()),
        _ => None,
    };
    projection.apply(Envelope::new(Uuid::now_v7(), source_event), Utc::now()).await?;
    // The flag alone does not bring back a document search no longer holds.
    if let Some(post_id) = reinstated {
        let post = hydrator.reinstated_post(&post_id).await?;
        projection.apply(Envelope::new(Uuid::now_v7(), post), Utc::now()).await?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use async_trait::async_trait;
    use chrono::DateTime;
    use error::AppError;

    use super::*;
    use crate::application::fakes::{Fixture, post_event};
    use crate::infrastructure::decode::ContentRef;
    use crate::infrastructure::decode::wire::{EnforcementWire, SubjectWire};

    /// Answers `reinstated_post` with the post as `post` now reads it, or "not
    /// caught up yet".
    struct Source {
        converged: bool,
    }

    #[async_trait]
    impl SourceHydrator for Source {
        async fn hydrate(&self, _: ContentRef, _: DateTime<Utc>) -> Result<SourceEvent, SearchError> {
            unreachable!("moderation events need no content")
        }
        async fn reinstated_post(&self, post_id: &str) -> Result<SourceEvent, SearchError> {
            if !self.converged {
                return Err(SearchError::SourceNotConverged { id: post_id.to_owned() });
            }
            Ok(post_event(post_id, "acct-1", "back again", 5))
        }
    }

    fn reversal(entity_type: &str, id: &str) -> ModerationWireEvent {
        ModerationWireEvent::EnforcementReversed(EnforcementWire {
            subject: SubjectWire { entity_type: entity_type.to_owned(), entity_id: id.to_owned() },
            action: None,
            occurred_at: Utc::now(),
        })
    }

    #[tokio::test]
    async fn a_lifted_takedown_brings_back_a_post_search_no_longer_holds() {
        let fx = Fixture::new();
        let projection = fx.projection_handler();
        assert!(!fx.index.contains(EntityKind::Post, "post-1"), "dropped during the takedown");

        process(&projection, &Source { converged: true }, &reversal("post", "post-1")).await.unwrap();
        assert!(fx.index.is_visible(EntityKind::Post, "post-1"));
        assert_eq!(fx.index.caption(EntityKind::Post, "post-1").as_deref(), Some("back again"));
    }

    #[tokio::test]
    async fn a_reversal_post_has_not_applied_yet_is_retried() {
        let fx = Fixture::new();
        let err = process(&fx.projection_handler(), &Source { converged: false }, &reversal("post", "post-1"))
            .await
            .unwrap_err();
        assert_eq!(err.error_code(), "SCH-8004");
        assert!(err.is_retryable());
    }

    #[tokio::test]
    async fn only_a_post_reversal_re_hydrates() {
        let fx = Fixture::new();
        // A profile reversal flips its flag only: the source is never asked.
        process(&fx.projection_handler(), &Source { converged: false }, &reversal("profile", "prof-1"))
            .await
            .unwrap();
    }
}
