use std::sync::Arc;

use chrono::Utc;
use cqrs::Envelope;
use tracing::{error, info};
use uuid::Uuid;

use transport::kafka::consumer::{run_consumer, KafkaConsumerHandle, ProcessOutcome, RetryPolicy};
use transport::kafka::producer::KafkaProducerHandle;

use crate::application::command::{ProcessAssetCommand, ProcessAssetHandler};
use crate::domain::event::DomainEvent;
use crate::domain::value_object::MediaKind;
use crate::error::MediaError;

/// Runs the Plane B processing consumer on the shared at-least-once runner. It
/// consumes the service's own `media.v1.events` and acts only on `AssetUploaded`
/// (the finalize trigger); every other lifecycle event is a committed no-op.
pub async fn run_process_consumer(
    consumer: KafkaConsumerHandle,
    handler: Arc<ProcessAssetHandler>,
    producer: KafkaProducerHandle,
) {
    info!("media processing consumer started");
    let policy = RetryPolicy::default();
    let result = run_consumer::<DomainEvent, _>(&consumer, &producer, &policy, move |event| {
        let handler = Arc::clone(&handler);
        Box::pin(async move { process(handler.as_ref(), event).await })
    })
    .await;
    if let Err(e) = result {
        error!(error = %e, "media processing consumer stopped");
    }
}

async fn process(handler: &ProcessAssetHandler, event: &DomainEvent) -> ProcessOutcome {
    match event {
        // Image finalizations only; video is transcoded by the dedicated
        // media-worker (a committed no-op here).
        DomainEvent::AssetUploaded(e) if !matches!(e.kind, MediaKind::Video | MediaKind::AvatarClip) => {
            let cmd = ProcessAssetCommand { asset_id: e.asset_id };
            let result = handler
                .handle(Envelope::new(Uuid::now_v7(), cmd), Utc::now())
                .await
                .map(|_| ());
            ProcessOutcome::from_result(result)
        }
        // Not an image processing trigger — commit and move on.
        _ => ProcessOutcome::from_result(Ok::<(), MediaError>(())),
    }
}
