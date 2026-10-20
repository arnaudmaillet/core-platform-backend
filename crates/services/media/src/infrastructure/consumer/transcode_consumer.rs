use std::sync::Arc;

use chrono::Utc;
use cqrs::Envelope;
use tracing::{error, info};
use uuid::Uuid;

use transport::kafka::consumer::{run_consumer, KafkaConsumerHandle, ProcessOutcome, RetryPolicy};
use transport::kafka::producer::KafkaProducerHandle;

use crate::application::command::{TranscodeAssetCommand, TranscodeAssetHandler};
use crate::domain::event::DomainEvent;
use crate::domain::value_object::MediaKind;
use crate::error::MediaError;

/// Runs the Plane B **video** transcode consumer (the `media-worker` role) on the
/// shared at-least-once runner. It consumes `media.v1.events` and acts only on
/// `AssetUploaded` for `Video` assets — image uploads are a committed no-op here
/// (they are handled in-process by `media-server`). Uses a distinct consumer group
/// from the image processor so both independently see every event.
pub async fn run_transcode_consumer(
    consumer: KafkaConsumerHandle,
    handler: Arc<TranscodeAssetHandler>,
    producer: KafkaProducerHandle,
) {
    info!("media transcode consumer started");
    let policy = RetryPolicy::default();
    let result = run_consumer::<DomainEvent, _>(&consumer, &producer, &policy, move |event| {
        let handler = Arc::clone(&handler);
        Box::pin(async move { process(handler.as_ref(), event).await })
    })
    .await;
    if let Err(e) = result {
        error!(error = %e, "media transcode consumer stopped");
    }
}

async fn process(handler: &TranscodeAssetHandler, event: &DomainEvent) -> ProcessOutcome {
    match event {
        // Only video finalizations trigger a transcode; everything else (including
        // image uploads) is a committed no-op on this consumer.
        // … and animated avatars' clips, baked into sheets (#945).
        DomainEvent::AssetUploaded(e) if matches!(e.kind, MediaKind::Video | MediaKind::AvatarClip) => {
            let cmd = TranscodeAssetCommand { asset_id: e.asset_id };
            let result = handler
                .handle(Envelope::new(Uuid::now_v7(), cmd), Utc::now())
                .await
                .map(|_| ());
            ProcessOutcome::from_result(result)
        }
        _ => ProcessOutcome::from_result(Ok::<(), MediaError>(())),
    }
}
