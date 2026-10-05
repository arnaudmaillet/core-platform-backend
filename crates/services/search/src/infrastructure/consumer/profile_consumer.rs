use std::sync::Arc;

use chrono::Utc;
use cqrs::Envelope;
use tracing::{error, info};
use transport::kafka::consumer::{KafkaConsumerHandle, ProcessOutcome, RetryPolicy, run_consumer};
use transport::kafka::producer::KafkaProducerHandle;
use uuid::Uuid;

use crate::application::command::ProjectionHandler;
use crate::infrastructure::decode::{Decoded, ProfileWireEvent, map_profile};
use crate::infrastructure::hydrate::SourceHydrator;

/// Runs the `profile.v1.events` consumer. A content change (create / update /
/// handle-change / verify) is hydrated (gRPC `GetProfileById`) into a full snapshot
/// before projection; a delete projects directly; an owner-masking flip sets the
/// owner-authority visibility without a fetch.
pub async fn run_profile_consumer(
    consumer: KafkaConsumerHandle,
    projection: Arc<ProjectionHandler>,
    hydrator: Arc<dyn SourceHydrator>,
    producer: KafkaProducerHandle,
) {
    info!("search profile consumer started");
    let policy = RetryPolicy::default();
    let result = run_consumer::<ProfileWireEvent, _>(&consumer, &producer, &policy, move |event| {
        let projection = Arc::clone(&projection);
        let hydrator = Arc::clone(&hydrator);
        Box::pin(async move { process(projection, hydrator, event).await })
    })
    .await;
    if let Err(e) = result {
        error!(error = %e, "search profile consumer stopped");
    }
}

async fn process(
    projection: Arc<ProjectionHandler>,
    hydrator: Arc<dyn SourceHydrator>,
    event: &ProfileWireEvent,
) -> ProcessOutcome {
    let now = Utc::now();
    let result = match map_profile(event.clone()) {
        Err(e) => Err(e),
        Ok(Decoded::Ready(source_event)) => projection
            .apply(Envelope::new(Uuid::now_v7(), source_event), now)
            .await
            .map(|_| ()),
        Ok(Decoded::NeedsContent(content_ref)) => match hydrator.hydrate(content_ref, now).await {
            Ok(source_event) => projection
                .apply(Envelope::new(Uuid::now_v7(), source_event), now)
                .await
                .map(|_| ()),
            Err(e) => Err(e),
        },
        Ok(Decoded::Ignore) => Ok(()),
    };
    ProcessOutcome::from_result(result)
}
