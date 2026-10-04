use std::sync::Arc;

use serde::Deserialize;
use tracing::{error, info};

use error::AppError;
use transport::kafka::consumer::{run_consumer, KafkaConsumerHandle, ProcessOutcome, RetryPolicy};
use transport::kafka::producer::KafkaProducerHandle;

use crate::application::port::AuthorLocationStore;
use crate::domain::value_object::{LocationSharing, ProfileId};

/// Lenient read DTO for `profile.v1.events` (the internally-tagged
/// `{"type": ...}` stream). Only `ProfileLocationSettingsChanged` is acted on;
/// all other variants deserialize and are skipped.
#[derive(Debug, Deserialize)]
struct ProfileV1Event {
    #[serde(rename = "type")]
    event_type: String,
    #[serde(default)]
    profile_id: String,
    #[serde(default)]
    ghost:      Option<bool>,
    /// `precise` | `city`.
    #[serde(default)]
    precision:  Option<String>,
}

#[derive(Debug, PartialEq, Eq)]
enum Outcome {
    Skip,
    Record(ProfileId, LocationSharing),
    Poison(String),
}

fn outcome(event: &ProfileV1Event) -> Outcome {
    if event.event_type != "ProfileLocationSettingsChanged" {
        return Outcome::Skip;
    }
    let profile_id = match ProfileId::try_from(event.profile_id.as_str()) {
        Ok(id) => id,
        Err(e) => return Outcome::Poison(e.to_string()),
    };
    let city = match event.precision.as_deref() {
        Some("city") => true,
        Some("precise") => false,
        other => return Outcome::Poison(format!("precision {other:?}")),
    };
    let Some(ghost) = event.ghost else {
        return Outcome::Poison("no ghost flag".into());
    };
    Outcome::Record(profile_id, LocationSharing { ghost, city })
}

/// Runs the author location-sharing projection consumer on the shared
/// at-least-once runner.
///
/// Consumes `profile.v1.events`, upserts `profile_id → sharing` on each
/// `ProfileLocationSettingsChanged`, and commits everything else as a no-op.
/// The upsert is last-writer-wins (per-profile order from the topic key), so
/// redelivery is harmless. `GetPost` reads it to coarsen or drop a post's
/// location for anyone but its author.
pub async fn run_author_location_consumer(
    consumer: KafkaConsumerHandle,
    store: Arc<dyn AuthorLocationStore>,
    producer: KafkaProducerHandle,
) {
    info!("post author-location consumer started");

    let policy = RetryPolicy::default();
    let result = run_consumer::<ProfileV1Event, _>(&consumer, &producer, &policy, move |event| {
        let store = Arc::clone(&store);
        Box::pin(async move { process_event(store.as_ref(), event).await })
    })
    .await;

    if let Err(e) = result {
        error!(error = %e, "post author-location consumer stopped");
    }
}

async fn process_event(store: &dyn AuthorLocationStore, event: &ProfileV1Event) -> ProcessOutcome {
    match outcome(event) {
        Outcome::Skip => ProcessOutcome::Done,
        Outcome::Poison(reason) => ProcessOutcome::Reject(reason),
        Outcome::Record(profile_id, sharing) => match store.set(&profile_id, sharing).await {
            Ok(())                     => ProcessOutcome::Done,
            Err(e) if e.is_retryable() => ProcessOutcome::Retry(e.to_string()),
            Err(e)                     => ProcessOutcome::Reject(e.to_string()),
        },
    }
}

#[cfg(test)]
mod tests {
    use profile::infrastructure::publisher::wire::ProfileEventWire;
    use uuid::Uuid;

    use super::*;

    /// Serialized with profile's own wire type, read back as the consumer does.
    fn wire(event: ProfileEventWire) -> ProfileV1Event {
        serde_json::from_slice(&serde_json::to_vec(&event).unwrap()).unwrap()
    }

    #[test]
    fn location_settings_are_read_from_profiles_own_wire() {
        let id = Uuid::now_v7().to_string();
        let event = wire(ProfileEventWire::ProfileLocationSettingsChanged {
            profile_id: id.clone(),
            ghost: true,
            precision: "city".into(),
            occurred_at_ms: 1,
        });
        assert_eq!(
            outcome(&event),
            Outcome::Record(
                ProfileId::try_from(id.as_str()).unwrap(),
                LocationSharing { ghost: true, city: true },
            ),
        );

        let other = wire(ProfileEventWire::ProfileUpdated { profile_id: id, occurred_at_ms: 1 });
        assert_eq!(outcome(&other), Outcome::Skip);
    }

    #[test]
    fn an_unknown_precision_is_poison_not_precise() {
        let json = r#"{"type":"ProfileLocationSettingsChanged","profile_id":"0192f1b4-7c3e-7000-8000-000000000001","ghost":false,"precision":"street"}"#;
        let event: ProfileV1Event = serde_json::from_str(json).unwrap();
        assert!(matches!(outcome(&event), Outcome::Poison(_)));
    }
}
