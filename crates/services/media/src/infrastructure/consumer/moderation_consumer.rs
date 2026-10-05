use std::sync::Arc;

use chrono::Utc;
use cqrs::Envelope;
use serde::Deserialize;
use tracing::{error, info};
use uuid::Uuid;

use transport::kafka::consumer::{run_consumer, KafkaConsumerHandle, ProcessOutcome, RetryPolicy};
use transport::kafka::producer::KafkaProducerHandle;

use crate::application::command::{ApplyModerationCommand, ApplyModerationHandler, ModerationAction};
use crate::domain::value_object::AssetId;
use crate::error::MediaError;

/// Lenient wire view of `moderation.v1.events` — media owns its own read schema and
/// must not depend on the `moderation` crate (a sideways edge). The serde tag is the
/// snake_case variant name (e.g. `enforcement_applied`).
#[derive(Debug, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
enum ModerationWireEvent {
    EnforcementApplied {
        subject: WireSubject,
        action: String,
        #[serde(default)]
        enforcement_id: Option<serde_json::Value>,
    },
    EnforcementReversed {
        subject: WireSubject,
        #[serde(default)]
        enforcement_id: Option<serde_json::Value>,
    },
    #[serde(other)]
    Other,
}

#[derive(Debug, Deserialize)]
struct WireSubject {
    entity_type: String,
    entity_id: String,
}

/// Runs the moderation consumer: maps an enforcement on a *media* subject into a
/// quarantine (content removal / visibility limit) or a restore (reversal).
pub async fn run_moderation_consumer(
    consumer: KafkaConsumerHandle,
    handler: Arc<ApplyModerationHandler>,
    producer: KafkaProducerHandle,
) {
    info!("media moderation consumer started");
    let policy = RetryPolicy::default();
    let result =
        run_consumer::<ModerationWireEvent, _>(&consumer, &producer, &policy, move |event| {
            let handler = Arc::clone(&handler);
            Box::pin(async move { process(handler.as_ref(), event).await })
        })
        .await;
    if let Err(e) = result {
        error!(error = %e, "media moderation consumer stopped");
    }
}

async fn process(handler: &ApplyModerationHandler, event: &ModerationWireEvent) -> ProcessOutcome {
    let Some((asset_id, action, enforcement_id)) = map(event) else {
        // Not a media enforcement (or unparseable id) — committed no-op.
        return ProcessOutcome::from_result(Ok::<(), MediaError>(()));
    };
    let result = handler
        .handle(
            Envelope::new(Uuid::now_v7(), ApplyModerationCommand { asset_id, action, enforcement_id }),
            Utc::now(),
        )
        .await
        .map(|_| ());
    ProcessOutcome::from_result(result)
}

/// Distills a moderation event into a media action, or `None` to skip.
fn map(event: &ModerationWireEvent) -> Option<(AssetId, ModerationAction, Option<String>)> {
    // The enforcement id, whatever its JSON shape (a string, or a newtype).
    let id_of = |v: &Option<serde_json::Value>| {
        v.as_ref().map(|v| v.as_str().map(str::to_owned).unwrap_or_else(|| v.to_string()))
    };
    match event {
        ModerationWireEvent::EnforcementApplied { subject, action, enforcement_id }
            if subject.entity_type == "media"
                && matches!(action.as_str(), "remove_content" | "visibility_limit") =>
        {
            AssetId::try_from(subject.entity_id.as_str())
                .ok()
                .map(|id| (id, ModerationAction::Quarantine, id_of(enforcement_id)))
        }
        ModerationWireEvent::EnforcementReversed { subject, enforcement_id } if subject.entity_type == "media" => {
            AssetId::try_from(subject.entity_id.as_str())
                .ok()
                .map(|id| (id, ModerationAction::Restore, id_of(enforcement_id)))
        }
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn enforcements_on_media_map_with_their_id() {
        let id = AssetId::new().as_str();
        let applied: ModerationWireEvent = serde_json::from_value(serde_json::json!({
            "type": "enforcement_applied", "action": "remove_content", "enforcement_id": "e-1",
            "subject": { "entity_type": "media", "entity_id": id },
        }))
        .unwrap();
        let (_, action, enforcement) = map(&applied).unwrap();
        assert_eq!((action, enforcement.as_deref()), (ModerationAction::Quarantine, Some("e-1")));

        let reversed: ModerationWireEvent = serde_json::from_value(serde_json::json!({
            "type": "enforcement_reversed", "subject": { "entity_type": "media", "entity_id": id },
        }))
        .unwrap();
        let (_, action, enforcement) = map(&reversed).unwrap();
        assert_eq!((action, enforcement), (ModerationAction::Restore, None));
    }
}
