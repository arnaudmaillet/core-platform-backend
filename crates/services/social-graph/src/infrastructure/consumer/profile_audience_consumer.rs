use std::sync::Arc;

use serde::Deserialize;
use tracing::{error, info};
use uuid::Uuid;

use cqrs::{CommandBus, Envelope};
use error::AppError;
use transport::kafka::consumer::{run_consumer, KafkaConsumerHandle, ProcessOutcome, RetryPolicy};
use transport::kafka::producer::KafkaProducerHandle;

use crate::application::command::{AudienceFact, RecordProfileAudienceCommand};
use crate::domain::interaction::{InteractionAudience, InteractionPolicy};

/// Lenient read DTO for `profile.v1.events` (internally tagged on `type`,
/// PascalCase — profile's `ProfileEventWire`). Only the audience facts are
/// read; every other event deserializes and is skipped.
#[derive(Debug, Deserialize)]
struct ProfileV1Event {
    #[serde(rename = "type")]
    event_type: String,
    #[serde(default)]
    profile_id: String,
    /// `public` | `private`, on `ProfileVisibilityChanged`.
    #[serde(default)]
    visibility: Option<String>,
    /// Audiences on `ProfileInteractionSettingsChanged`.
    #[serde(default)]
    comments: Option<String>,
    #[serde(default)]
    mentions: Option<String>,
    #[serde(default)]
    messages: Option<String>,
}

/// What an event means for the audience projection, if anything.
fn fact(event: &ProfileV1Event) -> Result<Option<AudienceFact>, String> {
    Ok(Some(match event.event_type.as_str() {
        "ProfileVisibilityChanged" => match event.visibility.as_deref() {
            Some("private") => AudienceFact::Private(true),
            Some("public") => AudienceFact::Private(false),
            other => return Err(format!("ProfileVisibilityChanged with visibility {other:?}")),
        },
        "ProfileHidden" | "ProfileDeleted" => AudienceFact::Hidden(true),
        "ProfileRestored" => AudienceFact::Hidden(false),
        "ProfileInteractionSettingsChanged" => {
            let audience = |field: &Option<String>, name: &str| {
                field
                    .as_deref()
                    .and_then(InteractionAudience::parse)
                    .ok_or_else(|| format!("ProfileInteractionSettingsChanged with {name} {field:?}"))
            };
            AudienceFact::Interaction(InteractionPolicy {
                comments: audience(&event.comments, "comments")?,
                mentions: audience(&event.mentions, "mentions")?,
                messages: audience(&event.messages, "messages")?,
            })
        }
        _ => return Ok(None),
    }))
}

/// Runs the profile-audience projection consumer on the shared at-least-once
/// runner: `profile.v1.events` → `social_graph.profile_audience`, read by the
/// access check. Column upserts, so redelivery is harmless.
pub async fn run_profile_audience_consumer<CB>(
    consumer:    KafkaConsumerHandle,
    command_bus: Arc<CB>,
    producer:    KafkaProducerHandle,
) where
    CB: CommandBus + Send + Sync + 'static,
{
    info!("social-graph profile-audience consumer started");

    let policy = RetryPolicy::default();
    let result = run_consumer::<ProfileV1Event, _>(&consumer, &producer, &policy, move |event| {
        let command_bus = Arc::clone(&command_bus);
        Box::pin(async move { process_event(command_bus.as_ref(), event).await })
    })
    .await;

    if let Err(e) = result {
        error!(error = %e, "social-graph profile-audience consumer stopped");
    }
}

async fn process_event<CB: CommandBus>(command_bus: &CB, event: &ProfileV1Event) -> ProcessOutcome {
    let fact = match fact(event) {
        Ok(Some(fact)) => fact,
        Ok(None) => return ProcessOutcome::Done,
        Err(reason) => return ProcessOutcome::Reject(reason),
    };
    let cmd = RecordProfileAudienceCommand { profile_id: event.profile_id.clone(), fact };
    match command_bus.dispatch(Envelope::new(Uuid::now_v7(), cmd)).await {
        Ok(())                     => ProcessOutcome::Done,
        Err(e) if e.is_retryable() => ProcessOutcome::Retry(e.to_string()),
        Err(e)                     => ProcessOutcome::Reject(e.to_string()),
    }
}

#[cfg(test)]
mod tests {
    use profile::infrastructure::publisher::wire::ProfileEventWire;

    use super::*;

    /// Serializes with profile's own wire type and reads it back the way the
    /// consumer does: the contract, not a hand-written fixture.
    fn wire(event: ProfileEventWire) -> ProfileV1Event {
        serde_json::from_slice(&serde_json::to_vec(&event).unwrap()).unwrap()
    }

    #[test]
    fn audience_facts_are_read_from_profiles_own_wire() {
        let visibility = |v: &str| ProfileEventWire::ProfileVisibilityChanged {
            profile_id: "p-1".into(),
            visibility: v.into(),
            occurred_at_ms: 1,
        };
        assert_eq!(fact(&wire(visibility("private"))), Ok(Some(AudienceFact::Private(true))));
        assert_eq!(fact(&wire(visibility("public"))), Ok(Some(AudienceFact::Private(false))));

        let hidden = ProfileEventWire::ProfileHidden {
            profile_id: "p-1".into(),
            masking_reason: "account_suspended".into(),
            occurred_at_ms: 1,
        };
        assert_eq!(fact(&wire(hidden)), Ok(Some(AudienceFact::Hidden(true))));
        let deleted = ProfileEventWire::ProfileDeleted { profile_id: "p-1".into(), occurred_at_ms: 1 };
        assert_eq!(fact(&wire(deleted)), Ok(Some(AudienceFact::Hidden(true))));
        let restored = ProfileEventWire::ProfileRestored { profile_id: "p-1".into(), occurred_at_ms: 1 };
        assert_eq!(fact(&wire(restored)), Ok(Some(AudienceFact::Hidden(false))));
        let event = wire(ProfileEventWire::ProfileRestored { profile_id: "p-9".into(), occurred_at_ms: 1 });
        assert_eq!(event.profile_id, "p-9");
    }

    #[test]
    fn interaction_settings_are_read_from_profiles_own_wire() {
        let settings = ProfileEventWire::ProfileInteractionSettingsChanged {
            profile_id: "p-1".into(),
            comments: "followers".into(),
            mentions: "mutuals".into(),
            messages: "no_one".into(),
            allow_downloads: false,
            show_like_counts: true,
            occurred_at_ms: 1,
        };
        assert_eq!(
            fact(&wire(settings)),
            Ok(Some(AudienceFact::Interaction(InteractionPolicy {
                comments: InteractionAudience::Followers,
                mentions: InteractionAudience::Mutuals,
                messages: InteractionAudience::NoOne,
            })))
        );
    }

    #[test]
    fn other_profile_events_are_skipped() {
        let updated = ProfileEventWire::ProfileUpdated { profile_id: "p-1".into(), occurred_at_ms: 1 };
        assert_eq!(fact(&wire(updated)), Ok(None));
    }
}
