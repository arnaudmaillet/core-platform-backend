use std::sync::Arc;

use serde::Deserialize;
use tracing::{error, info};
use uuid::Uuid;

use cqrs::{CommandBus, Envelope};
use error::AppError;
use transport::kafka::consumer::{run_consumer, KafkaConsumerHandle, ProcessOutcome, RetryPolicy};
use transport::kafka::producer::KafkaProducerHandle;

use crate::application::command::apply_moderation::ApplyModerationCommand;
use crate::domain::value_object::ModerationRestriction;

/// Lenient read DTO for `moderation.v1.events` (internally tagged on `type`,
/// snake_case — moderation's `DomainEvent`). Only enforcement events on a post
/// are acted on; everything else deserializes and is skipped.
#[derive(Debug, Deserialize)]
struct ModerationV1Event {
    #[serde(rename = "type")]
    event_type: String,
    #[serde(default)]
    subject:    Option<SubjectWire>,
    /// `ActionType`, snake_case. Present on `enforcement_applied` only.
    #[serde(default)]
    action:     Option<String>,
    /// Per-subject `EnforcementVersion`.
    #[serde(default)]
    version:    Option<i64>,
}

#[derive(Debug, Deserialize)]
struct SubjectWire {
    entity_type: String,
    entity_id:   String,
}

/// What an event asks post to do.
#[derive(Debug, PartialEq, Eq)]
enum Outcome {
    /// Not about a post's visibility: commit and move on.
    Skip,
    Apply(ApplyModerationCommand),
    /// Malformed for its type: dead-letter it.
    Poison(String),
}

/// The restriction a content action imposes on a post. Actor-level actions
/// (warn, restrict_actor, suspend, ban) and no_action leave the post as is.
fn restriction_for(action: &str) -> Option<ModerationRestriction> {
    match action {
        "remove_content"   => Some(ModerationRestriction::Removed),
        "visibility_limit" => Some(ModerationRestriction::Limited),
        "age_gate"         => Some(ModerationRestriction::AgeGated),
        _                  => None,
    }
}

fn outcome(event: &ModerationV1Event) -> Outcome {
    let restriction = match event.event_type.as_str() {
        "enforcement_applied" => match event.action.as_deref().and_then(restriction_for) {
            Some(r) => r,
            None => return Outcome::Skip,
        },
        // A reversal lifts the restriction. With one enforcement in force at a
        // time per post, which is how moderation versions a subject, that is the
        // whole state.
        "enforcement_reversed" => ModerationRestriction::None,
        _ => return Outcome::Skip,
    };
    let Some(subject) = &event.subject else {
        return Outcome::Poison(format!("{} without a subject", event.event_type));
    };
    if subject.entity_type != "post" {
        return Outcome::Skip;
    }
    let Some(version) = event.version else {
        return Outcome::Poison(format!("{} without a version", event.event_type));
    };
    Outcome::Apply(ApplyModerationCommand {
        post_id: subject.entity_id.clone(),
        restriction,
        version,
    })
}

/// Runs the moderation-outcome consumer on the shared at-least-once runner.
///
/// Consumes `moderation.v1.events` and records each enforcement applied to or
/// reversed on a post, so post's reads hide a removed post from everyone but its
/// author. The command is version-guarded, so redelivery and reordering converge.
pub async fn run_moderation_consumer<CB>(
    consumer:    KafkaConsumerHandle,
    command_bus: Arc<CB>,
    producer:    KafkaProducerHandle,
) where
    CB: CommandBus + Send + Sync + 'static,
{
    info!("post moderation consumer started");

    let policy = RetryPolicy::default();
    let result = run_consumer::<ModerationV1Event, _>(&consumer, &producer, &policy, move |event| {
        let command_bus = Arc::clone(&command_bus);
        Box::pin(async move { process_event(command_bus.as_ref(), event).await })
    })
    .await;

    if let Err(e) = result {
        error!(error = %e, "post moderation consumer stopped");
    }
}

async fn process_event<CB: CommandBus>(command_bus: &CB, event: &ModerationV1Event) -> ProcessOutcome {
    let command = match outcome(event) {
        Outcome::Skip           => return ProcessOutcome::Done,
        Outcome::Poison(reason) => return ProcessOutcome::Reject(reason),
        Outcome::Apply(command) => command,
    };
    match command_bus.dispatch(Envelope::new(Uuid::now_v7(), command)).await {
        Ok(())                     => ProcessOutcome::Done,
        Err(e) if e.is_retryable() => ProcessOutcome::Retry(e.to_string()),
        Err(e)                     => ProcessOutcome::Reject(e.to_string()),
    }
}

#[cfg(test)]
mod tests {
    use chrono::Utc;
    use moderation::domain::event::{DomainEvent, EnforcementApplied, EnforcementReversed};
    use moderation::domain::value_object::{
        ActionType, ActorId, EnforcementId, EnforcementVersion, EntityType, SubjectRef,
    };

    use super::*;

    const POST: &str = "0190f0a0-0000-7000-8000-000000000001";

    /// Serializes with moderation's own types, then reads it back the way the
    /// consumer does: the wire contract, not a hand-written fixture.
    fn wire(event: DomainEvent) -> ModerationV1Event {
        serde_json::from_slice(&serde_json::to_vec(&event).unwrap()).unwrap()
    }

    fn subject(entity_type: EntityType) -> SubjectRef {
        SubjectRef::new(entity_type, POST, ActorId::from_uuid(Uuid::now_v7()), "feed").unwrap()
    }

    fn applied(entity_type: EntityType, action: ActionType, version: i64) -> ModerationV1Event {
        wire(DomainEvent::EnforcementApplied(EnforcementApplied {
            enforcement_id: EnforcementId::new(),
            subject: subject(entity_type),
            actor_id: ActorId::from_uuid(Uuid::now_v7()),
            action,
            version: EnforcementVersion::from_i64(version),
            applied_at: Utc::now(),
            expires_at: None,
            occurred_at: Utc::now(),
            correlation_id: Uuid::now_v7(),
        }))
    }

    fn apply(restriction: ModerationRestriction, version: i64) -> Outcome {
        Outcome::Apply(ApplyModerationCommand { post_id: POST.into(), restriction, version })
    }

    #[test]
    fn content_actions_on_a_post_become_restrictions() {
        assert_eq!(
            outcome(&applied(EntityType::Post, ActionType::RemoveContent, 3)),
            apply(ModerationRestriction::Removed, 3)
        );
        assert_eq!(
            outcome(&applied(EntityType::Post, ActionType::VisibilityLimit, 1)),
            apply(ModerationRestriction::Limited, 1)
        );
        assert_eq!(
            outcome(&applied(EntityType::Post, ActionType::AgeGate, 1)),
            apply(ModerationRestriction::AgeGated, 1)
        );
    }

    #[test]
    fn a_reversal_lifts_the_restriction() {
        let event = wire(DomainEvent::EnforcementReversed(EnforcementReversed {
            enforcement_id: EnforcementId::new(),
            subject: subject(EntityType::Post),
            actor_id: ActorId::from_uuid(Uuid::now_v7()),
            version: EnforcementVersion::from_i64(4),
            occurred_at: Utc::now(),
            correlation_id: Uuid::now_v7(),
        }));
        assert_eq!(outcome(&event), apply(ModerationRestriction::None, 4));
    }

    #[test]
    fn other_entities_and_actor_level_actions_are_skipped() {
        assert_eq!(outcome(&applied(EntityType::Comment, ActionType::RemoveContent, 1)), Outcome::Skip);
        assert_eq!(outcome(&applied(EntityType::Profile, ActionType::RemoveContent, 1)), Outcome::Skip);
        for action in [ActionType::NoAction, ActionType::Warn, ActionType::Suspend, ActionType::Ban] {
            assert_eq!(outcome(&applied(EntityType::Post, action, 1)), Outcome::Skip, "{action:?}");
        }
    }

    #[test]
    fn non_enforcement_events_are_skipped_and_malformed_ones_dead_lettered() {
        let other: ModerationV1Event =
            serde_json::from_str(r#"{"type":"case_opened","case_id":"c-1"}"#).unwrap();
        assert_eq!(outcome(&other), Outcome::Skip);

        let no_version: ModerationV1Event = serde_json::from_str(
            r#"{"type":"enforcement_reversed","subject":{"entity_type":"post","entity_id":"p-1"}}"#,
        )
        .unwrap();
        assert!(matches!(outcome(&no_version), Outcome::Poison(_)));
    }
}
