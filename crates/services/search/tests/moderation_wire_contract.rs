//! Wire contract with `moderation.v1.events`: decode events serialized by
//! moderation's **own** types, not hand-written fixtures. The decoder used to match
//! `"Post"` / `"RemoveContent"` while moderation emits `"post"` / `"remove_content"`,
//! so no takedown ever reached the index; its unit tests passed on PascalCase
//! fixtures nobody produces.

use chrono::Utc;
use moderation::domain::event::{DomainEvent, EnforcementApplied, EnforcementReversed};
use moderation::domain::value_object::{
    ActionType, ActorId, EnforcementId, EnforcementVersion, EntityType, SubjectRef,
};
use search::domain::EntityKind;
use search::domain::event::{ModerationEvent, SourceEvent};
use search::infrastructure::decode::{Decoded, decode_moderation};
use uuid::Uuid;

fn subject(entity_type: EntityType, id: &str) -> SubjectRef {
    SubjectRef::new(entity_type, id, ActorId::from_uuid(Uuid::now_v7()), "feed").unwrap()
}

fn applied(entity_type: EntityType, action: ActionType) -> Vec<u8> {
    let event = DomainEvent::EnforcementApplied(EnforcementApplied {
        enforcement_id: EnforcementId::new(),
        subject: subject(entity_type, "target-1"),
        actor_id: ActorId::from_uuid(Uuid::now_v7()),
        action,
        version: EnforcementVersion::from_i64(1),
        applied_at: Utc::now(),
        expires_at: None,
        occurred_at: Utc::now(),
        correlation_id: Uuid::now_v7(),
    });
    serde_json::to_vec(&event).unwrap()
}

fn revoked_kind(decoded: Decoded) -> Option<EntityKind> {
    match decoded {
        Decoded::Ready(SourceEvent::Moderation(ModerationEvent::VisibilityRevoked(v))) => v.kind,
        other => panic!("expected a visibility revoke, got {other:?}"),
    }
}

#[test]
fn content_takedowns_from_moderation_revoke_search_visibility() {
    for action in [ActionType::RemoveContent, ActionType::VisibilityLimit] {
        let post = decode_moderation(&applied(EntityType::Post, action)).unwrap();
        assert_eq!(revoked_kind(post), Some(EntityKind::Post), "{action:?} on a post");
        let profile = decode_moderation(&applied(EntityType::Profile, action)).unwrap();
        assert_eq!(revoked_kind(profile), Some(EntityKind::Profile), "{action:?} on a profile");
    }
}

#[test]
fn actor_level_actions_from_moderation_are_ignored() {
    for action in [ActionType::Warn, ActionType::Suspend, ActionType::Ban] {
        let decoded = decode_moderation(&applied(EntityType::Account, action)).unwrap();
        assert_eq!(decoded, Decoded::Ignore, "{action:?}");
    }
}

#[test]
fn a_reversal_from_moderation_restores_search_visibility() {
    let event = DomainEvent::EnforcementReversed(EnforcementReversed {
        enforcement_id: EnforcementId::new(),
        subject: subject(EntityType::Post, "target-1"),
        actor_id: ActorId::from_uuid(Uuid::now_v7()),
        version: EnforcementVersion::from_i64(2),
        occurred_at: Utc::now(),
        correlation_id: Uuid::now_v7(),
    });
    match decode_moderation(&serde_json::to_vec(&event).unwrap()).unwrap() {
        Decoded::Ready(SourceEvent::Moderation(ModerationEvent::VisibilityRestored(v))) => {
            assert_eq!(v.kind, Some(EntityKind::Post));
            assert_eq!(v.id, "target-1");
        }
        other => panic!("expected a visibility restore, got {other:?}"),
    }
}
