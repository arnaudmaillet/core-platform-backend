//! Feeds the discovery pool and the interest tags (#662) from five streams,
//! one consumer group:
//!
//! - `post.v1.events` (post's `DomainEvent`, tagged `type`, PascalCase):
//!   `PostPublished` pools a post with its caption's hashtags, `PostDeleted`
//!   drops it for good;
//! - `moderation.v1.events` (moderation's `DomainEvent`, tagged `type`,
//!   snake_case): `enforcement_applied` with a content action on a `post`
//!   restricts it (`remove_content` / `visibility_limit` hide it, `age_gate`
//!   keeps it for STANDARD readers only), `enforcement_reversed` lifts it;
//! - `counter.v1.popularity` (untagged `{entity_type, entity_id, score}`): a
//!   post's all-time popularity, which drives its hot score;
//! - `engagement.reactions` (tagged `event_type`, snake_case): a new reaction
//!   (`upserted` without an `old_kind`) teaches the reactor the post's tags; a
//!   changed or removed one teaches nothing;
//! - `profile.v1.events` (tagged `type`): `ProfileDeleted` erases the
//!   profile's interests.
//!
//! Told apart by shape. Every pool write is idempotent and the moderation one
//! is version-guarded, so redelivery and cross-topic reordering converge.

use std::sync::Arc;

use serde::Deserialize;
use transport::kafka::config::client::KafkaClientConfig;
use transport::kafka::config::consumer::{AutoOffsetReset, ConsumerConfig};
use transport::kafka::consumer::builder::KafkaConsumerBuilder;
use transport::kafka::consumer::{run_consumer, ProcessOutcome, RetryPolicy};
use transport::kafka::producer::KafkaProducerHandle;
use uuid::Uuid;

use cqrs::{CommandBus, Envelope};

use crate::application::command::apply_discovery_signal::{ApplyDiscoverySignalCommand, DiscoverySignal};
use crate::domain::value_object::interest::hashtags;
use crate::domain::value_object::Restriction;
use crate::infrastructure::worker::{build_dlq_producer, dispatch_outcome};

const TOPIC_POST: &str = "post.v1.events";
const TOPIC_MODERATION: &str = "moderation.v1.events";
const TOPIC_POPULARITY: &str = "counter.v1.popularity";
const TOPIC_REACTIONS: &str = "engagement.reactions";
const TOPIC_PROFILE: &str = "profile.v1.events";

/// A lenient superset of the five payloads.
#[derive(Debug, Deserialize)]
pub struct DiscoveryEvent {
    #[serde(rename = "type", default)]
    event_type:      Option<String>,
    // post.v1.events
    #[serde(default)]
    post_id:         Option<String>,
    #[serde(default)]
    profile_id:      Option<String>,
    #[serde(default)]
    published_at_ms: Option<i64>,
    #[serde(default)]
    caption:         Option<String>,
    // moderation.v1.events
    #[serde(default)]
    subject:         Option<Subject>,
    #[serde(default)]
    action:          Option<String>,
    #[serde(default)]
    version:         Option<i64>,
    // counter.v1.popularity
    #[serde(default)]
    entity_type:     Option<String>,
    #[serde(default)]
    entity_id:       Option<String>,
    #[serde(default)]
    score:           Option<f64>,
    // engagement.reactions (`post_id`, `profile_id` = the reactor)
    #[serde(rename = "event_type", default)]
    reaction:        Option<String>,
    #[serde(default)]
    old_kind:        Option<String>,
    #[serde(default)]
    event_at_ms:     Option<i64>,
}

#[derive(Debug, Deserialize)]
struct Subject {
    entity_type: String,
    entity_id:   String,
}

/// What an event asks the pool to do.
#[derive(Debug, PartialEq)]
enum Outcome {
    Skip,
    Apply(DiscoverySignal),
    Poison(String),
}

fn outcome(event: &DiscoveryEvent) -> Outcome {
    if let Some(reaction_type) = event.reaction.as_deref() {
        return reaction(reaction_type, event);
    }
    let Some(event_type) = event.event_type.as_deref() else {
        return popularity(event);
    };
    match event_type {
        "PostPublished" => match (&event.post_id, &event.profile_id, event.published_at_ms) {
            (Some(post_id), Some(author_id), Some(published_at_ms)) if published_at_ms > 0 => {
                Outcome::Apply(DiscoverySignal::Published {
                    post_id:   post_id.clone(),
                    author_id: author_id.clone(),
                    published_at_ms,
                    tags:      hashtags(event.caption.as_deref().unwrap_or_default()),
                })
            }
            _ => Outcome::Poison("PostPublished without post_id, profile_id or published_at_ms".into()),
        },
        "PostDeleted" => match &event.post_id {
            Some(post_id) => Outcome::Apply(DiscoverySignal::Deleted { post_id: post_id.clone() }),
            None => Outcome::Poison("PostDeleted without a post_id".into()),
        },
        "enforcement_applied" | "enforcement_reversed" => moderation(event_type, event),
        "ProfileDeleted" => match &event.profile_id {
            Some(profile_id) => Outcome::Apply(DiscoverySignal::ProfileErased { profile_id: profile_id.clone() }),
            None => Outcome::Poison("ProfileDeleted without a profile_id".into()),
        },
        _ => Outcome::Skip,
    }
}

/// A first reaction to a post teaches its tags; a kind changed (an
/// `old_kind`) or a reaction removed teaches nothing.
fn reaction(reaction_type: &str, event: &DiscoveryEvent) -> Outcome {
    if reaction_type != "upserted" || event.old_kind.is_some() {
        return Outcome::Skip;
    }
    match (&event.post_id, &event.profile_id, event.event_at_ms) {
        (Some(post_id), Some(profile_id), Some(at_ms)) => Outcome::Apply(DiscoverySignal::Reacted {
            post_id:    post_id.clone(),
            profile_id: profile_id.clone(),
            at_ms,
        }),
        _ => Outcome::Poison("reaction upserted without post_id, profile_id or event_at_ms".into()),
    }
}

fn moderation(event_type: &str, event: &DiscoveryEvent) -> Outcome {
    let restriction = if event_type == "enforcement_reversed" {
        Restriction::None
    } else {
        match event.action.as_deref().and_then(Restriction::for_action) {
            Some(restriction) => restriction,
            None => return Outcome::Skip, // actor-level action or no_action
        }
    };
    let Some(subject) = &event.subject else {
        return Outcome::Poison(format!("{event_type} without a subject"));
    };
    if subject.entity_type != "post" {
        return Outcome::Skip;
    }
    let Some(version) = event.version else {
        return Outcome::Poison(format!("{event_type} without a version"));
    };
    Outcome::Apply(DiscoverySignal::Restricted { post_id: subject.entity_id.clone(), restriction, version })
}

fn popularity(event: &DiscoveryEvent) -> Outcome {
    match (event.entity_type.as_deref(), &event.entity_id, event.score) {
        (Some("post"), Some(post_id), Some(score)) => {
            Outcome::Apply(DiscoverySignal::Popularity { post_id: post_id.clone(), score })
        }
        (Some(_), Some(_), Some(_)) => Outcome::Skip, // another entity kind
        _ => Outcome::Poison("untagged event that is not a popularity signal".into()),
    }
}

pub struct DiscoveryWorker<CB> {
    kafka_config: KafkaClientConfig,
    command_bus:  Arc<CB>,
    group_id:     String,
}

impl<CB: CommandBus + 'static> DiscoveryWorker<CB> {
    pub fn new(kafka_config: KafkaClientConfig, command_bus: Arc<CB>, group_id: impl Into<String>) -> Self {
        Self { kafka_config, command_bus, group_id: group_id.into() }
    }

    pub async fn run(self) {
        let producer = match build_dlq_producer(&self.kafka_config) {
            Ok(producer) => producer,
            Err(e) => {
                tracing::error!(error = %e, "failed to build DLQ producer — discovery consumer not started");
                return;
            }
        };
        let worker = Arc::new(self);
        loop {
            match worker.clone().run_once(&producer).await {
                Ok(()) => tracing::warn!("discovery consumer exited cleanly — restarting"),
                Err(e) => {
                    tracing::error!(error = %e, "discovery consumer error — restarting after 5 s");
                    tokio::time::sleep(tokio::time::Duration::from_secs(5)).await;
                }
            }
        }
    }

    async fn run_once(self: Arc<Self>, producer: &KafkaProducerHandle) -> Result<(), String> {
        let mut config = ConsumerConfig::new(self.kafka_config.clone(), &self.group_id);
        config.auto_offset_reset  = AutoOffsetReset::Earliest;
        config.enable_auto_commit = false;

        let handle = KafkaConsumerBuilder::new(config)
            .subscribe_many([TOPIC_POST, TOPIC_MODERATION, TOPIC_POPULARITY, TOPIC_REACTIONS, TOPIC_PROFILE])
            .build()
            .map_err(|e| e.to_string())?;
        tracing::info!(group = %self.group_id, "discovery consumer started");

        let policy = RetryPolicy::default();
        run_consumer::<DiscoveryEvent, _>(&handle, producer, &policy, move |event| {
            let worker = Arc::clone(&self);
            Box::pin(async move { worker.process(event).await })
        })
        .await
        .map_err(|e| e.to_string())
    }

    async fn process(&self, event: &DiscoveryEvent) -> ProcessOutcome {
        let signal = match outcome(event) {
            Outcome::Skip => return ProcessOutcome::Done,
            Outcome::Poison(reason) => return ProcessOutcome::Reject(reason),
            Outcome::Apply(signal) => signal,
        };
        let command = ApplyDiscoverySignalCommand { signal };
        dispatch_outcome(self.command_bus.dispatch(Envelope::new(Uuid::now_v7(), command)).await)
    }
}

#[cfg(test)]
mod tests {
    use chrono::Utc;
    use moderation::domain::event::{DomainEvent as ModerationEvent, EnforcementApplied, EnforcementReversed};
    use moderation::domain::value_object::{
        ActionType, ActorId, EnforcementId, EnforcementVersion, EntityType, SubjectRef,
    };
    use post::domain::event::{DomainEvent as PostEvent, PostDeletedEvent, PostPublishedEvent, PostUpdatedEvent};

    use super::*;

    const POST: &str = "0190f0a0-0000-7000-8000-000000000001";
    const AUTHOR: &str = "0190f0a0-0000-7000-8000-0000000000aa";

    /// Serialized by the producer's own types, read back the way the consumer
    /// does: the wire contract, not a hand-written fixture.
    fn wire(bytes: Vec<u8>) -> DiscoveryEvent {
        serde_json::from_slice(&bytes).unwrap()
    }

    fn published() -> PostEvent {
        PostEvent::PostPublished(PostPublishedEvent {
            post_id:         POST.into(),
            profile_id:      AUTHOR.into(),
            kind:            "image".into(),
            published_at_ms: 1_760_000_000_000,
            author_tier:     0,
            audio_id:        None,
            audio_kind:      None,
            caption:         "Sunset #Paris #paris #golden_hour".into(),
            thumbnail_url:   None,
            lat:             None,
            lng:             None,
        })
    }

    fn applied(entity_type: EntityType, action: ActionType, version: i64) -> DiscoveryEvent {
        wire(serde_json::to_vec(&ModerationEvent::EnforcementApplied(EnforcementApplied {
            enforcement_id: EnforcementId::new(),
            subject: SubjectRef::new(entity_type, POST, ActorId::from_uuid(Uuid::now_v7()), "feed").unwrap(),
            actor_id: ActorId::from_uuid(Uuid::now_v7()),
            action,
            version: EnforcementVersion::from_i64(version),
            applied_at: Utc::now(),
            expires_at: None,
            occurred_at: Utc::now(),
            correlation_id: Uuid::now_v7(),
        }))
        .unwrap())
    }

    #[test]
    fn post_events_pool_and_drop_posts() {
        assert_eq!(
            outcome(&wire(serde_json::to_vec(&published()).unwrap())),
            Outcome::Apply(DiscoverySignal::Published {
                post_id:         POST.into(),
                author_id:       AUTHOR.into(),
                published_at_ms: 1_760_000_000_000,
                tags:            vec!["paris".into(), "golden_hour".into()],
            })
        );
        let deleted = PostEvent::PostDeleted(PostDeletedEvent {
            post_id: POST.into(), profile_id: AUTHOR.into(), deleted_at_ms: 1,
        });
        assert_eq!(
            outcome(&wire(serde_json::to_vec(&deleted).unwrap())),
            Outcome::Apply(DiscoverySignal::Deleted { post_id: POST.into() })
        );
        let updated = PostEvent::PostUpdated(PostUpdatedEvent {
            post_id: POST.into(), profile_id: AUTHOR.into(), updated_at_ms: 1,
        });
        assert_eq!(outcome(&wire(serde_json::to_vec(&updated).unwrap())), Outcome::Skip);
    }

    #[test]
    fn content_actions_restrict_reversals_lift_actor_actions_skip() {
        for (action, restriction) in [
            (ActionType::RemoveContent, Restriction::Removed),
            (ActionType::VisibilityLimit, Restriction::Limited),
            (ActionType::AgeGate, Restriction::AgeGated),
        ] {
            assert_eq!(
                outcome(&applied(EntityType::Post, action, 3)),
                Outcome::Apply(DiscoverySignal::Restricted { post_id: POST.into(), restriction, version: 3 })
            );
        }
        let reversed = wire(serde_json::to_vec(&ModerationEvent::EnforcementReversed(EnforcementReversed {
            enforcement_id: EnforcementId::new(),
            subject: SubjectRef::new(EntityType::Post, POST, ActorId::from_uuid(Uuid::now_v7()), "").unwrap(),
            actor_id: ActorId::from_uuid(Uuid::now_v7()),
            version: EnforcementVersion::from_i64(4),
            occurred_at: Utc::now(),
            correlation_id: Uuid::now_v7(),
        }))
        .unwrap());
        assert_eq!(
            outcome(&reversed),
            Outcome::Apply(DiscoverySignal::Restricted { post_id: POST.into(), restriction: Restriction::None, version: 4 })
        );
        assert_eq!(outcome(&applied(EntityType::Post, ActionType::Warn, 5)), Outcome::Skip);
        assert_eq!(outcome(&applied(EntityType::Comment, ActionType::RemoveContent, 5)), Outcome::Skip);
    }

    #[test]
    fn popularity_signals_drive_the_hot_score() {
        let event = counter::infrastructure::PopularityEvent {
            entity_type: "post".into(),
            entity_id:   POST.into(),
            score:       12.5,
        };
        assert_eq!(
            outcome(&wire(serde_json::to_vec(&event).unwrap())),
            Outcome::Apply(DiscoverySignal::Popularity { post_id: POST.into(), score: 12.5 })
        );
        let profile = counter::infrastructure::PopularityEvent {
            entity_type: "profile".into(),
            entity_id:   AUTHOR.into(),
            score:       3.0,
        };
        assert_eq!(outcome(&wire(serde_json::to_vec(&profile).unwrap())), Outcome::Skip);
        assert!(matches!(outcome(&wire(b"{}".to_vec())), Outcome::Poison(_)));
    }

    #[test]
    fn a_first_reaction_teaches_the_tags_a_change_or_removal_does_not() {
        use engagement::domain::event::reaction_event::ReactionKafkaEvent;
        use engagement::domain::event::{ReactionRemovedEvent, ReactionUpsertedEvent};
        use engagement::domain::value_object::ReactionKind;

        const READER: &str = "0190f0a0-0000-7000-8000-0000000000bb";
        let upserted = |old_kind| {
            wire(serde_json::to_vec(&ReactionKafkaEvent::Upserted(ReactionUpsertedEvent {
                post_id:     POST.into(),
                profile_id:  READER.into(),
                new_kind:    ReactionKind::Fire,
                new_weight:  2,
                old_kind,
                old_weight:  old_kind.map(|_| 1),
                event_at_ms: 1_760_000_000_500,
            }))
            .unwrap())
        };
        assert_eq!(
            outcome(&upserted(None)),
            Outcome::Apply(DiscoverySignal::Reacted { post_id: POST.into(), profile_id: READER.into(), at_ms: 1_760_000_000_500 })
        );
        assert_eq!(outcome(&upserted(Some(ReactionKind::Heart))), Outcome::Skip);
        let removed = ReactionKafkaEvent::Removed(ReactionRemovedEvent {
            post_id: POST.into(), profile_id: READER.into(), kind: ReactionKind::Fire, weight: 2, event_at_ms: 1,
        });
        assert_eq!(outcome(&wire(serde_json::to_vec(&removed).unwrap())), Outcome::Skip);
    }

    #[test]
    fn a_deleted_profile_erases_its_interests_other_profile_events_skip() {
        use profile::infrastructure::publisher::wire::ProfileEventWire;

        let deleted = ProfileEventWire::ProfileDeleted { profile_id: AUTHOR.into(), occurred_at_ms: 1 };
        assert_eq!(
            outcome(&wire(serde_json::to_vec(&deleted).unwrap())),
            Outcome::Apply(DiscoverySignal::ProfileErased { profile_id: AUTHOR.into() })
        );
        let updated = ProfileEventWire::ProfileUpdated { profile_id: AUTHOR.into(), occurred_at_ms: 1 };
        assert_eq!(outcome(&wire(serde_json::to_vec(&updated).unwrap())), Outcome::Skip);
    }
}
