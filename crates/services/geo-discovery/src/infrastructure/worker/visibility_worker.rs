//! Keeps the map in step with what may still be shown: a deleted post leaves it
//! for good; a post moderation removed or limited leaves it until a newer
//! reversal brings it back.
//!
//! One consumer on two topics with different payloads, told apart by shape:
//! - `post.deleted` (legacy, bare `{post_id, profile_id, deleted_at_ms}`, no tag);
//! - `moderation.v1.events` (moderation's `DomainEvent`, tagged `type`,
//!   snake_case). Only `enforcement_applied` / `enforcement_reversed` on a
//!   `post` matter: `remove_content` / `visibility_limit` / `age_gate` hide (the
//!   map is a discovery surface with no content level, so a limited or
//!   age-gated post leaves it too — no reader's age is known here), a reversal
//!   restores.

use std::sync::Arc;

use serde::Deserialize;
use transport::kafka::config::client::KafkaClientConfig;
use transport::kafka::config::consumer::{AutoOffsetReset, ConsumerConfig};
use transport::kafka::consumer::builder::KafkaConsumerBuilder;
use transport::kafka::consumer::{run_consumer, ProcessOutcome, RetryPolicy};
use transport::kafka::producer::KafkaProducerHandle;

use crate::application::command::{ApplyMapVisibilityCommand, ApplyMapVisibilityHandler};
use crate::application::port::{CardStore, PinStore, SpatialIndex, TileRepository};
use crate::domain::value_object::VisibilityChange;
use crate::infrastructure::worker::build_dlq_producer;

const TOPIC_POST_DELETED: &str = "post.deleted";
const TOPIC_MODERATION: &str = "moderation.v1.events";

/// A lenient superset of both payloads.
#[derive(Debug, Deserialize)]
pub struct VisibilityEvent {
    /// Present on moderation events only.
    #[serde(rename = "type", default)]
    event_type: Option<String>,
    /// Present on `post.deleted` only.
    #[serde(default)]
    post_id:    Option<String>,
    #[serde(default)]
    subject:    Option<Subject>,
    #[serde(default)]
    action:     Option<String>,
    #[serde(default)]
    version:    Option<i64>,
}

#[derive(Debug, Deserialize)]
struct Subject {
    entity_type: String,
    entity_id:   String,
}

/// What an event asks the map to do.
#[derive(Debug, PartialEq, Eq)]
enum Outcome {
    Skip,
    Apply(ApplyMapVisibilityCommand),
    Poison(String),
}

fn outcome(event: &VisibilityEvent) -> Outcome {
    let Some(event_type) = event.event_type.as_deref() else {
        // post.deleted
        return match &event.post_id {
            Some(post_id) => Outcome::Apply(ApplyMapVisibilityCommand {
                post_id: post_id.clone(),
                change:  VisibilityChange::Deleted,
            }),
            None => Outcome::Poison("untagged event without a post_id".into()),
        };
    };
    let restricted = match event_type {
        "enforcement_applied" => match event.action.as_deref() {
            Some("remove_content" | "visibility_limit" | "age_gate") => true,
            _ => return Outcome::Skip, // actor-level, warn
        },
        "enforcement_reversed" => false,
        _ => return Outcome::Skip,
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
    Outcome::Apply(ApplyMapVisibilityCommand {
        post_id: subject.entity_id.clone(),
        change:  VisibilityChange::Moderation { restricted, version },
    })
}

pub struct VisibilityWorker<SI, CS, TR, PS> {
    kafka_config:    KafkaClientConfig,
    spatial_index:   Arc<SI>,
    card_store:      Arc<CS>,
    tile_repository: Arc<TR>,
    pin_store:       Arc<PS>,
    group_id:        String,
}

impl<SI, CS, TR, PS> VisibilityWorker<SI, CS, TR, PS>
where
    SI: SpatialIndex + 'static,
    CS: CardStore + 'static,
    TR: TileRepository + 'static,
    PS: PinStore + 'static,
{
    pub fn new(
        kafka_config:    KafkaClientConfig,
        spatial_index:   Arc<SI>,
        card_store:      Arc<CS>,
        tile_repository: Arc<TR>,
        pin_store:       Arc<PS>,
        group_id:        impl Into<String>,
    ) -> Self {
        Self { kafka_config, spatial_index, card_store, tile_repository, pin_store, group_id: group_id.into() }
    }

    pub async fn run(self) {
        let producer = match build_dlq_producer(&self.kafka_config) {
            Ok(producer) => producer,
            Err(e) => {
                tracing::error!(error = %e, "failed to build DLQ producer — map visibility consumer not started");
                return;
            }
        };
        let worker = Arc::new(self);
        loop {
            match worker.clone().run_once(&producer).await {
                Ok(()) => tracing::warn!("map visibility consumer exited cleanly — restarting"),
                Err(e) => {
                    tracing::error!(error = %e, "map visibility consumer error — restarting after 5 s");
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
            .subscribe_many([TOPIC_POST_DELETED, TOPIC_MODERATION])
            .build()
            .map_err(|e| e.to_string())?;
        tracing::info!(group = %self.group_id, "map visibility consumer started");

        let policy = RetryPolicy::default();
        run_consumer::<VisibilityEvent, _>(&handle, producer, &policy, move |event| {
            let worker = Arc::clone(&self);
            Box::pin(async move { worker.process(event).await })
        })
        .await
        .map_err(|e| e.to_string())
    }

    async fn process(&self, event: &VisibilityEvent) -> ProcessOutcome {
        use cqrs::{CommandHandler, Envelope};

        let cmd = match outcome(event) {
            Outcome::Skip => return ProcessOutcome::Done,
            Outcome::Poison(reason) => return ProcessOutcome::Reject(reason),
            Outcome::Apply(cmd) => cmd,
        };
        let handler = ApplyMapVisibilityHandler {
            spatial_index:   Arc::clone(&self.spatial_index),
            card_store:      Arc::clone(&self.card_store),
            tile_repository: Arc::clone(&self.tile_repository),
            pin_store:       Arc::clone(&self.pin_store),
        };
        ProcessOutcome::from_result(handler.handle(Envelope::new(uuid::Uuid::now_v7(), cmd)).await)
    }
}

#[cfg(test)]
mod tests {
    use chrono::Utc;
    use moderation::domain::event::{DomainEvent, EnforcementApplied, EnforcementReversed};
    use moderation::domain::value_object::{
        ActionType, ActorId, EnforcementId, EnforcementVersion, EntityType, SubjectRef,
    };
    use uuid::Uuid;

    use super::*;

    const POST: &str = "0190f0a0-0000-7000-8000-000000000001";

    fn decode(bytes: &[u8]) -> VisibilityEvent {
        serde_json::from_slice(bytes).unwrap()
    }

    fn applied(entity_type: EntityType, action: ActionType, version: i64) -> VisibilityEvent {
        decode(&serde_json::to_vec(&DomainEvent::EnforcementApplied(EnforcementApplied {
            enforcement_id: EnforcementId::new(),
            subject: SubjectRef::new(entity_type, POST, ActorId::from_uuid(Uuid::now_v7()), "map").unwrap(),
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

    fn apply(change: VisibilityChange) -> Outcome {
        Outcome::Apply(ApplyMapVisibilityCommand { post_id: POST.into(), change })
    }

    #[test]
    fn a_deleted_post_leaves_the_map() {
        // The legacy payload, as post serializes PostDeletedEvent.
        let event = decode(br#"{"post_id":"0190f0a0-0000-7000-8000-000000000001","profile_id":"p","deleted_at_ms":1}"#);
        assert_eq!(outcome(&event), apply(VisibilityChange::Deleted));
    }

    #[test]
    fn takedowns_limits_and_age_gates_hide_reversals_restore() {
        for action in [ActionType::RemoveContent, ActionType::VisibilityLimit, ActionType::AgeGate] {
            assert_eq!(
                outcome(&applied(EntityType::Post, action, 3)),
                apply(VisibilityChange::Moderation { restricted: true, version: 3 })
            );
        }
        let reversed = decode(&serde_json::to_vec(&DomainEvent::EnforcementReversed(EnforcementReversed {
            enforcement_id: EnforcementId::new(),
            subject: SubjectRef::new(EntityType::Post, POST, ActorId::from_uuid(Uuid::now_v7()), "").unwrap(),
            actor_id: ActorId::from_uuid(Uuid::now_v7()),
            version: EnforcementVersion::from_i64(4),
            occurred_at: Utc::now(),
            correlation_id: Uuid::now_v7(),
        }))
        .unwrap());
        assert_eq!(outcome(&reversed), apply(VisibilityChange::Moderation { restricted: false, version: 4 }));
    }

    #[test]
    fn other_entities_and_actions_are_skipped() {
        assert_eq!(outcome(&applied(EntityType::Comment, ActionType::RemoveContent, 1)), Outcome::Skip);
        for action in [ActionType::Warn, ActionType::Ban] {
            assert_eq!(outcome(&applied(EntityType::Post, action, 1)), Outcome::Skip, "{action:?}");
        }
        assert_eq!(outcome(&decode(br#"{"type":"case_opened"}"#)), Outcome::Skip);
    }
}
