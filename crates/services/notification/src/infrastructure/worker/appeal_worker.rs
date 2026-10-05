//! Appeal outcomes (#744, #745): moderation's `appeal_resolved` on
//! `moderation.v1.events` becomes a notice to **every profile** of the
//! appellant account, which moderation names on the event (moderation reasons
//! about accounts, notifications about profiles).
//!
//! - upheld → "your appeal was decided: the decision stands" (`AppealUpheld`);
//! - overturned → "your appeal was decided in your favour" (`AppealOverturned`:
//!   a sanction lifted, or a reporter's case back under review — the appeal,
//!   which the app opens, says which).
//!
//! The notice comes from the platform: no sender, nothing to block. Every other
//! moderation event is ignored.

use std::sync::Arc;

use chrono::{DateTime, Utc};
use serde::Deserialize;
use transport::kafka::config::client::KafkaClientConfig;
use transport::kafka::config::consumer::{AutoOffsetReset, ConsumerConfig};
use transport::kafka::consumer::builder::KafkaConsumerBuilder;
use transport::kafka::consumer::{run_consumer, ProcessOutcome, RetryPolicy};
use transport::kafka::producer::KafkaProducerHandle;

use crate::application::port::stream_registry::NotificationPayload;
use crate::application::port::{NotificationRepository, StreamRegistry, UnreadCounter};
use crate::domain::aggregate::Notification;
use crate::domain::value_object::{NotificationId, NotificationKind, ProfileId, SubjectId, SubjectKind};
use crate::error::NotificationError;
use crate::infrastructure::worker::build_dlq_producer;

const TOPIC_MODERATION_EVENTS: &str = "moderation.v1.events";

/// `moderation.v1.events`, internally tagged on `type` (snake_case): only
/// `appeal_resolved` matters here.
#[derive(Debug, Clone, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum ModerationEventPayload {
    AppealResolved(AppealResolvedPayload),
    #[serde(other)]
    Other,
}

#[derive(Debug, Clone, Deserialize)]
pub struct AppealResolvedPayload {
    pub appeal_id: String,
    pub overturned: bool,
    /// The appellant account's profiles; absent from events older than #744
    /// (nobody to tell).
    #[serde(default)]
    pub profile_ids: Vec<String>,
    pub occurred_at: DateTime<Utc>,
}

/// One profile told of an appeal's outcome.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AppealNotice {
    pub target: ProfileId,
    pub kind: NotificationKind,
    pub appeal: SubjectId,
    /// Stable per (appeal, profile): the notification id and the unread claim.
    pub business_key: String,
    pub at: DateTime<Utc>,
}

/// The notices `event` becomes, one per named profile. A malformed appeal id
/// is poison (an error); a malformed profile id is skipped, the others told.
pub fn notices_of(event: &AppealResolvedPayload) -> Result<Vec<AppealNotice>, NotificationError> {
    let appeal = SubjectId::try_from(event.appeal_id.as_str())?;
    let kind = if event.overturned { NotificationKind::AppealOverturned } else { NotificationKind::AppealUpheld };
    let mut notices = Vec::with_capacity(event.profile_ids.len());
    for raw in &event.profile_ids {
        let Ok(target) = ProfileId::try_from(raw.as_str()) else {
            tracing::warn!(profile_id = %raw, appeal = %event.appeal_id, "appeal outcome: not a profile id — skipped");
            continue;
        };
        if notices.iter().any(|n: &AppealNotice| n.target == target) {
            continue;
        }
        notices.push(AppealNotice {
            target,
            kind,
            appeal,
            business_key: format!("{}:{}:{}", kind.as_str(), appeal, target),
            at: event.occurred_at,
        });
    }
    Ok(notices)
}

pub struct AppealNotificationWorker<R, U, S> {
    kafka_config: KafkaClientConfig,
    repository:   Arc<R>,
    counter:      Arc<U>,
    stream_reg:   Arc<S>,
    group_id:     String,
}

impl<R, U, S> AppealNotificationWorker<R, U, S>
where
    R: NotificationRepository,
    U: UnreadCounter,
    S: StreamRegistry,
{
    pub fn new(
        kafka_config: KafkaClientConfig,
        repository:   Arc<R>,
        counter:      Arc<U>,
        stream_reg:   Arc<S>,
        group_id:     impl Into<String>,
    ) -> Self {
        Self { kafka_config, repository, counter, stream_reg, group_id: group_id.into() }
    }

    pub async fn run(self) {
        let producer = match build_dlq_producer(&self.kafka_config) {
            Ok(producer) => producer,
            Err(e) => {
                tracing::error!(error = %e, "failed to build DLQ producer — appeal notification consumer not started");
                return;
            }
        };
        let worker = Arc::new(self);
        loop {
            match worker.clone().run_once(&producer).await {
                Ok(()) => tracing::warn!("appeal notification consumer exited cleanly — restarting"),
                Err(e) => {
                    tracing::error!(error = %e, "appeal notification consumer error — restarting after 5 s");
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
            .subscribe_many([TOPIC_MODERATION_EVENTS])
            .build()
            .map_err(|e| e.to_string())?;
        tracing::info!(group = %self.group_id, "appeal notification consumer started");

        let policy = RetryPolicy::default();
        run_consumer::<ModerationEventPayload, _>(&handle, producer, &policy, move |event| {
            let worker = Arc::clone(&self);
            Box::pin(async move { ProcessOutcome::from_result(worker.process(event).await) })
        })
        .await
        .map_err(|e| e.to_string())
    }

    /// Handles one event, as the runner hands it (public so the integration
    /// suite drives it without a broker). Redelivery is harmless: the
    /// notification id and the unread claim are per (appeal, profile).
    pub async fn process(&self, event: &ModerationEventPayload) -> Result<(), NotificationError> {
        let ModerationEventPayload::AppealResolved(resolved) = event else {
            return Ok(());
        };
        for notice in notices_of(resolved)? {
            let notification = Notification::from_platform(
                NotificationId::deterministic(&notice.business_key),
                notice.target,
                notice.kind,
                SubjectKind::Appeal,
                notice.appeal,
                notice.at,
            );
            self.repository.insert(&notification).await?;
            self.counter.increment_once(&notice.target, &notice.business_key).await?;

            self.stream_reg.broadcast(
                &notice.target,
                Arc::new(NotificationPayload {
                    notification_id:   notification.id().as_uuid(),
                    target_profile_id: notification.target_profile_id().as_uuid(),
                    sender_profile_id: notification.sender_profile_id().as_uuid(),
                    sample_sender_ids: notification.sample_sender_ids().to_vec(),
                    sender_count:      notification.sender_count(),
                    kind:              notification.kind(),
                    subject_kind:      notification.subject_kind(),
                    subject_id:        notification.subject_id().as_uuid(),
                    created_at_ms:     notification.created_at().timestamp_millis(),
                }),
            );
            tracing::debug!(kind = notice.kind.as_str(), target = %notice.target, "appeal notification written");
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use uuid::Uuid;

    fn resolved(overturned: bool, profile_ids: Vec<String>) -> AppealResolvedPayload {
        AppealResolvedPayload { appeal_id: Uuid::now_v7().to_string(), overturned, profile_ids, occurred_at: Utc::now() }
    }

    #[test]
    fn every_named_profile_is_told_once_with_the_outcome() {
        let (a, b) = (Uuid::now_v7().to_string(), Uuid::now_v7().to_string());
        let event = resolved(true, vec![a.clone(), b.clone(), a.clone(), "not-a-uuid".into()]);
        let notices = notices_of(&event).unwrap();
        let targets: Vec<String> = notices.iter().map(|n| n.target.as_str()).collect();
        assert_eq!(targets, vec![a, b], "deduplicated, the bad id skipped");
        assert!(notices.iter().all(|n| n.kind == NotificationKind::AppealOverturned));
        assert_ne!(notices[0].business_key, notices[1].business_key, "one claim per profile");
        assert_eq!(notices[0].appeal.as_str(), event.appeal_id);

        let upheld = notices_of(&resolved(false, vec![Uuid::now_v7().to_string()])).unwrap();
        assert_eq!(upheld[0].kind, NotificationKind::AppealUpheld);
    }

    #[test]
    fn an_event_naming_nobody_tells_nobody_and_a_bad_appeal_id_is_poison() {
        assert!(notices_of(&resolved(false, Vec::new())).unwrap().is_empty());
        let mut bad = resolved(false, vec![Uuid::now_v7().to_string()]);
        bad.appeal_id = "nope".into();
        assert!(notices_of(&bad).is_err());
    }

    #[test]
    fn other_moderation_events_decode_and_are_ignored() {
        let json = serde_json::json!({ "type": "case_opened", "case_id": "c", "occurred_at": Utc::now() });
        assert!(matches!(
            serde_json::from_value::<ModerationEventPayload>(json).unwrap(),
            ModerationEventPayload::Other
        ));
    }

    /// The contract with the producer: moderation's own `AppealResolved`, as
    /// its publisher serializes the event enum, decodes into what this worker
    /// reads.
    #[test]
    fn moderations_event_decodes_as_the_worker_reads_it() {
        use moderation::domain::event::{AppealResolved, DomainEvent};
        use moderation::domain::value_object::{ActorId, AppealId, DecisionId};

        let profile = Uuid::now_v7().to_string();
        let appeal = AppealId::from_uuid(Uuid::now_v7());
        let event = DomainEvent::AppealResolved(AppealResolved {
            appeal_id: appeal,
            decision_id: DecisionId::from_uuid(Uuid::now_v7()),
            actor_id: ActorId::from_uuid(Uuid::now_v7()),
            by_reporter: true,
            overturned: true,
            profile_ids: vec![profile.clone()],
            occurred_at: Utc::now(),
            correlation_id: Uuid::now_v7(),
        });
        let read: ModerationEventPayload = serde_json::from_str(&serde_json::to_string(&event).unwrap()).unwrap();
        let ModerationEventPayload::AppealResolved(read) = read else { panic!("an appeal outcome") };
        let notices = notices_of(&read).unwrap();
        assert_eq!(notices.len(), 1);
        assert_eq!(notices[0].target.as_str(), profile);
        assert_eq!(notices[0].appeal.as_uuid(), appeal.as_uuid());
        assert_eq!(notices[0].kind, NotificationKind::AppealOverturned);
    }
}
