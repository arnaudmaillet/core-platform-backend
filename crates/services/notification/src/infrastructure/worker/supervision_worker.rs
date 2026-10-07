//! Family supervision notices (#670): account's `supervision_started` /
//! `supervision_ended` on `account.v1.events` become notices to the side(s)
//! that must know — every profile of that account, which account names on the
//! event:
//!
//! - started → both sides ("you and X are paired");
//! - ended by the teen → the supervisor; by the supervisor → the teen;
//! - the teen turned 18, or an account was erased → both sides.
//!
//! The sender is the other side's (first) profile, so the app shows who; the
//! subject is the other side's account (the app opens Settings →
//! Supervision). Not block-gated: a teen is always told about supervision.
//! Feed only (no push category covers it). Every other account event is
//! ignored.

use std::sync::Arc;

use chrono::{DateTime, Utc};
use serde::Deserialize;
use transport::kafka::config::client::KafkaClientConfig;
use transport::kafka::config::consumer::{AutoOffsetReset, ConsumerConfig};
use transport::kafka::consumer::builder::KafkaConsumerBuilder;
use transport::kafka::consumer::{run_consumer, ProcessOutcome, RetryPolicy};
use transport::kafka::producer::KafkaProducerHandle;

use crate::application::port::stream_registry::NotificationPayload;
use crate::application::port::{NoPush, NotificationRepository, PushNotifier, StreamRegistry, UnreadCounter};
use crate::domain::aggregate::Notification;
use crate::domain::value_object::{NotificationId, NotificationKind, ProfileId, SubjectId, SubjectKind};
use crate::error::NotificationError;
use crate::infrastructure::worker::build_dlq_producer;

const TOPIC_ACCOUNT_EVENTS: &str = "account.v1.events";

/// `account.v1.events`, tagged on `type` (snake_case): only the supervision
/// events matter here.
#[derive(Debug, Clone, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum AccountEventPayload {
    SupervisionStarted(SupervisionPayload),
    SupervisionEnded(SupervisionPayload),
    #[serde(other)]
    Other,
}

#[derive(Debug, Clone, Deserialize)]
pub struct SupervisionPayload {
    /// The teen's account.
    pub account_id:             String,
    pub supervisor_id:          String,
    /// `ended` only: by_teen, by_supervisor, came_of_age, account_deleted.
    #[serde(default)]
    pub ended_by:               String,
    #[serde(default)]
    pub teen_profile_ids:       Vec<String>,
    #[serde(default)]
    pub supervisor_profile_ids: Vec<String>,
    pub occurred_at:            DateTime<Utc>,
}

/// One profile told about a supervision.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SupervisionNotice {
    pub target:       ProfileId,
    /// The other side's first profile (`None`: it has none left).
    pub sender:       Option<ProfileId>,
    pub kind:         NotificationKind,
    /// The other side's account.
    pub subject:      SubjectId,
    /// Stable per (event, profile): the notification id and the unread claim.
    pub business_key: String,
    pub at:           DateTime<Utc>,
}

/// Which side(s) an event tells, and as what.
fn sides_of(event: &AccountEventPayload) -> Option<(&SupervisionPayload, NotificationKind, bool, bool)> {
    match event {
        AccountEventPayload::SupervisionStarted(p) => Some((p, NotificationKind::SupervisionStarted, true, true)),
        AccountEventPayload::SupervisionEnded(p) => Some(match p.ended_by.as_str() {
            "by_teen" => (p, NotificationKind::SupervisionEnded, false, true),
            "by_supervisor" => (p, NotificationKind::SupervisionEnded, true, false),
            "came_of_age" => (p, NotificationKind::SupervisionCameOfAge, true, true),
            _ => (p, NotificationKind::SupervisionEnded, true, true),
        }),
        AccountEventPayload::Other => None,
    }
}

fn profiles(ids: &[String]) -> Vec<ProfileId> {
    let mut out: Vec<ProfileId> = Vec::new();
    for raw in ids {
        match ProfileId::try_from(raw.as_str()) {
            Ok(id) if !out.contains(&id) => out.push(id),
            Ok(_) => {}
            Err(_) => tracing::warn!(profile_id = %raw, "supervision notice: not a profile id — skipped"),
        }
    }
    out
}

/// The notices `event` becomes. Malformed account ids are poison (an error);
/// a malformed profile id is skipped, the others told.
pub fn notices_of(event: &AccountEventPayload) -> Result<Vec<SupervisionNotice>, NotificationError> {
    let Some((p, kind, tell_teen, tell_supervisor)) = sides_of(event) else {
        return Ok(Vec::new());
    };
    let teen_account = SubjectId::try_from(p.account_id.as_str())?;
    let supervisor_account = SubjectId::try_from(p.supervisor_id.as_str())?;
    let (teen, supervisor) = (profiles(&p.teen_profile_ids), profiles(&p.supervisor_profile_ids));
    let mut notices = Vec::new();
    let mut tell = |targets: &[ProfileId], sender: Option<ProfileId>, subject: SubjectId| {
        for target in targets {
            notices.push(SupervisionNotice {
                target: *target,
                sender,
                kind,
                subject,
                business_key: format!(
                    "{}:{}:{}:{}:{}",
                    kind.as_str(),
                    p.account_id,
                    p.supervisor_id,
                    p.occurred_at.timestamp_millis(),
                    target
                ),
                at: p.occurred_at,
            });
        }
    };
    if tell_teen {
        tell(&teen, supervisor.first().copied(), supervisor_account);
    }
    if tell_supervisor {
        tell(&supervisor, teen.first().copied(), teen_account);
    }
    Ok(notices)
}

pub struct SupervisionNotificationWorker<R, U, S> {
    kafka_config: KafkaClientConfig,
    repository:   Arc<R>,
    counter:      Arc<U>,
    stream_reg:   Arc<S>,
    push:         Arc<dyn PushNotifier>,
    group_id:     String,
}

impl<R, U, S> SupervisionNotificationWorker<R, U, S>
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
        Self { kafka_config, repository, counter, stream_reg, push: Arc::new(NoPush), group_id: group_id.into() }
    }

    /// Sends each notification written for the first time as a push (#654).
    pub fn with_push(mut self, push: Arc<dyn PushNotifier>) -> Self {
        self.push = push;
        self
    }

    pub async fn run(self) {
        let producer = match build_dlq_producer(&self.kafka_config) {
            Ok(producer) => producer,
            Err(e) => {
                tracing::error!(error = %e, "failed to build DLQ producer — supervision notification consumer not started");
                return;
            }
        };
        let worker = Arc::new(self);
        loop {
            match worker.clone().run_once(&producer).await {
                Ok(()) => tracing::warn!("supervision notification consumer exited cleanly — restarting"),
                Err(e) => {
                    tracing::error!(error = %e, "supervision notification consumer error — restarting after 5 s");
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
            .subscribe_many([TOPIC_ACCOUNT_EVENTS])
            .build()
            .map_err(|e| e.to_string())?;
        tracing::info!(group = %self.group_id, "supervision notification consumer started");

        let policy = RetryPolicy::default();
        run_consumer::<AccountEventPayload, _>(&handle, producer, &policy, move |event| {
            let worker = Arc::clone(&self);
            Box::pin(async move { ProcessOutcome::from_result(worker.process(event).await) })
        })
        .await
        .map_err(|e| e.to_string())
    }

    /// Handles one event (public so the integration suite drives it without a
    /// broker). Redelivery is harmless: ids and unread claims are per (event,
    /// profile).
    pub async fn process(&self, event: &AccountEventPayload) -> Result<(), NotificationError> {
        for notice in notices_of(event)? {
            let id = NotificationId::deterministic(&notice.business_key);
            let notification = match notice.sender {
                Some(sender) => {
                    Notification::create(id, notice.target, sender, notice.kind, SubjectKind::Account, notice.subject, notice.at)
                }
                None => Notification::from_platform(id, notice.target, notice.kind, SubjectKind::Account, notice.subject, notice.at),
            };
            self.repository.insert(&notification).await?;
            let first = self.counter.increment_once(&notice.target, &notice.business_key).await?;
            let payload = Arc::new(NotificationPayload {
                notification_id:   notification.id().as_uuid(),
                target_profile_id: notification.target_profile_id().as_uuid(),
                sender_profile_id: notification.sender_profile_id().as_uuid(),
                sample_sender_ids: notification.sample_sender_ids().to_vec(),
                sender_count:      notification.sender_count(),
                kind:              notification.kind(),
                subject_kind:      notification.subject_kind(),
                subject_id:        notification.subject_id().as_uuid(),
                created_at_ms:     notification.created_at().timestamp_millis(),
            });
            if first {
                self.push.notify(Arc::clone(&payload));
            }
            self.stream_reg.broadcast(&notice.target, payload);
            tracing::debug!(kind = notice.kind.as_str(), target = %notice.target, "supervision notification written");
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use uuid::Uuid;

    use super::*;

    fn id() -> String {
        Uuid::now_v7().to_string()
    }

    fn event(kind: &str, ended_by: &str, teen_profiles: &[String], supervisor_profiles: &[String]) -> AccountEventPayload {
        serde_json::from_value(serde_json::json!({
            "type": kind,
            "account_id": id(),
            "supervisor_id": id(),
            "ended_by": ended_by,
            "teen_profile_ids": teen_profiles,
            "supervisor_profile_ids": supervisor_profiles,
            "occurred_at": "2026-10-08T12:00:00Z",
            "correlation_id": id()
        }))
        .unwrap()
    }

    #[test]
    fn a_start_tells_both_sides_from_the_other_side() {
        let (teen, parent) = (id(), id());
        let notices = notices_of(&event("supervision_started", "", std::slice::from_ref(&teen), std::slice::from_ref(&parent))).unwrap();
        assert_eq!(notices.len(), 2);
        let to_teen = notices.iter().find(|n| n.target.as_str() == teen).unwrap();
        assert_eq!(to_teen.sender.map(|s| s.as_str()), Some(parent.clone()));
        assert_eq!(to_teen.kind, NotificationKind::SupervisionStarted);
        let to_parent = notices.iter().find(|n| n.target.as_str() == parent).unwrap();
        assert_eq!(to_parent.sender.map(|s| s.as_str()), Some(teen));
        assert_ne!(to_teen.business_key, to_parent.business_key);
    }

    #[test]
    fn an_end_tells_the_other_side_and_coming_of_age_tells_both() {
        let (teen, parent) = (id(), id());
        let by_teen = notices_of(&event("supervision_ended", "by_teen", std::slice::from_ref(&teen), std::slice::from_ref(&parent))).unwrap();
        assert_eq!(by_teen.iter().map(|n| n.target.as_str()).collect::<Vec<_>>(), vec![parent.clone()]);
        let by_parent = notices_of(&event("supervision_ended", "by_supervisor", std::slice::from_ref(&teen), std::slice::from_ref(&parent))).unwrap();
        assert_eq!(by_parent.iter().map(|n| n.target.as_str()).collect::<Vec<_>>(), vec![teen.clone()]);
        let of_age = notices_of(&event("supervision_ended", "came_of_age", std::slice::from_ref(&teen), std::slice::from_ref(&parent))).unwrap();
        assert_eq!(of_age.len(), 2);
        assert!(of_age.iter().all(|n| n.kind == NotificationKind::SupervisionCameOfAge));
        // An erased side with no profile left: the survivor is told by the platform.
        let erased = notices_of(&event("supervision_ended", "account_deleted", &[teen], &[])).unwrap();
        assert_eq!((erased.len(), erased[0].sender), (1, None));
    }

    #[test]
    fn other_account_events_are_ignored() {
        let other: AccountEventPayload =
            serde_json::from_value(serde_json::json!({ "type": "account_suspended", "account_id": id() })).unwrap();
        assert!(notices_of(&other).unwrap().is_empty());
    }

    /// The events as account serializes them (its own types).
    #[test]
    fn reads_accounts_own_supervision_events() {
        use account::domain::event::{DomainEvent, SupervisionEnded};
        use account::domain::value_object::AccountId;

        let (teen, parent) = (id(), id());
        let ended = DomainEvent::SupervisionEnded(SupervisionEnded {
            account_id: AccountId::new(),
            supervisor_id: AccountId::new(),
            ended_by: "by_supervisor".into(),
            teen_profile_ids: vec![teen.clone()],
            supervisor_profile_ids: vec![parent],
            occurred_at: Utc::now(),
            correlation_id: Uuid::now_v7(),
        });
        let wire: AccountEventPayload = serde_json::from_value(serde_json::to_value(&ended).unwrap()).unwrap();
        let notices = notices_of(&wire).unwrap();
        assert_eq!(notices.iter().map(|n| n.target.as_str()).collect::<Vec<_>>(), vec![teen]);
    }
}
