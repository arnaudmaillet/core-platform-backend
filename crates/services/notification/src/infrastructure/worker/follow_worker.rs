//! Follows, follow requests and their approval (#755): `social-graph.followed`
//! and `social-graph.follow_requested` become notifications.
//!
//! - a follow → the followee: "X started following you" (`Follow`);
//! - a request to a private profile → its owner: "X asked to follow you"
//!   (`FollowRequest`, the app opens the requests inbox);
//! - an approved request (`followed` with `via_request`) → the requester: "X
//!   accepted your follow request" (`FollowAccepted`, the app opens X's
//!   profile). The owner, who approved it, is not told again.
//!
//! The subject is the other profile. Self-notifications and blocked senders are
//! suppressed, like every worker.
//!
//! A request gone without becoming a follow (cancelled, declined, cut by a
//! block, moot once the profile is public) arrives on the request's topic with
//! `withdrawn_at`: its "X asked to follow you" is **retracted** — deleted, and
//! taken off the badge when it was still unread.

use std::sync::Arc;

use chrono::{DateTime, Utc};
use serde::Deserialize;
use transport::kafka::config::client::KafkaClientConfig;
use transport::kafka::config::consumer::{AutoOffsetReset, ConsumerConfig};
use transport::kafka::consumer::builder::KafkaConsumerBuilder;
use transport::kafka::consumer::{run_consumer, ProcessOutcome, RetryPolicy};
use transport::kafka::producer::KafkaProducerHandle;

use crate::application::port::stream_registry::NotificationPayload;
use crate::application::port::{BlockCache, NotificationRepository, StreamRegistry, UnreadCounter};
use crate::domain::aggregate::Notification;
use crate::domain::value_object::{NotificationId, NotificationKind, ProfileId, SubjectId, SubjectKind};
use crate::error::NotificationError;
use crate::infrastructure::worker::build_dlq_producer;

const TOPIC_FOLLOWED: &str = "social-graph.followed";
const TOPIC_FOLLOW_REQUESTED: &str = "social-graph.follow_requested";

/// Both topics' payloads: `actor_id` follows (or asked to follow) `target_id`.
/// A request carries `requested_at`, a follow `followed_at` (and `via_request`
/// when it is an approved request); a withdrawn request `requested_at` and
/// `withdrawn_at`.
#[derive(Debug, Clone, Deserialize)]
pub struct FollowEventPayload {
    pub actor_id: String,
    pub target_id: String,
    #[serde(default)]
    pub followed_at: Option<DateTime<Utc>>,
    #[serde(default)]
    pub requested_at: Option<DateTime<Utc>>,
    #[serde(default)]
    pub via_request: bool,
    #[serde(default)]
    pub withdrawn_at: Option<DateTime<Utc>>,
}

/// The notification an event becomes: who is told, by whom, of what.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FollowNotice {
    pub target: ProfileId,
    pub sender: ProfileId,
    pub kind: NotificationKind,
    /// Stable per event: the notification id and the unread claim.
    pub business_key: String,
    pub at: DateTime<Utc>,
}

/// What `event` notifies, or `None` (malformed times are poison upstream).
pub fn notice_of(event: &FollowEventPayload) -> Result<Option<FollowNotice>, NotificationError> {
    if event.withdrawn_at.is_some() {
        return Ok(None);
    }
    let actor = ProfileId::try_from(event.actor_id.as_str())?;
    let target = ProfileId::try_from(event.target_id.as_str())?;
    let (notice_target, sender, kind, at) = match (event.requested_at, event.followed_at) {
        (Some(at), _) => (target, actor, NotificationKind::FollowRequest, at),
        (None, Some(at)) if event.via_request => (actor, target, NotificationKind::FollowAccepted, at),
        (None, Some(at)) => (target, actor, NotificationKind::Follow, at),
        (None, None) => return Ok(None),
    };
    let business_key = business_key(kind, &actor, &target, at);
    Ok(Some(FollowNotice { target: notice_target, sender, kind, business_key, at }))
}

/// The notification an event about `actor` → `target` at `at` is stored under.
fn business_key(kind: NotificationKind, actor: &ProfileId, target: &ProfileId, at: DateTime<Utc>) -> String {
    format!("{}:{}:{}:{}", kind.as_str(), actor, target, at.timestamp_millis())
}

/// The "X asked to follow you" a withdrawal retracts: the owner's notice for
/// that request, or `None` when `event` withdraws nothing.
pub fn retraction_of(event: &FollowEventPayload) -> Result<Option<FollowNotice>, NotificationError> {
    let (Some(_), Some(requested_at)) = (event.withdrawn_at, event.requested_at) else { return Ok(None) };
    let actor = ProfileId::try_from(event.actor_id.as_str())?;
    let target = ProfileId::try_from(event.target_id.as_str())?;
    Ok(Some(FollowNotice {
        target,
        sender: actor,
        kind: NotificationKind::FollowRequest,
        business_key: business_key(NotificationKind::FollowRequest, &actor, &target, requested_at),
        at: requested_at,
    }))
}

pub struct FollowNotificationWorker<R, B, U, S> {
    kafka_config: KafkaClientConfig,
    repository:   Arc<R>,
    block_cache:  Arc<B>,
    counter:      Arc<U>,
    stream_reg:   Arc<S>,
    group_id:     String,
}

impl<R, B, U, S> FollowNotificationWorker<R, B, U, S>
where
    R: NotificationRepository,
    B: BlockCache,
    U: UnreadCounter,
    S: StreamRegistry,
{
    pub fn new(
        kafka_config: KafkaClientConfig,
        repository:   Arc<R>,
        block_cache:  Arc<B>,
        counter:      Arc<U>,
        stream_reg:   Arc<S>,
        group_id:     impl Into<String>,
    ) -> Self {
        Self { kafka_config, repository, block_cache, counter, stream_reg, group_id: group_id.into() }
    }

    pub async fn run(self) {
        let producer = match build_dlq_producer(&self.kafka_config) {
            Ok(producer) => producer,
            Err(e) => {
                tracing::error!(error = %e, "failed to build DLQ producer — follow notification consumer not started");
                return;
            }
        };
        let worker = Arc::new(self);
        loop {
            match worker.clone().run_once(&producer).await {
                Ok(()) => tracing::warn!("follow notification consumer exited cleanly — restarting"),
                Err(e) => {
                    tracing::error!(error = %e, "follow notification consumer error — restarting after 5 s");
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
            .subscribe_many([TOPIC_FOLLOWED, TOPIC_FOLLOW_REQUESTED])
            .build()
            .map_err(|e| e.to_string())?;
        tracing::info!(group = %self.group_id, "follow notification consumer started");

        let policy = RetryPolicy::default();
        run_consumer::<FollowEventPayload, _>(&handle, producer, &policy, move |event| {
            let worker = Arc::clone(&self);
            Box::pin(async move { ProcessOutcome::from_result(worker.process(event).await) })
        })
        .await
        .map_err(|e| e.to_string())
    }

    /// Handles one event, as the runner hands it (public so the integration
    /// suite drives it without a broker).
    pub async fn process(&self, event: &FollowEventPayload) -> Result<(), NotificationError> {
        if let Some(withdrawn) = retraction_of(event)? {
            return self.retract(&withdrawn).await;
        }
        let Some(notice) = notice_of(event)? else {
            tracing::debug!(actor = %event.actor_id, "follow event without a time — skipped");
            return Ok(());
        };
        // Intentional suppressions, not failures.
        if notice.sender == notice.target {
            return Ok(());
        }
        if self.block_cache.is_blocked(&notice.sender, &notice.target).await? {
            tracing::debug!(kind = notice.kind.as_str(), "follow notification suppressed: sender blocked");
            return Ok(());
        }

        let notification = Notification::create(
            NotificationId::deterministic(&notice.business_key),
            notice.target,
            notice.sender,
            notice.kind,
            SubjectKind::Profile,
            SubjectId::try_from(notice.sender.as_str().as_str())?,
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
        tracing::debug!(kind = notice.kind.as_str(), target = %notice.target, "follow notification written");
        Ok(())
    }

    /// Takes a withdrawn request's notice back: off the badge when it was
    /// still unread (and newer than a mark-all-read), then deleted. A replay
    /// finds nothing and does nothing; one that lands between the two steps
    /// leaves the badge one too high until the next mark-all-read, never low.
    async fn retract(&self, notice: &FollowNotice) -> Result<(), NotificationError> {
        let id = NotificationId::deterministic(&notice.business_key);
        let at_ms = notice.at.timestamp_millis();
        let was_unread = match self.repository.mark_read(&notice.target, id.as_uuid(), at_ms).await {
            Ok(was_unread) => was_unread,
            Err(NotificationError::NotificationNotFound { .. }) => return Ok(()),
            Err(e) => return Err(e),
        };
        if was_unread && at_ms > self.counter.get_read_horizon(&notice.target).await? {
            self.counter.decrement(&notice.target).await?;
        }
        self.repository.delete(&notice.target, id.as_uuid(), at_ms).await?;
        tracing::debug!(target = %notice.target, "withdrawn follow request: notification retracted");
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use uuid::Uuid;

    fn ids() -> (String, String) {
        (Uuid::now_v7().to_string(), Uuid::now_v7().to_string())
    }

    #[test]
    fn each_event_notifies_the_right_profile() {
        let (actor, target) = ids();
        let at = Utc::now();
        let event = |followed_at, requested_at, via_request| FollowEventPayload {
            actor_id: actor.clone(),
            target_id: target.clone(),
            followed_at,
            requested_at,
            via_request,
            withdrawn_at: None,
        };

        let follow = notice_of(&event(Some(at), None, false)).unwrap().unwrap();
        assert_eq!((follow.kind, follow.target.as_str(), follow.sender.as_str()), (NotificationKind::Follow, target.clone(), actor.clone()));

        let request = notice_of(&event(None, Some(at), false)).unwrap().unwrap();
        assert_eq!((request.kind, request.target.as_str()), (NotificationKind::FollowRequest, target.clone()));

        // Approved: the requester is told, by the owner.
        let accepted = notice_of(&event(Some(at), None, true)).unwrap().unwrap();
        assert_eq!(
            (accepted.kind, accepted.target.as_str(), accepted.sender.as_str()),
            (NotificationKind::FollowAccepted, actor.clone(), target.clone())
        );

        assert!(notice_of(&event(None, None, false)).unwrap().is_none());
        assert_ne!(follow.business_key, accepted.business_key);
    }

    #[test]
    fn the_wire_payloads_of_both_topics_decode() {
        let (actor, target) = ids();
        let followed = format!(r#"{{"actor_id":"{actor}","target_id":"{target}","followed_at":"2026-10-05T03:00:00Z"}}"#);
        let event: FollowEventPayload = serde_json::from_str(&followed).unwrap();
        assert!(event.followed_at.is_some() && !event.via_request, "older events: a direct follow");
        let requested = format!(r#"{{"actor_id":"{actor}","target_id":"{target}","requested_at":"2026-10-05T03:00:00Z"}}"#);
        let event: FollowEventPayload = serde_json::from_str(&requested).unwrap();
        assert!(event.requested_at.is_some());
    }

    /// The contract with the producer: social-graph's own events, as it
    /// serializes them, decode into what this worker reads.
    #[test]
    fn social_graphs_events_decode_as_the_worker_reads_them() {
        use social_graph::domain::event::{FollowRequested, ProfileFollowed};
        use social_graph::domain::value_object::ProfileId as GraphProfileId;

        let (actor, target) = ids();
        let (a, t) = (GraphProfileId::try_from(actor.as_str()).unwrap(), GraphProfileId::try_from(target.as_str()).unwrap());
        let at = Utc::now();

        let approved = ProfileFollowed { actor_id: a, target_id: t, followed_at: at, via_request: true };
        let read: FollowEventPayload = serde_json::from_str(&serde_json::to_string(&approved).unwrap()).unwrap();
        assert_eq!((read.actor_id.as_str(), read.target_id.as_str()), (actor.as_str(), target.as_str()));
        assert_eq!(notice_of(&read).unwrap().unwrap().kind, NotificationKind::FollowAccepted);

        let requested = FollowRequested { actor_id: a, target_id: t, requested_at: at };
        let read: FollowEventPayload = serde_json::from_str(&serde_json::to_string(&requested).unwrap()).unwrap();
        assert_eq!(notice_of(&read).unwrap().unwrap().kind, NotificationKind::FollowRequest);
    }

    /// A withdrawn request retracts the owner's notice for that very request
    /// (same id as when it was told), and notifies nothing new.
    #[test]
    fn a_withdrawal_retracts_the_requests_notice() {
        let (actor, target) = ids();
        let requested_at = Utc::now() - chrono::Duration::minutes(5);
        let request = FollowEventPayload {
            actor_id: actor.clone(),
            target_id: target.clone(),
            followed_at: None,
            requested_at: Some(requested_at),
            via_request: false,
            withdrawn_at: None,
        };
        let withdrawn = FollowEventPayload { withdrawn_at: Some(Utc::now()), ..request.clone() };

        assert!(notice_of(&withdrawn).unwrap().is_none(), "nothing new is told");
        assert!(retraction_of(&request).unwrap().is_none(), "a request retracts nothing");
        let told = notice_of(&request).unwrap().unwrap();
        let retracted = retraction_of(&withdrawn).unwrap().unwrap();
        assert_eq!(retracted.business_key, told.business_key, "the very notice it was told");
        assert_eq!((retracted.target, retracted.at), (told.target, told.at));
    }

    /// The contract with the producer for withdrawals.
    #[test]
    fn social_graphs_withdrawal_decodes_as_the_worker_reads_it() {
        use social_graph::domain::event::FollowRequestWithdrawn;
        use social_graph::domain::value_object::ProfileId as GraphProfileId;

        let (actor, target) = ids();
        let requested_at = Utc::now();
        let withdrawn = FollowRequestWithdrawn {
            actor_id: GraphProfileId::try_from(actor.as_str()).unwrap(),
            target_id: GraphProfileId::try_from(target.as_str()).unwrap(),
            requested_at,
            withdrawn_at: Utc::now(),
        };
        let read: FollowEventPayload = serde_json::from_str(&serde_json::to_string(&withdrawn).unwrap()).unwrap();
        let retracted = retraction_of(&read).unwrap().expect("a retraction");
        assert_eq!((retracted.target.as_str(), retracted.sender.as_str()), (target, actor));
        assert_eq!(retracted.at.timestamp_millis(), requested_at.timestamp_millis());
    }
}
