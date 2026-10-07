use std::sync::Arc;

use cqrs::{Command, CommandHandler, Envelope};
use validate_core::{FieldViolation, Validate};

use crate::application::port::{
    BlockCache, NotificationEventPublisher, NotificationRepository, NotificationStreamEvent,
    StreamRegistry, UnreadCounter,
};
use crate::application::port::stream_registry::NotificationPayload;
use crate::application::port::PushNotifier;
use crate::domain::aggregate::Notification;
use crate::domain::value_object::{
    NotificationId, NotificationKind, ProfileId, SubjectId, SubjectKind,
};
use crate::error::NotificationError;

/// Writes a single notification record to ScyllaDB and dispatches it to any
/// active gRPC streaming subscriber.
///
/// The block gate and self-notification guard are enforced here to provide a
/// single authoritative enforcement point regardless of which worker triggers
/// the command.
pub struct CreateNotificationCommand {
    pub notification_id:   String,
    pub target_profile_id: String,
    pub sender_profile_id: String,
    pub kind:              i32,
    pub subject_kind:      i32,
    pub subject_id:        String,
}

impl Command for CreateNotificationCommand {}

impl Validate for CreateNotificationCommand {
    fn validate(&self) -> Result<(), Vec<FieldViolation>> {
        let mut v = Vec::new();
        if self.notification_id.trim().is_empty() {
            v.push(FieldViolation::new("notification_id", "NTF-VAL-001", "notification_id must not be empty"));
        }
        if self.target_profile_id.trim().is_empty() {
            v.push(FieldViolation::new("target_profile_id", "NTF-VAL-002", "target_profile_id must not be empty"));
        }
        if self.sender_profile_id.trim().is_empty() {
            v.push(FieldViolation::new("sender_profile_id", "NTF-VAL-003", "sender_profile_id must not be empty"));
        }
        if self.subject_id.trim().is_empty() {
            v.push(FieldViolation::new("subject_id", "NTF-VAL-004", "subject_id must not be empty"));
        }
        if v.is_empty() { Ok(()) } else { Err(v) }
    }
}

pub struct CreateNotificationHandler<R, C, U, S> {
    pub repository:   Arc<R>,
    pub block_cache:  Arc<C>,
    pub counter:      Arc<U>,
    pub stream_registry: Arc<S>,
    /// Realtime push stream (`notification.v1.events`). A trait object so the
    /// harness injects a no-op without a broker.
    pub publisher:    Arc<dyn NotificationEventPublisher>,
    /// Sends it as a push (#654).
    pub push:         Arc<dyn PushNotifier>,
}

impl<R, C, U, S> CommandHandler<CreateNotificationCommand>
    for CreateNotificationHandler<R, C, U, S>
where
    R: NotificationRepository,
    C: BlockCache,
    U: UnreadCounter,
    S: StreamRegistry,
{
    type Error = NotificationError;

    async fn handle(
        &self,
        envelope: Envelope<CreateNotificationCommand>,
    ) -> Result<(), NotificationError> {
        let cmd = &envelope.payload;

        let target_id = ProfileId::try_from(cmd.target_profile_id.as_str())?;
        let sender_id = ProfileId::try_from(cmd.sender_profile_id.as_str())?;
        let kind      = NotificationKind::from_proto(cmd.kind)?;
        let subj_kind = SubjectKind::from_proto(cmd.subject_kind)?;
        let subj_id   = SubjectId::try_from(cmd.subject_id.as_str())?;
        let ntf_id    = NotificationId::try_from(cmd.notification_id.as_str())?;

        // Self-notification guard.
        if target_id == sender_id {
            return Err(NotificationError::SelfNotification {
                profile_id: sender_id.as_str(),
            });
        }

        // Block gate — checked against the Redis cache, miss = not blocked.
        if self.block_cache.is_blocked(&sender_id, &target_id).await? {
            return Err(NotificationError::SenderBlocked {
                sender_id: sender_id.as_str(),
                target_id: target_id.as_str(),
            });
        }

        // Synchronous command path (not a Kafka redelivery), so wall-clock time and
        // the caller-supplied id are appropriate; idempotency is the client's concern.
        let notification = Notification::create(
            ntf_id,
            target_id,
            sender_id,
            kind,
            subj_kind,
            subj_id,
            chrono::Utc::now(),
        );

        self.repository.insert(&notification).await?;

        // Increment unread counters (Redis L1 + ScyllaDB counter).
        self.counter.increment(&target_id).await?;

        // Best-effort real-time push — failure does not roll back the write.
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
        self.push.notify(Arc::clone(&payload));
        self.stream_registry.broadcast(&target_id, payload);

        // Best-effort publish to notification.v1.events for out-of-process realtime
        // push. The record is already durable, so a publish failure must not fail
        // the command — mirrors the in-process broadcast above.
        let stream_event = NotificationStreamEvent {
            recipient_id:    target_id.as_str(),
            notification_id: ntf_id.as_str(),
            kind:            kind.as_str().to_owned(),
            created_at_ms:   notification.created_at().timestamp_millis(),
            payload:         serde_json::json!({
                "kind":         kind.as_str(),
                "sender_count": notification.sender_count(),
                "subject_id":   notification.subject_id().as_uuid().to_string(),
            }),
        };
        if let Err(error) = self.publisher.publish(&stream_event).await {
            tracing::warn!(
                %error,
                notification_id = %ntf_id,
                "notification.v1.events publish failed (best-effort; record is durable)"
            );
        }

        tracing::debug!(
            notification_id   = %ntf_id,
            target_profile_id = %target_id,
            sender_profile_id = %sender_id,
            kind              = kind.as_str(),
            "notification created"
        );

        Ok(())
    }
}
