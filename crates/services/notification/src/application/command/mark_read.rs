use std::sync::Arc;

use cqrs::{Command, CommandHandler, Envelope};
use uuid::Uuid;
use validate_core::{FieldViolation, Validate};

use crate::application::port::{NotificationRepository, UnreadCounter};
use crate::domain::value_object::ProfileId;
use crate::error::NotificationError;

// ── MarkReadCommand ───────────────────────────────────────────────────────────

/// Marks a single notification as read and decrements the unread counter.
///
/// The client must supply `created_at_ms` because ScyllaDB requires the full
/// compound clustering key `(created_at, notification_id)` for a point UPDATE.
pub struct MarkReadCommand {
    pub profile_id:      String,
    pub notification_id: String,
    pub created_at_ms:   i64,
}

impl Command for MarkReadCommand {}

impl Validate for MarkReadCommand {
    fn validate(&self) -> Result<(), Vec<FieldViolation>> {
        let mut v = Vec::new();
        if self.profile_id.trim().is_empty() {
            v.push(FieldViolation::new("profile_id", "NTF-VAL-010", "profile_id must not be empty"));
        }
        if self.notification_id.trim().is_empty() {
            v.push(FieldViolation::new("notification_id", "NTF-VAL-011", "notification_id must not be empty"));
        }
        if self.created_at_ms <= 0 {
            v.push(FieldViolation::new("created_at_ms", "NTF-VAL-012", "created_at_ms must be a positive Unix timestamp in milliseconds"));
        }
        if v.is_empty() { Ok(()) } else { Err(v) }
    }
}

pub struct MarkReadHandler<R, U> {
    pub repository: Arc<R>,
    pub counter:    Arc<U>,
}

impl<R, U> CommandHandler<MarkReadCommand> for MarkReadHandler<R, U>
where
    R: NotificationRepository,
    U: UnreadCounter,
{
    type Error = NotificationError;

    async fn handle(
        &self,
        envelope: Envelope<MarkReadCommand>,
    ) -> Result<(), NotificationError> {
        let cmd = &envelope.payload;

        let profile_id = ProfileId::try_from(cmd.profile_id.as_str())?;
        let ntf_id     = Uuid::parse_str(&cmd.notification_id)
            .map_err(|_| NotificationError::InvalidNotificationId(cmd.notification_id.clone()))?;

        let was_unread = self.repository
            .mark_read(&profile_id, ntf_id, cmd.created_at_ms)
            .await?;

        if was_unread {
            self.counter.decrement(&profile_id).await?;

            tracing::debug!(
                notification_id = %ntf_id,
                profile_id      = %profile_id,
                "notification marked as read, counter decremented"
            );
        }

        Ok(())
    }
}

// ── MarkAllReadCommand ────────────────────────────────────────────────────────

/// Resets the unread counter to 0 and records a read_horizon timestamp.
///
/// Does NOT update individual `is_read` flags in ScyllaDB — this would require
/// a full partition scan with no bound on cost. The client applies the
/// `read_horizon_ms` field from `ListNotificationsResponse` locally.
pub struct MarkAllReadCommand {
    pub profile_id: String,
}

impl Command for MarkAllReadCommand {}

impl Validate for MarkAllReadCommand {
    fn validate(&self) -> Result<(), Vec<FieldViolation>> {
        if self.profile_id.trim().is_empty() {
            return Err(vec![FieldViolation::new(
                "profile_id",
                "NTF-VAL-020",
                "profile_id must not be empty",
            )]);
        }
        Ok(())
    }
}

pub struct MarkAllReadHandler<R, U> {
    pub repository: Arc<R>,
    pub counter:    Arc<U>,
}

impl<R, U> CommandHandler<MarkAllReadCommand> for MarkAllReadHandler<R, U>
where
    R: NotificationRepository,
    U: UnreadCounter,
{
    type Error = NotificationError;

    async fn handle(
        &self,
        envelope: Envelope<MarkAllReadCommand>,
    ) -> Result<(), NotificationError> {
        let profile_id = ProfileId::try_from(envelope.payload.profile_id.as_str())?;
        let horizon_ms = chrono::Utc::now().timestamp_millis();

        tokio::try_join!(
            self.repository.reset_counter(&profile_id),
            self.counter.reset(&profile_id),
        )?;

        self.counter.set_read_horizon(&profile_id, horizon_ms).await?;

        tracing::debug!(
            profile_id  = %profile_id,
            horizon_ms,
            "all notifications marked as read"
        );

        Ok(())
    }
}

// ── ClearNotificationsCommand ─────────────────────────────────────────────────

/// Unread rows read back after a clear to recount the badge (#971): what is
/// left is what arrived after the bound, normally a handful.
const RECOUNT_MAX: i32 = 1_000;

/// Deletes the holder's notifications created at or before `up_to_ms` (#971:
/// "clear all", up to what they were shown, so one that arrives meanwhile
/// survives). Idempotent; the unread count becomes the unread left.
pub struct ClearNotificationsCommand {
    pub profile_id: String,
    /// The `created_at_ms` of the newest notification the holder was shown.
    pub up_to_ms:   i64,
}

impl Command for ClearNotificationsCommand {}

impl Validate for ClearNotificationsCommand {
    fn validate(&self) -> Result<(), Vec<FieldViolation>> {
        let mut v = Vec::new();
        if self.profile_id.trim().is_empty() {
            v.push(FieldViolation::new("profile_id", "NTF-VAL-020", "profile_id must not be empty"));
        }
        if self.up_to_ms <= 0 {
            v.push(FieldViolation::new("up_to_ms", "NTF-VAL-030", "up_to_ms is the newest notification shown (> 0)"));
        }
        if v.is_empty() { Ok(()) } else { Err(v) }
    }
}

pub struct ClearNotificationsHandler<R, U> {
    pub repository: Arc<R>,
    pub counter:    Arc<U>,
}

impl<R, U> CommandHandler<ClearNotificationsCommand> for ClearNotificationsHandler<R, U>
where
    R: NotificationRepository,
    U: UnreadCounter,
{
    type Error = NotificationError;

    async fn handle(&self, envelope: Envelope<ClearNotificationsCommand>) -> Result<(), NotificationError> {
        let cmd = &envelope.payload;
        if cmd.up_to_ms <= 0 {
            return Err(NotificationError::DomainViolation {
                field:   "up_to_ms".into(),
                message: "the newest notification shown (> 0)".into(),
            });
        }
        let profile_id = ProfileId::try_from(cmd.profile_id.as_str())?;
        // Never past now: what has not arrived cannot have been shown.
        let up_to_ms = cmd.up_to_ms.min(chrono::Utc::now().timestamp_millis());
        self.repository.delete_up_to(&profile_id, up_to_ms).await?;

        // The badge: the unread left (newer than the bound, after the read
        // horizon, not read on its own).
        let horizon = self.counter.get_read_horizon(&profile_id).await?;
        let mut unread = 0_i64;
        let (mut cursor, mut read) = (None, 0);
        loop {
            let (page, next) = self.repository.list_paginated(&profile_id, 100, cursor).await?;
            read += page.len() as i32;
            unread += page
                .iter()
                .filter(|n| !n.is_read && n.created_at.timestamp_millis() > horizon)
                .count() as i64;
            match (page.last(), next) {
                (Some(last), Some(_)) if read < RECOUNT_MAX => {
                    cursor = Some((last.created_at.timestamp_millis(), last.notification_id));
                }
                _ => break,
            }
        }
        self.repository.reset_counter(&profile_id).await?;
        if unread > 0 {
            self.repository.add_to_counter(&profile_id, unread).await?;
        }
        self.counter.set(&profile_id, unread).await?;
        tracing::debug!(profile_id = %profile_id, up_to_ms, unread, "notifications cleared");
        Ok(())
    }
}

