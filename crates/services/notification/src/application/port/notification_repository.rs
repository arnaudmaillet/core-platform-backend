use async_trait::async_trait;
use chrono::{DateTime, Utc};
use uuid::Uuid;

use crate::domain::aggregate::Notification;
use crate::domain::value_object::{NotificationKind, ProfileId, SubjectKind};
use crate::error::NotificationError;

/// Feed projection returned by list operations.
/// Sourced directly from `notification.notifications_by_profile`.
#[derive(Debug, Clone)]
pub struct NotificationSummary {
    pub notification_id:   Uuid,
    pub target_profile_id: Uuid,
    pub sender_profile_id: Uuid,
    pub sample_sender_ids: Vec<Uuid>,
    pub sender_count:      i32,
    pub kind:              NotificationKind,
    pub subject_kind:      SubjectKind,
    pub subject_id:        Uuid,
    pub created_at:        DateTime<Utc>,
    pub is_read:           bool,
}

#[async_trait]
pub trait NotificationRepository: Send + Sync + 'static {
    /// Inserts a new notification row into `notifications_by_profile`.
    async fn insert(&self, notification: &Notification) -> Result<(), NotificationError>;

    /// Fetches a paginated slice of the activity feed for `profile_id`.
    ///
    /// Cursor is `(created_at_ms, notification_id)` from the last row of the
    /// previous page. Returns `(summaries, next_page_token)` where
    /// `next_page_token` is `None` when no more rows exist.
    async fn list_paginated(
        &self,
        profile_id: &ProfileId,
        limit:      i32,
        cursor:     Option<(i64, Uuid)>,
    ) -> Result<(Vec<NotificationSummary>, Option<String>), NotificationError>;

    /// Marks a single notification as read.
    ///
    /// Returns `true` if the notification was previously unread (so the caller
    /// can decrement the unread counter), `false` if it was already read.
    /// Returns `NotificationError::NotificationNotFound` if the row does not exist.
    async fn mark_read(
        &self,
        profile_id:      &ProfileId,
        notification_id: Uuid,
        created_at_ms:   i64,
    ) -> Result<bool, NotificationError>;

    /// Deletes a notification (a retracted one: a withdrawn follow request).
    /// Absent is fine.
    async fn delete(
        &self,
        profile_id:      &ProfileId,
        notification_id: Uuid,
        created_at_ms:   i64,
    ) -> Result<(), NotificationError>;

    /// Increments the ScyllaDB unread counter for `profile_id` by 1.
    async fn increment_counter(&self, profile_id: &ProfileId) -> Result<(), NotificationError>;

    /// Decrements the ScyllaDB unread counter for `profile_id` by 1 (floor: 0).
    async fn decrement_counter(&self, profile_id: &ProfileId) -> Result<(), NotificationError>;

    /// Deletes the ScyllaDB counter row for `profile_id`, effectively resetting to 0.
    async fn reset_counter(&self, profile_id: &ProfileId) -> Result<(), NotificationError>;

    /// Reads the current ScyllaDB unread counter value. Used as Redis fallback.
    async fn read_counter(&self, profile_id: &ProfileId) -> Result<i64, NotificationError>;
}
