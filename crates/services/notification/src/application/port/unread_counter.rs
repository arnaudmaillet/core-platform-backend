use async_trait::async_trait;

use crate::domain::value_object::ProfileId;
use crate::error::NotificationError;

/// Port for managing the per-profile unread notification badge count.
///
/// The implementation uses Redis as the hot-path (L1) and falls back to the
/// ScyllaDB counter table on cache miss. Both layers must be kept in sync by
/// the caller.
#[async_trait]
pub trait UnreadCounter: Send + Sync + 'static {
    /// Increments the counter, capped at the configured `unread_cap`.
    async fn increment(&self, profile_id: &ProfileId) -> Result<(), NotificationError>;

    /// Idempotent increment, guarded by a one-shot claim on `dedupe_key`.
    ///
    /// The first call for a given `dedupe_key` claims it and increments both the
    /// Redis L1 counter and the ScyllaDB counter; subsequent calls (e.g. a Kafka
    /// redelivery of the same event) observe the existing claim and do nothing.
    /// Returns `true` when the counter was actually incremented, `false` when the
    /// event was a duplicate. Used by the at-least-once Kafka workers.
    async fn increment_once(
        &self,
        profile_id: &ProfileId,
        dedupe_key: &str,
    ) -> Result<bool, NotificationError>;

    /// Decrements the counter. No-op if the counter is already 0.
    async fn decrement(&self, profile_id: &ProfileId) -> Result<(), NotificationError>;

    /// Resets the counter to 0 (mark-all-read path).
    async fn reset(&self, profile_id: &ProfileId) -> Result<(), NotificationError>;

    /// Sets the counter to `count` (#971: the unread left after a clear).
    async fn set(&self, profile_id: &ProfileId, count: i64) -> Result<(), NotificationError>;

    /// Returns the current unread count. Populates Redis from ScyllaDB on miss.
    async fn get(&self, profile_id: &ProfileId) -> Result<i64, NotificationError>;

    /// Sets the read_horizon timestamp (Unix ms) for the mark-all-read operation.
    /// The client applies this horizon locally when rendering the feed.
    async fn set_read_horizon(
        &self,
        profile_id: &ProfileId,
        horizon_ms: i64,
    ) -> Result<(), NotificationError>;

    /// Returns the current read_horizon timestamp, or 0 if never set.
    async fn get_read_horizon(&self, profile_id: &ProfileId) -> Result<i64, NotificationError>;
}
