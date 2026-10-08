use async_trait::async_trait;

use crate::domain::value_object::PostId;
use crate::error::EngagementError;

/// A post's view, share and comment counters, from Redis (its likes are the
/// [`LikeStore`](crate::application::port::LikeStore)'s, #665).
#[derive(Debug, Default)]
pub struct PostEngagementSnapshot {
    pub view_count:    i64,
    pub share_count:   i64,
    pub comment_count: i64,
}

/// Port for the Redis-primary atomic scoring layer.
///
/// All write methods are O(1) and involve a single Redis round-trip (INCR). The hot path never touches ScyllaDB.
#[async_trait]
pub trait ScoreStore: Send + Sync + 'static {
    /// Increments the view counter for `post_id` and marks it dirty for flush.
    async fn incr_view(&self, post_id: &PostId) -> Result<(), EngagementError>;

    /// Increments the share counter for `post_id` and marks it dirty for flush.
    async fn incr_share(&self, post_id: &PostId) -> Result<(), EngagementError>;

    /// Increments the comment counter for `post_id`.
    async fn incr_comment(&self, post_id: &PostId) -> Result<(), EngagementError>;

    /// Decrements the comment counter for `post_id`.
    async fn decr_comment(&self, post_id: &PostId) -> Result<(), EngagementError>;

    /// Reads the full engagement snapshot from Redis. Used by the query handler.
    async fn get_snapshot(&self, post_id: &PostId) -> Result<PostEngagementSnapshot, EngagementError>;
}
