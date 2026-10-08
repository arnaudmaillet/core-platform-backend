use async_trait::async_trait;

use crate::domain::value_object::PostId;
use crate::error::EngagementError;

/// The durable copy of a post's view, share and comment counters (Scylla),
/// flushed from Redis by the background workers.
#[async_trait]
pub trait CounterLedger: Send + Sync + 'static {
    /// Applies a view/share/comment counter delta to the counter table.
    async fn apply_interaction_delta(
        &self,
        post_id:       &PostId,
        view_delta:    i64,
        share_delta:   i64,
        comment_delta: i64,
    ) -> Result<(), EngagementError>;
}
