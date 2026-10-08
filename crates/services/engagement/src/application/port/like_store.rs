use async_trait::async_trait;

use crate::domain::value_object::LikeTarget;
use crate::error::EngagementError;

/// Likes per post and comment (#665: a like is a point): each account's
/// total on a target, and the target's sum. Redis-primary.
#[async_trait]
pub trait LikeStore: Send + Sync + 'static {
    /// Records `account`'s total on `target` — idempotent and order-proof: a
    /// total no larger than the one held changes nothing (a redelivered or
    /// late event). Returns the likes it added.
    async fn apply_total(&self, target: &LikeTarget, account: &str, total: i64) -> Result<i64, EngagementError>;

    /// Each target's like count, in order.
    async fn counts(&self, targets: &[LikeTarget]) -> Result<Vec<i64>, EngagementError>;

    /// `account`'s likes on each target, in order.
    async fn mine(&self, account: &str, targets: &[LikeTarget]) -> Result<Vec<i64>, EngagementError>;
}

/// The durable copy of the likes (Scylla), written by the stake consumer.
#[async_trait]
pub trait LikeLedger: Send + Sync + 'static {
    /// Records `account`'s total on `target` as of `at_micros` (the newer
    /// write wins).
    async fn record(
        &self,
        target: &LikeTarget,
        account: &str,
        profile_id: &str,
        total: i64,
        at_micros: i64,
    ) -> Result<(), EngagementError>;
}
