use async_trait::async_trait;
use chrono::{DateTime, Utc};

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

/// One target an account liked, for its GDPR export.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AccountLike {
    pub target:     LikeTarget,
    /// The account's points on it.
    pub total:      i64,
    /// The profile that liked last.
    pub profile_id: String,
    pub liked_at:   DateTime<Utc>,
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

    /// What `account` liked, in target order, up to `limit` after `after`
    /// (`kind:id`; the GDPR export).
    async fn list_by_account(
        &self,
        account: &str,
        limit: i32,
        after: Option<&LikeTarget>,
    ) -> Result<Vec<AccountLike>, EngagementError>;
}
