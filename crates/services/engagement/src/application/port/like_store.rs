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

    /// Forgets who `account` is on each target (its account was deleted):
    /// its entry goes, the targets' counts stay (the points are kept,
    /// anonymously).
    async fn forget(&self, account: &str, targets: &[LikeTarget]) -> Result<(), EngagementError>;
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

    /// Notes that `account` was deleted, for as long as one of its stakes
    /// could still arrive: the stake consumer then drops them.
    async fn mark_erased(&self, account: &str) -> Result<(), EngagementError>;

    async fn is_erased(&self, account: &str) -> Result<bool, EngagementError>;

    /// Deletes `account`'s rows on each target as of `at_micros`: a record
    /// stamped earlier (a stake made before the deletion, landing late) stays
    /// deleted.
    async fn forget(&self, account: &str, targets: &[LikeTarget], at_micros: i64) -> Result<(), EngagementError>;

    /// Deletes what `account` liked as of `at_micros` (the export's
    /// partition), likewise.
    async fn forget_account(&self, account: &str, at_micros: i64) -> Result<(), EngagementError>;
}
