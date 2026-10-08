use async_trait::async_trait;
use chrono::{DateTime, Utc};

use crate::domain::value_object::LikeTarget;
use crate::error::EngagementError;

/// Likes per post and comment (#665: a like is a point): each account's
/// total on a target, and the target's sum. Redis-primary. A target's likers
/// expire after a while without a like (its count never does); until they are
/// rehydrated from the durable copy, an account missing from them is unknown,
/// not zero.
#[async_trait]
pub trait LikeStore: Send + Sync + 'static {
    /// Records `account`'s total on `target` — idempotent and order-proof: a
    /// total no larger than the one held changes nothing (a redelivered or
    /// late event). Returns the likes it added; `None` when `account` is
    /// unknown on `target` (rehydrate it first).
    async fn apply_total(&self, target: &LikeTarget, account: &str, total: i64) -> Result<Option<i64>, EngagementError>;

    /// Each target's like count, in order.
    async fn counts(&self, targets: &[LikeTarget]) -> Result<Vec<i64>, EngagementError>;

    /// `account`'s likes on each target, in order; `None` where unknown.
    async fn mine(&self, account: &str, targets: &[LikeTarget]) -> Result<Vec<Option<i64>>, EngagementError>;

    /// Loads `likers` (from the durable copy) into `target`'s, keeping any
    /// total it already holds (newer); `complete`: they were all of them, so
    /// an account still missing has none.
    async fn rehydrate(&self, target: &LikeTarget, likers: &[(String, i64)], complete: bool) -> Result<(), EngagementError>;

    /// Claims `target`'s rehydration for a minute: one at a time from reads.
    async fn claim_rehydration(&self, target: &LikeTarget) -> Result<bool, EngagementError>;

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

/// A deleted account's like on one target, as the durable copy keeps it:
/// the points under an anonymous liker id.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ForgottenLike {
    pub target:       LikeTarget,
    pub total:        i64,
    /// Stable for a given erasure ([`crate::application::erasure::anonymous_liker`]):
    /// forgetting again rewrites the same row.
    pub anonymous_id: uuid::Uuid,
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

    /// `account`'s total on `target`, if it liked it.
    async fn total_of(&self, target: &LikeTarget, account: &str) -> Result<Option<i64>, EngagementError>;

    /// Who liked `target` and how much, up to `limit` after the liker
    /// `after` (anonymous likers included: the sum is the count).
    async fn likers_of(
        &self,
        target: &LikeTarget,
        limit: i32,
        after: Option<&str>,
    ) -> Result<Vec<(String, i64)>, EngagementError>;

    /// Notes that `account` was deleted at `erased_at_micros` (the deletion's
    /// own time), for as long as one of its stakes could still arrive: the
    /// stake consumer then drops them.
    async fn mark_erased(&self, account: &str, erased_at_micros: i64) -> Result<(), EngagementError>;

    /// When `account` was deleted, if it was (and is still remembered).
    async fn erased_at(&self, account: &str) -> Result<Option<i64>, EngagementError>;

    /// Forgets `account` on each like as of `at_micros`, one target at a time
    /// and atomically: its row on the target becomes the anonymous one (same
    /// total; the count stays rebuildable from the durable copy), and its own
    /// row on the target goes. A record stamped earlier (a stake made before
    /// the deletion, landing late) stays deleted; a replay rewrites the same
    /// anonymous row.
    async fn forget(&self, account: &str, likes: &[ForgottenLike], at_micros: i64) -> Result<(), EngagementError>;

    /// Deletes what is left of `account`'s list as of `at_micros` (the
    /// export's partition), likewise.
    async fn forget_account(&self, account: &str, at_micros: i64) -> Result<(), EngagementError>;
}
