use async_trait::async_trait;
use chrono::{DateTime, Utc};

use crate::domain::value_object::LikeTarget;
use crate::error::EngagementError;

/// An account's position on a target (#665): its points, and the target's
/// like count just before its first like — how early it came (the
/// settlement's earliness). `arrival` is unknown for likes recorded before
/// it was kept.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct Position {
    pub total:   i64,
    pub arrival: Option<i64>,
}

/// What applying a total did: the likes it added, and the account's arrival
/// on the target (returned on every application, so a redelivery can record
/// it again).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Applied {
    pub added:   i64,
    pub arrival: Option<i64>,
}

/// Likes per post and comment (#665: a like is a point): each account's
/// total on a target, and the target's sum. Redis-primary. A target's likers
/// expire after a while without a like (its count never does); until they are
/// rehydrated from the durable copy, an account missing from them is unknown,
/// not zero.
#[async_trait]
pub trait LikeStore: Send + Sync + 'static {
    /// Records `account`'s total on `target` — idempotent and order-proof: a
    /// total no larger than the one held changes nothing (a redelivered or
    /// late event). On the account's first like it also keeps the target's
    /// count just before it (its arrival). `None` when `account` is unknown
    /// on `target` (rehydrate it first).
    async fn apply_total(&self, target: &LikeTarget, account: &str, total: i64) -> Result<Option<Applied>, EngagementError>;

    /// Each target's like count, in order.
    async fn counts(&self, targets: &[LikeTarget]) -> Result<Vec<i64>, EngagementError>;

    /// `account`'s position on each target, in order (a zero total where it
    /// has none); `None` where unknown.
    async fn positions(&self, account: &str, targets: &[LikeTarget]) -> Result<Vec<Option<Position>>, EngagementError>;

    /// `account`'s likes on each target, in order; `None` where unknown.
    async fn mine(&self, account: &str, targets: &[LikeTarget]) -> Result<Vec<Option<i64>>, EngagementError> {
        Ok(self.positions(account, targets).await?.into_iter().map(|p| p.map(|p| p.total)).collect())
    }

    /// Loads `likers` (from the durable copy) into `target`'s, keeping any
    /// position it already holds (newer); `complete`: they were all of them,
    /// so an account still missing has none.
    async fn rehydrate(&self, target: &LikeTarget, likers: &[(String, Position)], complete: bool) -> Result<(), EngagementError>;

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
    /// write wins), and its arrival when known (the same on every write).
    async fn record(
        &self,
        target: &LikeTarget,
        account: &str,
        profile_id: &str,
        position: Position,
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

    /// `account`'s position on `target`, if it liked it.
    async fn position_of(&self, target: &LikeTarget, account: &str) -> Result<Option<Position>, EngagementError>;

    /// Who liked `target` and their positions, up to `limit` after the liker
    /// `after` (anonymous likers included: the sum is the count).
    async fn likers_of(
        &self,
        target: &LikeTarget,
        limit: i32,
        after: Option<&str>,
    ) -> Result<Vec<(String, Position)>, EngagementError>;

    /// Notes that `account` was deleted at `erased_at_micros` (the deletion's
    /// own time), for as long as one of its stakes could still arrive: the
    /// stake consumer then drops them.
    async fn mark_erased(&self, account: &str, erased_at_micros: i64) -> Result<(), EngagementError>;

    /// When `account` was deleted, if it was (and is still remembered).
    async fn erased_at(&self, account: &str) -> Result<Option<i64>, EngagementError>;

    /// Which of `accounts` were deleted (and are still remembered); ids that
    /// are not accounts (anonymous likers) are never among them.
    async fn erased_among(&self, accounts: &[String]) -> Result<Vec<String>, EngagementError>;

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
