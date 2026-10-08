//! The ledger's storage. Every write is atomic with its ledger row, on the
//! account's shard, the wallet row locked.

use async_trait::async_trait;
use chrono::{DateTime, NaiveDate, Utc};
use uuid::Uuid;

use crate::domain::event::WalletEvent;
use crate::domain::{
    AccountId, Allocation, ClaimPolicy, Currency, DayPosition, DuePosition, IdempotencyKey, Observed, Settlement, StakeAsk, StakePackPolicy,
    StakePolicy, StakeTarget, Transaction, TransactionKind, Wallet,
};
use crate::error::WalletError;

/// How a claim ended.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ClaimOutcome {
    Claimed,
    TooEarly,
    DailyCapReached,
}

/// A claim's result and the wallet after it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ClaimResult {
    pub outcome: ClaimOutcome,
    /// Points credited (the first award, on a replayed key); 0 unless claimed.
    pub awarded: i32,
    pub wallet:  Wallet,
}

/// How buying a stake pack ended.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PackOutcome {
    Bought,
    StillActive,
    InsufficientGems,
}

/// How a gem spend ended.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SpendOutcome {
    Spent,
    InsufficientGems,
}

/// A gem spend asked by another service.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GemSpend {
    pub amount: i64,
    pub kind:   TransactionKind,
    pub ref_id: Option<String>,
}

/// How a batch of likes ended.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StakeOutcome {
    Staked,
    InsufficientBalance,
    TargetNotStakeable,
    RateLimited,
    TargetCapReached,
    NoStakeShots,
    ShotDoesNotFit,
    Expired,
    OwnContent,
}

/// A batch's result.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StakeResult {
    pub outcome:  StakeOutcome,
    /// Points moved (the first result's, on a replayed key).
    pub spent:    i64,
    /// The account's points on the target now.
    pub my_total: i64,
    /// This batch was the account's first on the target.
    pub first:    bool,
    pub wallet:   Wallet,
    /// The announcement written with a fresh stake (none on a replay).
    pub outbox:   Option<OutboxEvent>,
}

/// Who staked and on whose content: what the announcement names.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StakeAnnouncement {
    pub profile_id:        String,
    pub author_profile_id: String,
}

/// An event written to the outbox, to publish.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OutboxEvent {
    pub id:      Uuid,
    pub account: AccountId,
    pub event:   WalletEvent,
}

/// Who wrote a post or comment, and whether it can take likes.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TargetInfo {
    pub author_profile_id: String,
    /// Published and not taken down.
    pub stakeable:         bool,
}

/// Whether a reader may see an author's content (social-graph `CheckAccess`):
/// a like needs the content visible to the one who likes.
#[async_trait]
pub trait AudienceCheck: Send + Sync + 'static {
    /// `viewers`: the reader's profiles. `true` only for content fully visible.
    async fn visible(&self, viewers: &[String], author_profile_id: &str) -> Result<bool, WalletError>;
}

/// The posts and comments likes land on (post, comment over the mesh).
#[async_trait]
pub trait TargetDirectory: Send + Sync + 'static {
    /// `None`: no such post or comment.
    async fn target(&self, target: &StakeTarget) -> Result<Option<TargetInfo>, WalletError>;
}

/// Announces the wallet's events (`wallet.v1.events`).
#[async_trait]
pub trait EventPublisher: Send + Sync + 'static {
    async fn publish(&self, event: &WalletEvent) -> Result<(), WalletError>;
}

/// Where a history page starts (exclusive): the last row of the previous one.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TransactionCursor {
    pub created_at: DateTime<Utc>,
    pub id:         Uuid,
}

#[async_trait]
pub trait WalletStore: Send + Sync + 'static {
    /// The account's wallet, opened now if it has none — with
    /// `starter_gems`, recorded as a starter gift.
    async fn open(&self, account: &AccountId, starter_gems: i64, now: DateTime<Utc>) -> Result<Wallet, WalletError>;

    /// Claims the hourly reward (the wallet opened if needed, then locked):
    /// a key already used answers its first award; otherwise the policy
    /// decides, and a claim is credited with its ledger row.
    async fn claim(
        &self,
        account: &AccountId,
        key: &IdempotencyKey,
        policy: &ClaimPolicy,
        starter_gems: i64,
        now: DateTime<Utc>,
    ) -> Result<ClaimResult, WalletError>;

    /// Buys the stake pack (the wallet opened if needed, then locked): a key
    /// already used answers `Bought`; otherwise the policy decides, and a
    /// purchase is charged with its ledger row.
    async fn buy_stake_pack(
        &self,
        account: &AccountId,
        key: &IdempotencyKey,
        policy: &StakePackPolicy,
        starter_gems: i64,
        now: DateTime<Utc>,
    ) -> Result<(PackOutcome, Wallet), WalletError>;

    /// Spends gems (the wallet opened if needed, then locked): a key already
    /// used answers `Spent`; otherwise charged with its ledger row when the
    /// balance allows.
    async fn spend_gems(
        &self,
        account: &AccountId,
        key: &IdempotencyKey,
        spend: &GemSpend,
        starter_gems: i64,
        now: DateTime<Utc>,
    ) -> Result<(SpendOutcome, Wallet), WalletError>;

    /// Stakes a batch (the wallet opened if needed, then locked): a key
    /// already used answers its first result; otherwise the policy decides
    /// from what the account put on the target and staked in the last hour
    /// (since `hour_ago`), and a stake moves the points, the target's total
    /// and its ledger row together.
    /// A fresh stake writes its announcement to the outbox in the same
    /// transaction (`StakeResult::outbox`).
    #[allow(clippy::too_many_arguments)]
    async fn stake(
        &self,
        account: &AccountId,
        key: &IdempotencyKey,
        target: &StakeTarget,
        ask: StakeAsk,
        announcement: &StakeAnnouncement,
        policy: &StakePolicy,
        pack: &StakePackPolicy,
        starter_gems: i64,
        now: DateTime<Utc>,
    ) -> Result<StakeResult, WalletError>;

    /// Claims up to `limit` outbox events not yet published and not leased
    /// to another replica, oldest first (every shard), leasing them until
    /// `lease_until`: concurrent drainers share the rows, never the same one.
    async fn claim_unpublished(
        &self,
        limit: i64,
        now: DateTime<Utc>,
        lease_until: DateTime<Utc>,
    ) -> Result<Vec<OutboxEvent>, WalletError>;

    /// Marks an outbox event published.
    async fn mark_published(&self, event: &OutboxEvent, at: DateTime<Utc>) -> Result<(), WalletError>;

    /// Drops outbox events published before `before`; returns how many.
    async fn prune_outbox(&self, before: DateTime<Utc>) -> Result<u64, WalletError>;

    /// The account's points on `target`.
    async fn staked_on(&self, account: &AccountId, target: &StakeTarget) -> Result<i64, WalletError>;

    /// The account's transactions, newest first, after `after`.
    async fn history(
        &self,
        account: &AccountId,
        currency: Option<Currency>,
        after: Option<TransactionCursor>,
        limit: usize,
    ) -> Result<Vec<Transaction>, WalletError>;

    /// Erases the account's wallet and history; `false` when it had none.
    async fn erase(&self, account: &AccountId) -> Result<bool, WalletError>;

    /// Claims up to `limit` positions whose first stake was at or before
    /// `due_before`, not settled and not leased to another replica, oldest
    /// first (every shard), leasing them until `lease_until`.
    async fn claim_due_positions(
        &self,
        limit: i64,
        due_before: DateTime<Utc>,
        now: DateTime<Utc>,
        lease_until: DateTime<Utc>,
    ) -> Result<Vec<DuePosition>, WalletError>;

    /// Records a settlement and marks its position settled, at once; a
    /// position already settled keeps its first settlement.
    async fn record_settlement(&self, settlement: &Settlement) -> Result<(), WalletError>;

    /// The UTC days before `before` whose envelope is not computed yet
    /// (settlements not counted, or a computation left unfinished), oldest
    /// first.
    async fn pending_envelope_days(&self, before: NaiveDate) -> Result<Vec<NaiveDate>, WalletError>;

    /// Leases `day`'s envelope to this replica until `lease_until`; `false`
    /// when it is computed already or leased to another.
    async fn claim_envelope_day(
        &self,
        day: NaiveDate,
        now: DateTime<Utc>,
        lease_until: DateTime<Utc>,
    ) -> Result<bool, WalletError>;

    /// The positions settled during `day` (every shard).
    async fn day_positions(&self, day: NaiveDate) -> Result<Vec<DayPosition>, WalletError>;

    /// Records each position's share of `day`'s envelope.
    async fn record_allocations(
        &self,
        day: NaiveDate,
        allocations: &[(DayPosition, Allocation)],
    ) -> Result<(), WalletError>;

    /// Marks `day`'s envelope computed, with its summary.
    async fn complete_envelope_day(&self, day: NaiveDate, summary: &EnvelopeSummary) -> Result<(), WalletError>;
}

/// A day's envelope, as computed.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EnvelopeSummary {
    pub pool:        i64,
    pub allocated:   i64,
    pub positions:   i64,
    pub targets:     i64,
    pub model:       &'static str,
    pub computed_at: DateTime<Utc>,
}

/// What a post or comment came to, for the settlement (#665): engagement's
/// `GetLikePositions`, over the mesh.
#[async_trait]
pub trait LikePositions: Send + Sync + 'static {
    /// `account`'s position on each target (at most 100), in order.
    async fn positions(&self, account: &AccountId, targets: &[StakeTarget]) -> Result<Vec<Observed>, WalletError>;
}
