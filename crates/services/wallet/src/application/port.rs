//! The ledger's storage. Every write is atomic with its ledger row, on the
//! account's shard, the wallet row locked.

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use uuid::Uuid;

use crate::domain::event::WalletEvent;
use crate::domain::{
    AccountId, ClaimPolicy, Currency, IdempotencyKey, StakeAsk, StakePackPolicy, StakePolicy, StakeTarget, Transaction,
    TransactionKind, Wallet,
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
}

/// Who wrote a post or comment, and whether it can take likes.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TargetInfo {
    pub author_profile_id: String,
    /// Published and not taken down.
    pub stakeable:         bool,
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
    #[allow(clippy::too_many_arguments)]
    async fn stake(
        &self,
        account: &AccountId,
        key: &IdempotencyKey,
        target: &StakeTarget,
        ask: StakeAsk,
        policy: &StakePolicy,
        pack: &StakePackPolicy,
        starter_gems: i64,
        now: DateTime<Utc>,
    ) -> Result<StakeResult, WalletError>;

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
}
