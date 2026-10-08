//! The ledger's storage. Every write is atomic with its ledger row, on the
//! account's shard, the wallet row locked.

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use uuid::Uuid;

use crate::domain::{AccountId, ClaimPolicy, Currency, IdempotencyKey, StakePackPolicy, Transaction, TransactionKind, Wallet};
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
