//! [`WalletStore`] on Postgres (migration 0001). Everything for an account
//! lives on its shard; each write runs in one transaction with the wallet
//! row locked (`FOR UPDATE`), so concurrent claims never double-credit, and
//! the ledger row lands with the balance it moved (`balance_after`).

use async_trait::async_trait;
use chrono::{DateTime, NaiveDate, Utc};
use postgres_storage::{StorageError, TransactionManager};
use sqlx::{PgConnection, PgPool, Postgres};
use tracing::instrument;
use uuid::Uuid;

use crate::application::port::{
    ClaimOutcome, ClaimResult, GemSpend, PackOutcome, SpendOutcome, TransactionCursor, WalletStore,
};
use crate::domain::{
    AccountId, ClaimDecision, ClaimPolicy, Currency, IdempotencyKey, PackDecision, StakePackPolicy, Transaction,
    TransactionKind, Wallet,
};
use crate::error::WalletError;

/// The starter gift's key: once per wallet.
const STARTER_KEY: &str = "starter-gems";

fn storage(e: sqlx::Error) -> WalletError {
    WalletError::Storage(StorageError::from(e))
}

#[derive(Clone)]
pub struct PgWalletStore {
    tx: TransactionManager,
}

impl PgWalletStore {
    pub fn new(tx: TransactionManager) -> Self {
        Self { tx }
    }

    fn pool(&self, account: &AccountId) -> Result<&PgPool, WalletError> {
        self.tx.pool_for(&account.as_uuid()).map_err(WalletError::Storage)
    }

    /// A transaction on the account's shard, the wallet opened if needed and
    /// its row locked.
    async fn locked(
        &self,
        account: &AccountId,
        starter_gems: i64,
        now: DateTime<Utc>,
    ) -> Result<(sqlx::Transaction<'static, Postgres>, Wallet), WalletError> {
        let mut tx = self.pool(account)?.begin().await.map_err(storage)?;
        ensure_open(&mut tx, account, starter_gems, now).await?;
        let wallet = read_wallet(&mut tx, account, true).await?;
        Ok((tx, wallet))
    }
}

/// The delta a key already moved, if it was used.
async fn replayed(conn: &mut PgConnection, account: &AccountId, key: &IdempotencyKey) -> Result<Option<i64>, WalletError> {
    let row: Option<(i64,)> =
        sqlx::query_as("SELECT delta FROM wallet_transactions WHERE account_id = $1 AND idempotency_key = $2")
            .bind(account.as_uuid())
            .bind(key.as_str())
            .fetch_optional(conn)
            .await
            .map_err(storage)?;
    Ok(row.map(|(delta,)| delta))
}

/// Writes a wallet's gems side (and its stake shots).
async fn write_gems(conn: &mut PgConnection, wallet: &Wallet, now: DateTime<Utc>) -> Result<(), WalletError> {
    sqlx::query("UPDATE wallets SET gems = $2, gems_spent = $3, stake_shots = $4, updated_at = $5 WHERE account_id = $1")
        .bind(wallet.account.as_uuid())
        .bind(wallet.gems)
        .bind(wallet.gems_spent)
        .bind(wallet.stake_shots)
        .bind(now)
        .execute(conn)
        .await
        .map_err(storage)?;
    Ok(())
}

/// A movement for the ledger.
struct Movement<'a> {
    currency:      Currency,
    delta:         i64,
    balance_after: i64,
    kind:          TransactionKind,
    ref_id:        Option<&'a str>,
}

type WalletRow = (
    Uuid,
    i64,
    i64,
    i64,
    i64,
    i64,
    i64,
    Option<DateTime<Utc>>,
    i32,
    Option<NaiveDate>,
    i32,
    i32,
);

const WALLET_COLUMNS: &str = "account_id, points, gems, points_earned, points_spent, gems_earned, gems_spent, \
                              last_claim_at, claimed_today, claimed_day, streak_days, stake_shots";

fn wallet_from(row: WalletRow) -> Wallet {
    let (account, points, gems, points_earned, points_spent, gems_earned, gems_spent, last_claim_at, claimed_today, claimed_day, streak_days, stake_shots) = row;
    Wallet {
        account: AccountId::from_uuid(account),
        points,
        gems,
        points_earned,
        points_spent,
        gems_earned,
        gems_spent,
        last_claim_at,
        claimed_today,
        claimed_day,
        streak_days,
        stake_shots,
    }
}

/// Opens the wallet if it has none, with its starter gift; idempotent.
async fn ensure_open(
    conn: &mut PgConnection,
    account: &AccountId,
    starter_gems: i64,
    now: DateTime<Utc>,
) -> Result<(), WalletError> {
    let opened = sqlx::query(
        "INSERT INTO wallets (account_id, gems, gems_earned, created_at, updated_at) VALUES ($1, $2, $2, $3, $3) \
         ON CONFLICT (account_id) DO NOTHING",
    )
    .bind(account.as_uuid())
    .bind(starter_gems)
    .bind(now)
    .execute(&mut *conn)
    .await
    .map_err(storage)?
    .rows_affected()
        == 1;
    if opened && starter_gems > 0 {
        let gift = Movement {
            currency:      Currency::Gems,
            delta:         starter_gems,
            balance_after: starter_gems,
            kind:          TransactionKind::StarterGift,
            ref_id:        None,
        };
        record(conn, account, gift, &IdempotencyKey::system(STARTER_KEY), now).await?;
    }
    Ok(())
}

async fn record(
    conn: &mut PgConnection,
    account: &AccountId,
    movement: Movement<'_>,
    key: &IdempotencyKey,
    now: DateTime<Utc>,
) -> Result<(), WalletError> {
    sqlx::query(
        "INSERT INTO wallet_transactions \
         (id, account_id, currency, delta, balance_after, kind, ref_id, idempotency_key, created_at) \
         VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)",
    )
    .bind(Uuid::now_v7())
    .bind(account.as_uuid())
    .bind(movement.currency.as_str())
    .bind(movement.delta)
    .bind(movement.balance_after)
    .bind(movement.kind.as_str())
    .bind(movement.ref_id)
    .bind(key.as_str())
    .bind(now)
    .execute(conn)
    .await
    .map_err(storage)?;
    Ok(())
}

async fn read_wallet(conn: &mut PgConnection, account: &AccountId, lock: bool) -> Result<Wallet, WalletError> {
    let sql = format!(
        "SELECT {WALLET_COLUMNS} FROM wallets WHERE account_id = $1{}",
        if lock { " FOR UPDATE" } else { "" }
    );
    let row: WalletRow = sqlx::query_as(&sql).bind(account.as_uuid()).fetch_one(conn).await.map_err(storage)?;
    Ok(wallet_from(row))
}

#[async_trait]
impl WalletStore for PgWalletStore {
    #[instrument(name = "wallet.open", skip(self))]
    async fn open(&self, account: &AccountId, starter_gems: i64, now: DateTime<Utc>) -> Result<Wallet, WalletError> {
        let mut tx = self.pool(account)?.begin().await.map_err(storage)?;
        ensure_open(&mut tx, account, starter_gems, now).await?;
        let wallet = read_wallet(&mut tx, account, false).await?;
        tx.commit().await.map_err(storage)?;
        Ok(wallet)
    }

    #[instrument(name = "wallet.claim", skip(self, key, policy))]
    async fn claim(
        &self,
        account: &AccountId,
        key: &IdempotencyKey,
        policy: &ClaimPolicy,
        starter_gems: i64,
        now: DateTime<Utc>,
    ) -> Result<ClaimResult, WalletError> {
        let (mut tx, mut wallet) = self.locked(account, starter_gems, now).await?;

        // A key already used: its first award, nothing new.
        if let Some(delta) = replayed(&mut tx, account, key).await? {
            tx.commit().await.map_err(storage)?;
            let awarded = i32::try_from(delta).map_err(|_| WalletError::LedgerInconsistent { reason: format!("claim of {delta}") })?;
            return Ok(ClaimResult { outcome: ClaimOutcome::Claimed, awarded, wallet });
        }

        let (outcome, awarded) = match wallet.decide_claim(policy, now) {
            ClaimDecision::Claim { awarded, streak } => {
                wallet.apply_claim(awarded, streak, now);
                sqlx::query(
                    "UPDATE wallets SET points = $2, points_earned = $3, last_claim_at = $4, claimed_today = $5, \
                     claimed_day = $6, streak_days = $7, updated_at = $4 WHERE account_id = $1",
                )
                .bind(account.as_uuid())
                .bind(wallet.points)
                .bind(wallet.points_earned)
                .bind(now)
                .bind(wallet.claimed_today)
                .bind(wallet.claimed_day)
                .bind(wallet.streak_days)
                .execute(&mut *tx)
                .await
                .map_err(storage)?;
                let movement = Movement {
                    currency:      Currency::Points,
                    delta:         i64::from(awarded),
                    balance_after: wallet.points,
                    kind:          TransactionKind::Claim,
                    ref_id:        None,
                };
                record(&mut tx, account, movement, key, now).await?;
                (ClaimOutcome::Claimed, awarded)
            }
            ClaimDecision::TooEarly { .. } => (ClaimOutcome::TooEarly, 0),
            ClaimDecision::DailyCapReached { .. } => (ClaimOutcome::DailyCapReached, 0),
        };
        tx.commit().await.map_err(storage)?;
        Ok(ClaimResult { outcome, awarded, wallet })
    }

    #[instrument(name = "wallet.buy_stake_pack", skip(self, key, policy))]
    async fn buy_stake_pack(
        &self,
        account: &AccountId,
        key: &IdempotencyKey,
        policy: &StakePackPolicy,
        starter_gems: i64,
        now: DateTime<Utc>,
    ) -> Result<(PackOutcome, Wallet), WalletError> {
        let (mut tx, mut wallet) = self.locked(account, starter_gems, now).await?;
        let outcome = if replayed(&mut tx, account, key).await?.is_some() {
            PackOutcome::Bought
        } else {
            match wallet.decide_stake_pack(policy) {
                PackDecision::Buy => {
                    wallet.apply_stake_pack(policy);
                    write_gems(&mut tx, &wallet, now).await?;
                    let movement = Movement {
                        currency:      Currency::Gems,
                        delta:         -policy.price_gems,
                        balance_after: wallet.gems,
                        kind:          TransactionKind::StakePack,
                        ref_id:        None,
                    };
                    record(&mut tx, account, movement, key, now).await?;
                    PackOutcome::Bought
                }
                PackDecision::StillActive => PackOutcome::StillActive,
                PackDecision::InsufficientGems => PackOutcome::InsufficientGems,
            }
        };
        tx.commit().await.map_err(storage)?;
        Ok((outcome, wallet))
    }

    #[instrument(name = "wallet.spend_gems", skip(self, key))]
    async fn spend_gems(
        &self,
        account: &AccountId,
        key: &IdempotencyKey,
        spend: &GemSpend,
        starter_gems: i64,
        now: DateTime<Utc>,
    ) -> Result<(SpendOutcome, Wallet), WalletError> {
        let (mut tx, mut wallet) = self.locked(account, starter_gems, now).await?;
        let outcome = if replayed(&mut tx, account, key).await?.is_some() {
            SpendOutcome::Spent
        } else if wallet.can_spend_gems(spend.amount) {
            wallet.spend_gems(spend.amount);
            write_gems(&mut tx, &wallet, now).await?;
            let movement = Movement {
                currency:      Currency::Gems,
                delta:         -spend.amount,
                balance_after: wallet.gems,
                kind:          spend.kind,
                ref_id:        spend.ref_id.as_deref(),
            };
            record(&mut tx, account, movement, key, now).await?;
            SpendOutcome::Spent
        } else {
            SpendOutcome::InsufficientGems
        };
        tx.commit().await.map_err(storage)?;
        Ok((outcome, wallet))
    }

    #[instrument(name = "wallet.history", skip(self))]
    async fn history(
        &self,
        account: &AccountId,
        currency: Option<Currency>,
        after: Option<TransactionCursor>,
        limit: usize,
    ) -> Result<Vec<Transaction>, WalletError> {
        let rows: Vec<(Uuid, String, i64, i64, String, Option<String>, DateTime<Utc>)> = sqlx::query_as(
            "SELECT id, currency, delta, balance_after, kind, ref_id, created_at FROM wallet_transactions \
             WHERE account_id = $1 AND ($2::text IS NULL OR currency = $2) \
               AND ($3::timestamptz IS NULL OR (created_at, id) < ($3, $4)) \
             ORDER BY created_at DESC, id DESC LIMIT $5",
        )
        .bind(account.as_uuid())
        .bind(currency.map(Currency::as_str))
        .bind(after.map(|a| a.created_at))
        .bind(after.map(|a| a.id))
        .bind(i64::try_from(limit).unwrap_or(i64::MAX))
        .fetch_all(self.pool(account)?)
        .await
        .map_err(storage)?;
        rows.into_iter()
            .map(|(id, currency, delta, balance_after, kind, ref_id, created_at)| {
                Ok(Transaction {
                    id,
                    account: *account,
                    currency: Currency::parse(&currency)
                        .ok_or_else(|| WalletError::LedgerInconsistent { reason: format!("currency {currency:?}") })?,
                    delta,
                    balance_after,
                    kind: TransactionKind::parse(&kind),
                    ref_id,
                    created_at,
                })
            })
            .collect()
    }

    #[instrument(name = "wallet.erase", skip(self))]
    async fn erase(&self, account: &AccountId) -> Result<bool, WalletError> {
        let mut tx = self.pool(account)?.begin().await.map_err(storage)?;
        sqlx::query("DELETE FROM wallet_transactions WHERE account_id = $1")
            .bind(account.as_uuid())
            .execute(&mut *tx)
            .await
            .map_err(storage)?;
        let erased = sqlx::query("DELETE FROM wallets WHERE account_id = $1")
            .bind(account.as_uuid())
            .execute(&mut *tx)
            .await
            .map_err(storage)?
            .rows_affected()
            > 0;
        tx.commit().await.map_err(storage)?;
        Ok(erased)
    }
}
