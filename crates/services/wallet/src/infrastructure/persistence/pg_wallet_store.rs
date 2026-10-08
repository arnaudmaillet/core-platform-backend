//! [`WalletStore`] on Postgres (migration 0001). Everything for an account
//! lives on its shard; each write runs in one transaction with the wallet
//! row locked (`FOR UPDATE`), so concurrent claims never double-credit, and
//! the ledger row lands with the balance it moved (`balance_after`).

use async_trait::async_trait;
use chrono::{DateTime, NaiveDate, TimeDelta, Utc};
use postgres_storage::{StorageError, TransactionManager};
use sqlx::{PgConnection, PgPool, Postgres};
use tracing::instrument;
use uuid::Uuid;

use crate::application::port::{
    ClaimOutcome, ClaimResult, EnvelopeSummary, GemSpend, OutboxEvent, PackOutcome, SpendOutcome, StakeAnnouncement, StakeOutcome,
    StakeResult, TransactionCursor, WalletStore,
};
use crate::domain::event::{StakeCommitted, WalletEvent};
use crate::domain::{
    AccountId, Allocation, ClaimDecision, ClaimPolicy, Currency, DayPosition, DuePosition, IdempotencyKey, PackDecision, Settlement, StakeAsk,
    StakeDecision, StakePackPolicy, StakePolicy, StakeTarget, Transaction, TransactionKind, Wallet,
};
use crate::error::WalletError;

/// How long the writer of a stake holds its outbox row (it publishes right
/// after the commit; the drainers take it only past this).
const WRITER_LEASE_SECS: i64 = 30;

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

    /// Where `envelope_days` lives: the nil UUID's shard, the same on every
    /// replica (the pools' own order is not).
    fn envelope_pool(&self) -> Result<&PgPool, WalletError> {
        self.tx.pool_for(&Uuid::nil()).map_err(WalletError::Storage)
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

/// What `account` put on `target`, and its first batch's key.
async fn on_target(
    conn: &mut PgConnection,
    account: &AccountId,
    target: &StakeTarget,
) -> Result<Option<(i64, String)>, WalletError> {
    sqlx::query_as("SELECT total, first_key FROM stakes WHERE account_id = $1 AND target_kind = $2 AND target_id = $3")
        .bind(account.as_uuid())
        .bind(target.kind())
        .bind(target.id())
        .fetch_optional(conn)
        .await
        .map_err(storage)
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

    #[instrument(name = "wallet.stake", skip(self, key, announcement, policy, pack))]
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
    ) -> Result<StakeResult, WalletError> {
        let (mut tx, mut wallet) = self.locked(account, starter_gems, now).await?;
        let before = on_target(&mut tx, account, target).await?;
        let on = before.as_ref().map_or(0, |(total, _)| *total);

        // A key already used: its first result, nothing new.
        if let Some(delta) = replayed(&mut tx, account, key).await? {
            tx.commit().await.map_err(storage)?;
            let first = before.is_some_and(|(_, first_key)| first_key == key.as_str());
            return Ok(StakeResult { outcome: StakeOutcome::Staked, spent: -delta, my_total: on, first, wallet, outbox: None });
        }

        let (last_hour,): (i64,) = sqlx::query_as(
            "SELECT COALESCE(SUM(-delta), 0)::bigint FROM wallet_transactions \
             WHERE account_id = $1 AND kind = 'stake' AND created_at > $2",
        )
        .bind(account.as_uuid())
        .bind(now - chrono::TimeDelta::hours(1))
        .fetch_one(&mut *tx)
        .await
        .map_err(storage)?;

        let outcome = match wallet.decide_stake(ask, on, last_hour, policy, pack) {
            StakeDecision::Stake { points, shot } => {
                wallet.apply_stake(points, shot);
                sqlx::query(
                    "UPDATE wallets SET points = $2, points_spent = $3, stake_shots = $4, updated_at = $5 WHERE account_id = $1",
                )
                .bind(account.as_uuid())
                .bind(wallet.points)
                .bind(wallet.points_spent)
                .bind(wallet.stake_shots)
                .bind(now)
                .execute(&mut *tx)
                .await
                .map_err(storage)?;
                sqlx::query(
                    "INSERT INTO stakes (account_id, target_kind, target_id, total, first_key, first_at, last_at) \
                     VALUES ($1, $2, $3, $4, $5, $6, $6) \
                     ON CONFLICT (account_id, target_kind, target_id) \
                     DO UPDATE SET total = stakes.total + EXCLUDED.total, last_at = EXCLUDED.last_at",
                )
                .bind(account.as_uuid())
                .bind(target.kind())
                .bind(target.id())
                .bind(points)
                .bind(key.as_str())
                .bind(now)
                .execute(&mut *tx)
                .await
                .map_err(storage)?;
                let reference = target.reference();
                let movement = Movement {
                    currency:      Currency::Points,
                    delta:         -points,
                    balance_after: wallet.points,
                    kind:          TransactionKind::Stake,
                    ref_id:        Some(&reference),
                };
                record(&mut tx, account, movement, key, now).await?;
                // The announcement, in the same transaction: never a stake
                // without its event.
                let outbox = OutboxEvent {
                    id: Uuid::now_v7(),
                    account: *account,
                    event: WalletEvent::StakeCommitted(StakeCommitted {
                        account_id:        account.to_string(),
                        profile_id:        announcement.profile_id.clone(),
                        target_kind:       target.kind().to_owned(),
                        target_id:         target.id().to_owned(),
                        author_profile_id: announcement.author_profile_id.clone(),
                        points,
                        total:             on + points,
                        first:             before.is_none(),
                        stake_key:         key.as_str().to_owned(),
                        staked_at:         now,
                    }),
                };
                // Leased to this replica, which publishes it right after the
                // commit; the drainers only take it if that fails.
                sqlx::query(
                    "INSERT INTO wallet_outbox (id, account_id, event, created_at, claimed_until) \
                     VALUES ($1, $2, $3, $4, $5)",
                )
                .bind(outbox.id)
                .bind(account.as_uuid())
                .bind(sqlx::types::Json(&outbox.event))
                .bind(now)
                .bind(now + chrono::TimeDelta::seconds(WRITER_LEASE_SECS))
                    .execute(&mut *tx)
                    .await
                    .map_err(storage)?;
                tx.commit().await.map_err(storage)?;
                return Ok(StakeResult {
                    outcome: StakeOutcome::Staked,
                    spent: points,
                    my_total: on + points,
                    first: before.is_none(),
                    wallet,
                    outbox: Some(outbox),
                });
            }
            StakeDecision::InsufficientBalance => StakeOutcome::InsufficientBalance,
            StakeDecision::RateLimited => StakeOutcome::RateLimited,
            StakeDecision::TargetCapReached => StakeOutcome::TargetCapReached,
            StakeDecision::NoStakeShots => StakeOutcome::NoStakeShots,
            StakeDecision::ShotDoesNotFit => StakeOutcome::ShotDoesNotFit,
        };
        tx.commit().await.map_err(storage)?;
        Ok(StakeResult { outcome, spent: 0, my_total: on, first: false, wallet, outbox: None })
    }

    #[instrument(name = "wallet.outbox.claim", skip(self))]
    async fn claim_unpublished(
        &self,
        limit: i64,
        now: DateTime<Utc>,
        lease_until: DateTime<Utc>,
    ) -> Result<Vec<OutboxEvent>, WalletError> {
        let mut events = Vec::new();
        for pool in self.tx.all_pools() {
            let mut rows: Vec<(Uuid, Uuid, sqlx::types::Json<WalletEvent>, DateTime<Utc>)> = sqlx::query_as(
                "UPDATE wallet_outbox SET claimed_until = $3 WHERE id IN ( \
                   SELECT id FROM wallet_outbox \
                   WHERE published_at IS NULL AND (claimed_until IS NULL OR claimed_until < $2) \
                   ORDER BY created_at LIMIT $1 FOR UPDATE SKIP LOCKED) \
                 RETURNING id, account_id, event, created_at",
            )
            .bind(limit)
            .bind(now)
            .bind(lease_until)
            .fetch_all(pool)
            .await
            .map_err(storage)?;
            rows.sort_by_key(|(_, _, _, created_at)| *created_at);
            events.extend(rows.into_iter().map(|(id, account, event, _)| OutboxEvent {
                id,
                account: AccountId::from_uuid(account),
                event: event.0,
            }));
        }
        Ok(events)
    }

    #[instrument(name = "wallet.outbox.mark_published", skip(self, event))]
    async fn mark_published(&self, event: &OutboxEvent, at: DateTime<Utc>) -> Result<(), WalletError> {
        sqlx::query("UPDATE wallet_outbox SET published_at = $2 WHERE id = $1 AND published_at IS NULL")
            .bind(event.id)
            .bind(at)
            .execute(self.pool(&event.account)?)
            .await
            .map_err(storage)?;
        Ok(())
    }

    #[instrument(name = "wallet.outbox.prune", skip(self))]
    async fn prune_outbox(&self, before: DateTime<Utc>) -> Result<u64, WalletError> {
        let mut pruned = 0;
        for pool in self.tx.all_pools() {
            pruned += sqlx::query("DELETE FROM wallet_outbox WHERE published_at IS NOT NULL AND published_at < $1")
                .bind(before)
                .execute(pool)
                .await
                .map_err(storage)?
                .rows_affected();
        }
        Ok(pruned)
    }

    #[instrument(name = "wallet.staked_on", skip(self))]
    async fn staked_on(&self, account: &AccountId, target: &StakeTarget) -> Result<i64, WalletError> {
        let mut conn = self.pool(account)?.acquire().await.map_err(storage)?;
        Ok(on_target(&mut conn, account, target).await?.map_or(0, |(total, _)| total))
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
        // Unpublished announcements still go out (their likes counted);
        // published ones are the drainer's to prune.
        sqlx::query("DELETE FROM settlements WHERE account_id = $1")
            .bind(account.as_uuid())
            .execute(&mut *tx)
            .await
            .map_err(storage)?;
        sqlx::query("DELETE FROM stakes WHERE account_id = $1")
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

    #[instrument(name = "wallet.settlement.claim_due", skip(self))]
    async fn claim_due_positions(
        &self,
        limit: i64,
        due_before: DateTime<Utc>,
        now: DateTime<Utc>,
        lease_until: DateTime<Utc>,
    ) -> Result<Vec<DuePosition>, WalletError> {
        let mut due = Vec::new();
        for pool in self.tx.all_pools() {
            let rows: Vec<(Uuid, String, String, i64, DateTime<Utc>)> = sqlx::query_as(
                "UPDATE stakes SET settle_claimed_until = $4 WHERE (account_id, target_kind, target_id) IN ( \
                   SELECT account_id, target_kind, target_id FROM stakes \
                   WHERE settled_at IS NULL AND first_at <= $2 \
                     AND (settle_claimed_until IS NULL OR settle_claimed_until < $3) \
                   ORDER BY first_at LIMIT $1 FOR UPDATE SKIP LOCKED) \
                 RETURNING account_id, target_kind, target_id, total, first_at",
            )
            .bind(limit)
            .bind(due_before)
            .bind(now)
            .bind(lease_until)
            .fetch_all(pool)
            .await
            .map_err(storage)?;
            for (account, kind, id, total, first_at) in rows {
                // A row the service cannot read is left unsettled, never guessed.
                let target = match kind.as_str() {
                    "post" => StakeTarget::Post(id),
                    "comment" => StakeTarget::Comment(id),
                    _ => continue,
                };
                due.push(DuePosition { account: AccountId::from_uuid(account), target, points: total, first_at });
            }
        }
        due.sort_by_key(|p| p.first_at);
        Ok(due)
    }

    #[instrument(name = "wallet.settlement.record", skip(self, settlement))]
    async fn record_settlement(&self, settlement: &Settlement) -> Result<(), WalletError> {
        let account = settlement.account.as_uuid();
        let mut tx = self.pool(&settlement.account)?.begin().await.map_err(storage)?;
        sqlx::query(
            "INSERT INTO settlements (account_id, target_kind, target_id, points, first_at, settled_at, \
               count_on_arrival, count_at_settlement, earliness, pre_score, model) \
             VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11) \
             ON CONFLICT (account_id, target_kind, target_id) DO NOTHING",
        )
        .bind(account)
        .bind(settlement.target.kind())
        .bind(settlement.target.id())
        .bind(settlement.points)
        .bind(settlement.first_at)
        .bind(settlement.settled_at)
        .bind(settlement.count_on_arrival)
        .bind(settlement.count_at_settlement)
        .bind(settlement.earliness)
        .bind(settlement.pre_score)
        .bind(settlement.model)
        .execute(&mut *tx)
        .await
        .map_err(storage)?;
        sqlx::query(
            "UPDATE stakes SET settled_at = $4, settle_claimed_until = NULL \
             WHERE account_id = $1 AND target_kind = $2 AND target_id = $3 AND settled_at IS NULL",
        )
        .bind(account)
        .bind(settlement.target.kind())
        .bind(settlement.target.id())
        .bind(settlement.settled_at)
        .execute(&mut *tx)
        .await
        .map_err(storage)?;
        tx.commit().await.map_err(storage)?;
        Ok(())
    }

    #[instrument(name = "wallet.envelope.pending", skip(self))]
    async fn pending_envelope_days(&self, before: NaiveDate) -> Result<Vec<NaiveDate>, WalletError> {
        let before_at = day_start(before);
        let mut days = std::collections::BTreeSet::new();
        for pool in self.tx.all_pools() {
            let rows: Vec<(NaiveDate,)> = sqlx::query_as(
                "SELECT DISTINCT (settled_at AT TIME ZONE 'UTC')::date FROM settlements \
                 WHERE envelope_day IS NULL AND settled_at < $1",
            )
            .bind(before_at)
            .fetch_all(pool)
            .await
            .map_err(storage)?;
            days.extend(rows.into_iter().map(|(day,)| day));
        }
        // A computation left unfinished (its rows counted, its summary not).
        let unfinished: Vec<(NaiveDate,)> =
            sqlx::query_as("SELECT day FROM envelope_days WHERE computed_at IS NULL AND day < $1")
                .bind(before)
                .fetch_all(self.envelope_pool()?)
                .await
                .map_err(storage)?;
        days.extend(unfinished.into_iter().map(|(day,)| day));
        Ok(days.into_iter().collect())
    }

    #[instrument(name = "wallet.envelope.claim", skip(self))]
    async fn claim_envelope_day(
        &self,
        day: NaiveDate,
        now: DateTime<Utc>,
        lease_until: DateTime<Utc>,
    ) -> Result<bool, WalletError> {
        let claimed: Option<(NaiveDate,)> = sqlx::query_as(
            "INSERT INTO envelope_days (day, claimed_until) VALUES ($1, $3) \
             ON CONFLICT (day) DO UPDATE SET claimed_until = EXCLUDED.claimed_until \
             WHERE envelope_days.computed_at IS NULL \
               AND (envelope_days.claimed_until IS NULL OR envelope_days.claimed_until < $2) \
             RETURNING day",
        )
        .bind(day)
        .bind(now)
        .bind(lease_until)
        .fetch_optional(self.envelope_pool()?)
        .await
        .map_err(storage)?;
        Ok(claimed.is_some())
    }

    #[instrument(name = "wallet.envelope.positions", skip(self))]
    async fn day_positions(&self, day: NaiveDate) -> Result<Vec<DayPosition>, WalletError> {
        let (start, end) = (day_start(day), day_start(day) + TimeDelta::days(1));
        let mut positions = Vec::new();
        for pool in self.tx.all_pools() {
            let rows: Vec<(Uuid, String, String, i64, Option<f64>)> = sqlx::query_as(
                "SELECT account_id, target_kind, target_id, count_at_settlement, pre_score FROM settlements \
                 WHERE settled_at >= $1 AND settled_at < $2",
            )
            .bind(start)
            .bind(end)
            .fetch_all(pool)
            .await
            .map_err(storage)?;
            for (account, kind, id, count, pre_score) in rows {
                let target = match kind.as_str() {
                    "post" => StakeTarget::Post(id),
                    "comment" => StakeTarget::Comment(id),
                    _ => continue,
                };
                positions.push(DayPosition { account: AccountId::from_uuid(account), target, count_at_settlement: count, pre_score });
            }
        }
        // The same order on every run: the same shares, recomputed.
        positions.sort_by_key(|p| (p.account.as_uuid(), p.target.reference()));
        Ok(positions)
    }

    #[instrument(name = "wallet.envelope.record", skip(self, allocations))]
    async fn record_allocations(
        &self,
        day: NaiveDate,
        allocations: &[(DayPosition, Allocation)],
    ) -> Result<(), WalletError> {
        for (position, allocation) in allocations {
            sqlx::query(
                "UPDATE settlements SET outcome = $4, score = $5, provisional_gems = $6, envelope_day = $7 \
                 WHERE account_id = $1 AND target_kind = $2 AND target_id = $3",
            )
            .bind(position.account.as_uuid())
            .bind(position.target.kind())
            .bind(position.target.id())
            .bind(allocation.outcome)
            .bind(allocation.score)
            .bind(allocation.provisional_gems)
            .bind(day)
            .execute(self.pool(&position.account)?)
            .await
            .map_err(storage)?;
        }
        Ok(())
    }

    #[instrument(name = "wallet.envelope.complete", skip(self, summary))]
    async fn complete_envelope_day(&self, day: NaiveDate, summary: &EnvelopeSummary) -> Result<(), WalletError> {
        sqlx::query(
            "UPDATE envelope_days SET computed_at = $2, pool = $3, allocated = $4, positions = $5, targets = $6, \
               model = $7, claimed_until = NULL WHERE day = $1",
        )
        .bind(day)
        .bind(summary.computed_at)
        .bind(summary.pool)
        .bind(summary.allocated)
        .bind(summary.positions)
        .bind(summary.targets)
        .bind(summary.model)
        .execute(self.envelope_pool()?)
        .await
        .map_err(storage)?;
        Ok(())
    }
}

/// Midnight (UTC) at the start of `day`.
fn day_start(day: NaiveDate) -> DateTime<Utc> {
    day.and_time(chrono::NaiveTime::MIN).and_utc()
}
