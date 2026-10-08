//! Live, container-backed integration suite for the wallet ledger: the real
//! Postgres adapter, its row locks and constraints.
//!
//! ```text
//! cargo test -p wallet --features integration-wallet
//! ```
#![cfg(feature = "integration-wallet")]

use std::sync::Arc;

use chrono::{TimeZone, Utc};
use postgres_storage::TransactionManager;
use sqlx::PgPool;
use uuid::Uuid;

use wallet::application::port::{ClaimOutcome, GemSpend, PackOutcome, SpendOutcome, WalletStore};
use wallet::application::Wallets;
use wallet::config::WalletConfig;
use wallet::domain::{AccountId, ClaimPolicy, Currency, IdempotencyKey, Operation, StakePackPolicy, TransactionKind};
use wallet::infrastructure::persistence::PgWalletStore;

const MIGRATIONS_DIR: &str = concat!(env!("CARGO_MANIFEST_DIR"), "/migrations");

async fn pool() -> PgPool {
    let url = test_support::containers::postgres_ready(MIGRATIONS_DIR).await;
    PgPool::connect(&url).await.expect("connect to the test Postgres")
}

fn store(pool: &PgPool) -> Arc<PgWalletStore> {
    Arc::new(PgWalletStore::new(TransactionManager::new(pool.clone())))
}

/// Each balance equals the sum of its currency's ledger deltas.
async fn assert_reconciles(pool: &PgPool, account: &AccountId) {
    let (points, gems): (i64, i64) = sqlx::query_as("SELECT points, gems FROM wallets WHERE account_id = $1")
        .bind(account.as_uuid())
        .fetch_one(pool)
        .await
        .unwrap();
    let sums: Vec<(String, i64)> = sqlx::query_as(
        "SELECT currency, SUM(delta)::bigint FROM wallet_transactions WHERE account_id = $1 GROUP BY currency",
    )
    .bind(account.as_uuid())
    .fetch_all(pool)
    .await
    .unwrap();
    let sum = |c: &str| sums.iter().find(|(currency, _)| currency == c).map_or(0, |(_, s)| *s);
    assert_eq!((points, gems), (sum("points"), sum("gems")), "balances reconcile with the ledger");
}

#[tokio::test]
async fn concurrent_opens_give_the_starter_gems_once() {
    let pool = pool().await;
    let store = store(&pool);
    let account = AccountId::from_uuid(Uuid::now_v7());
    let opens = (0..8).map(|_| {
        let store = Arc::clone(&store);
        tokio::spawn(async move { store.open(&account, 100, Utc::now()).await.unwrap() })
    });
    for open in opens {
        assert_eq!(open.await.unwrap().gems, 100);
    }
    let history = store.history(&account, None, None, 10).await.unwrap();
    assert_eq!(history.len(), 1);
    assert_eq!(history[0].kind, TransactionKind::StarterGift);
    assert_reconciles(&pool, &account).await;
}

#[tokio::test]
async fn concurrent_claims_credit_once_per_hour_and_replays_answer_the_same() {
    let pool = pool().await;
    let store = store(&pool);
    let account = AccountId::from_uuid(Uuid::now_v7());
    let now = Utc.with_ymd_and_hms(2026, 10, 8, 12, 0, 0).unwrap();
    let claims = (0..8).map(|_| {
        let store = Arc::clone(&store);
        tokio::spawn(async move {
            let key = IdempotencyKey::for_operation(Operation::Claim, &Uuid::now_v7().to_string()).unwrap();
            store.claim(&account, &key, &ClaimPolicy::default(), 100, now).await.unwrap()
        })
    });
    let mut claimed = 0;
    for claim in claims {
        if claim.await.unwrap().outcome == ClaimOutcome::Claimed {
            claimed += 1;
        }
    }
    assert_eq!(claimed, 1, "the row lock serializes: one claim per hour");

    let key = IdempotencyKey::for_operation(Operation::Claim, "retry-key-0001").unwrap();
    let later = now + chrono::TimeDelta::hours(1);
    let first = store.claim(&account, &key, &ClaimPolicy::default(), 100, later).await.unwrap();
    let replay = store.claim(&account, &key, &ClaimPolicy::default(), 100, later).await.unwrap();
    assert_eq!((first.outcome, first.awarded), (ClaimOutcome::Claimed, 25));
    assert_eq!((replay.outcome, replay.awarded, replay.wallet.points), (ClaimOutcome::Claimed, 25, 50));
    assert_reconciles(&pool, &account).await;
}

#[tokio::test]
async fn the_history_pages_over_postgres_and_erasure_clears_it() {
    let pool = pool().await;
    let wallets = Wallets::new(store(&pool), WalletConfig::default());
    let account = Uuid::now_v7().to_string();
    let start = Utc.with_ymd_and_hms(2026, 10, 8, 0, 0, 0).unwrap();
    for hour in 0..5 {
        let now = start + chrono::TimeDelta::hours(hour);
        wallets.claim(&account, &Uuid::now_v7().to_string(), now).await.unwrap();
    }
    let first = wallets.history(&account, None, 3, "").await.unwrap();
    let rest = wallets.history(&account, None, 3, first.next_page_token.as_deref().unwrap()).await.unwrap();
    assert_eq!((first.transactions.len(), rest.transactions.len()), (3, 3), "5 claims + the starter gift");
    assert!(rest.next_page_token.is_none());
    let mut ids: Vec<_> = first.transactions.iter().chain(&rest.transactions).map(|t| t.id).collect();
    ids.dedup();
    assert_eq!(ids.len(), 6, "no row twice");
    let gems = wallets.history(&account, Some(Currency::Gems), 0, "").await.unwrap();
    assert_eq!(gems.transactions.len(), 1);
    assert_reconciles(&pool, &AccountId::parse(&account).unwrap()).await;

    let id = AccountId::parse(&account).unwrap();
    assert!(wallets.erase(&id).await.unwrap());
    let (left,): (i64,) = sqlx::query_as("SELECT COUNT(*) FROM wallet_transactions WHERE account_id = $1")
        .bind(id.as_uuid())
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(left, 0);
}

#[tokio::test]
async fn concurrent_pack_buys_charge_once_and_gem_spends_are_recorded() {
    let pool = pool().await;
    let store = store(&pool);
    let account = AccountId::from_uuid(Uuid::now_v7());
    let buys = (0..6).map(|_| {
        let store = Arc::clone(&store);
        tokio::spawn(async move {
            let key = IdempotencyKey::for_operation(Operation::StakePack, &Uuid::now_v7().to_string()).unwrap();
            store.buy_stake_pack(&account, &key, &StakePackPolicy::default(), 100, Utc::now()).await.unwrap().0
        })
    });
    let mut bought = 0;
    for buy in buys {
        if buy.await.unwrap() == PackOutcome::Bought {
            bought += 1;
        }
    }
    assert_eq!(bought, 1, "packs do not stack, even raced");

    let spend = GemSpend { amount: 30, kind: TransactionKind::CountryUnlock, ref_id: Some("FR".into()) };
    let key = IdempotencyKey::for_operation(Operation::SpendGems, "country-FR").unwrap();
    let (first, wallet) = store.spend_gems(&account, &key, &spend, 100, Utc::now()).await.unwrap();
    let (replay, again) = store.spend_gems(&account, &key, &spend, 100, Utc::now()).await.unwrap();
    assert_eq!((first, wallet.gems), (SpendOutcome::Spent, 20));
    assert_eq!((replay, again.gems), (SpendOutcome::Spent, 20), "charged once");
    let more = GemSpend { amount: 30, kind: TransactionKind::CountryUnlock, ref_id: Some("US".into()) };
    let key = IdempotencyKey::for_operation(Operation::SpendGems, "country-US").unwrap();
    assert_eq!(store.spend_gems(&account, &key, &more, 100, Utc::now()).await.unwrap().0, SpendOutcome::InsufficientGems);

    let history = store.history(&account, Some(Currency::Gems), None, 10).await.unwrap();
    let kinds: Vec<_> = history.iter().map(|t| (t.kind, t.delta, t.ref_id.as_deref())).collect();
    assert_eq!(kinds, vec![
        (TransactionKind::CountryUnlock, -30, Some("FR")),
        (TransactionKind::StakePack, -50, None),
        (TransactionKind::StarterGift, 100, None),
    ]);
    assert_reconciles(&pool, &account).await;
}
