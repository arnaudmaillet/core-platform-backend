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

use wallet::application::port::{
    ClaimOutcome, GemSpend, PackOutcome, SpendOutcome, StakeAnnouncement, StakeOutcome, WalletStore,
};
use wallet::application::Wallets;
use wallet::config::WalletConfig;
use wallet::domain::{
    AccountId, ClaimPolicy, Currency, IdempotencyKey, Operation, StakeAsk, StakePackPolicy, StakePolicy, StakeTarget,
    TransactionKind,
};
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

fn announcement() -> StakeAnnouncement {
    StakeAnnouncement { profile_id: "liker".into(), author_profile_id: "author".into() }
}

/// Funds `account` with `points` (direct, for the test: claims are hourly).
async fn fund(pool: &PgPool, store: &PgWalletStore, account: &AccountId, points: i64) {
    store.open(account, 0, Utc::now()).await.unwrap();
    sqlx::query("UPDATE wallets SET points = $2, points_earned = $2 WHERE account_id = $1")
        .bind(account.as_uuid())
        .bind(points)
        .execute(pool)
        .await
        .unwrap();
    sqlx::query(
        "INSERT INTO wallet_transactions (id, account_id, currency, delta, balance_after, kind, idempotency_key, created_at) \
         VALUES ($1, $2, 'points', $3, $3, 'claim', 'sys:test-funds', now())",
    )
    .bind(Uuid::now_v7())
    .bind(account.as_uuid())
    .bind(points)
    .execute(pool)
    .await
    .unwrap();
}

#[tokio::test]
async fn concurrent_batches_never_pass_the_targets_room_and_replays_spend_nothing() {
    let pool = pool().await;
    let store = store(&pool);
    let account = AccountId::from_uuid(Uuid::now_v7());
    fund(&pool, &store, &account, 2_000).await;
    let target = StakeTarget::Post(format!("post-{}", Uuid::now_v7()));
    let batches = (0..8).map(|_| {
        let (store, target) = (Arc::clone(&store), target.clone());
        tokio::spawn(async move {
            let key = IdempotencyKey::for_operation(Operation::Stake, &Uuid::now_v7().to_string()).unwrap();
            store
                .stake(&account, &key, &target, StakeAsk::Points(40), &announcement(), &StakePolicy::default(), &StakePackPolicy::default(), 0, Utc::now())
                .await
                .unwrap()
        })
    });
    let mut spent = 0;
    let mut firsts = 0;
    for batch in batches {
        let result = batch.await.unwrap();
        spent += result.spent;
        firsts += i32::from(result.first);
    }
    assert_eq!(spent, 250, "8 × 40 asked, the room is 250");
    assert_eq!(firsts, 1, "one first batch");
    assert_eq!(store.staked_on(&account, &target).await.unwrap(), 250);

    let key = IdempotencyKey::for_operation(Operation::Stake, "replayed-batch").unwrap();
    let other = StakeTarget::Comment(format!("comment-{}", Uuid::now_v7()));
    let first = store
        .stake(&account, &key, &other, StakeAsk::Points(12), &announcement(), &StakePolicy::default(), &StakePackPolicy::default(), 0, Utc::now())
        .await
        .unwrap();
    let replay = store
        .stake(&account, &key, &other, StakeAsk::Points(12), &announcement(), &StakePolicy::default(), &StakePackPolicy::default(), 0, Utc::now())
        .await
        .unwrap();
    assert_eq!((first.spent, first.first), (12, true));
    assert_eq!((replay.outcome, replay.spent, replay.first, replay.my_total), (StakeOutcome::Staked, 12, true, 12));
    assert_eq!(replay.wallet.points, 2_000 - 250 - 12, "spent once");
    assert_reconciles(&pool, &account).await;

    let history = store.history(&account, Some(Currency::Points), None, 1).await.unwrap();
    assert_eq!((history[0].kind, history[0].ref_id.clone()), (TransactionKind::Stake, Some(other.reference())));
}

#[tokio::test]
async fn the_hour_caps_stakes_and_erasure_clears_them() {
    let pool = pool().await;
    let store = store(&pool);
    let account = AccountId::from_uuid(Uuid::now_v7());
    fund(&pool, &store, &account, 5_000).await;
    let mut total = 0;
    for i in 0..5 {
        let key = IdempotencyKey::for_operation(Operation::Stake, &format!("hour-batch-{i}")).unwrap();
        let target = StakeTarget::Post(format!("p{i}-{}", Uuid::now_v7()));
        let result = store
            .stake(&account, &key, &target, StakeAsk::Points(250), &announcement(), &StakePolicy::default(), &StakePackPolicy::default(), 0, Utc::now())
            .await
            .unwrap();
        total += result.spent;
        if i == 4 {
            assert_eq!(result.outcome, StakeOutcome::RateLimited, "1,000 points an hour");
        }
    }
    assert_eq!(total, 1_000);
    assert_reconciles(&pool, &account).await;

    assert!(store.erase(&account).await.unwrap());
    let (left,): (i64,) = sqlx::query_as("SELECT COUNT(*) FROM stakes WHERE account_id = $1")
        .bind(account.as_uuid())
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(left, 0);
}

/// Every fresh stake leaves its announcement in the outbox, in the same
/// transaction; a replay leaves none; published events are marked, then
/// pruned.
#[tokio::test]
async fn a_stake_and_its_announcement_are_written_together() {
    let pool = pool().await;
    let store = store(&pool);
    let account = AccountId::from_uuid(Uuid::now_v7());
    fund(&pool, &store, &account, 100).await;
    let target = StakeTarget::Post(format!("post-{}", Uuid::now_v7()));
    let key = IdempotencyKey::for_operation(Operation::Stake, "outbox-batch-1").unwrap();
    let (who, policy, pack) = (announcement(), StakePolicy::default(), StakePackPolicy::default());
    let stake = || store.stake(&account, &key, &target, StakeAsk::Points(3), &who, &policy, &pack, 0, Utc::now());
    let fresh = stake().await.unwrap();
    let replay = stake().await.unwrap();
    assert!(fresh.outbox.is_some() && replay.outbox.is_none());

    let mine = |events: Vec<wallet::application::port::OutboxEvent>| events.into_iter().filter(|e| e.account == account).collect::<Vec<_>>();
    let now = Utc::now();
    // The writer holds its row while it publishes: no drainer takes it yet.
    assert!(mine(store.claim_unpublished(10_000, now, now + chrono::TimeDelta::seconds(60)).await.unwrap()).is_empty());
    // Past the writer's lease (it failed to publish): one drainer claims it,
    // a concurrent one does not.
    let later = now + chrono::TimeDelta::seconds(31);
    let lease = later + chrono::TimeDelta::seconds(60);
    let (a, b) = tokio::join!(store.claim_unpublished(10_000, later, lease), store.claim_unpublished(10_000, later, lease));
    let (a, b) = (mine(a.unwrap()), mine(b.unwrap()));
    assert_eq!(a.len() + b.len(), 1, "one replica claims it");
    let claimed = a.into_iter().chain(b).next().unwrap();
    assert_eq!(claimed, fresh.outbox.clone().unwrap(), "read back as written");
    store.mark_published(&claimed, later).await.unwrap();
    let much_later = later + chrono::TimeDelta::seconds(120);
    assert!(mine(store.claim_unpublished(10_000, much_later, much_later).await.unwrap()).is_empty(), "published");
    assert!(store.prune_outbox(Utc::now() + chrono::TimeDelta::days(1)).await.unwrap() >= 1);
}

/// Stake settlement (#665, shadow mode): a due position is leased to one
/// settler at a time, settles once, and goes with the account.
#[tokio::test]
async fn due_positions_are_leased_to_one_settler_and_settle_once() {
    use chrono::TimeDelta;
    use wallet::domain::{Observed, SettlementPolicy};

    let pool = pool().await;
    let store = store(&pool);
    let account = AccountId::from_uuid(Uuid::now_v7());
    fund(&pool, &store, &account, 500).await;
    let targets = [StakeTarget::Post(format!("post-{}", Uuid::now_v7())), StakeTarget::Comment(format!("c-{}", Uuid::now_v7()))];
    for target in &targets {
        let key = IdempotencyKey::for_operation(Operation::Stake, &Uuid::now_v7().to_string()).unwrap();
        store
            .stake(&account, &key, target, StakeAsk::Points(20), &announcement(), &StakePolicy::default(), &StakePackPolicy::default(), 0, Utc::now())
            .await
            .unwrap();
    }
    let now = Utc::now() + TimeDelta::hours(25);
    let mine = |due: Vec<wallet::domain::DuePosition>| due.into_iter().filter(|p| p.account == account).collect::<Vec<_>>();

    // Two settlers at once: each position goes to exactly one of them.
    let (a, b) = tokio::join!(
        store.claim_due_positions(10_000, now - TimeDelta::hours(24), now, now + TimeDelta::minutes(5)),
        store.claim_due_positions(10_000, now - TimeDelta::hours(24), now, now + TimeDelta::minutes(5)),
    );
    let (a, b) = (mine(a.unwrap()), mine(b.unwrap()));
    assert_eq!(a.len() + b.len(), 2, "both positions claimed, each once");
    let first = a.into_iter().chain(b).find(|p| p.target == targets[0]).unwrap();
    assert_eq!(first.points, 20);

    let policy = SettlementPolicy::default();
    let observed = Observed { total: 20, count_on_arrival: Some(5), count_now: 45 };
    store.record_settlement(&policy.settle(first.clone(), observed, now)).await.unwrap();
    // A second settlement of the same position keeps the first.
    let again = Observed { total: 20, count_on_arrival: Some(5), count_now: 999 };
    store.record_settlement(&policy.settle(first, again, now)).await.unwrap();
    let (count, earliness): (i64, Option<f64>) = sqlx::query_as(
        "SELECT count_at_settlement, earliness FROM settlements WHERE account_id = $1 AND target_id = $2",
    )
    .bind(account.as_uuid())
    .bind(targets[0].id())
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!((count, earliness), (45, Some(20.0 / 25.0)));

    // Once the lease ends, only the unsettled position comes back.
    let later = now + TimeDelta::minutes(10);
    let back = mine(store.claim_due_positions(10_000, later - TimeDelta::hours(24), later, later + TimeDelta::minutes(5)).await.unwrap());
    assert_eq!(back.iter().map(|p| p.target.clone()).collect::<Vec<_>>(), vec![targets[1].clone()]);
    // A position whose first stake is not a day old is not due.
    assert!(mine(store.claim_due_positions(10_000, Utc::now() - TimeDelta::hours(24), later, later).await.unwrap()).is_empty());

    store.erase(&account).await.unwrap();
    let (left,): (i64,) = sqlx::query_as("SELECT COUNT(*) FROM settlements WHERE account_id = $1")
        .bind(account.as_uuid())
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(left, 0, "settlements go with the account");
}

/// The daily curator envelope (#665, shadow mode): once a day is over, its
/// settlements share the pool — one replica computes it, once — and the
/// shares land on each settlement; nothing is minted.
#[tokio::test]
async fn a_finished_days_envelope_is_computed_once_over_postgres() {
    use chrono::{NaiveDate, TimeDelta};
    use wallet::domain::{DuePosition, Observed, SettlementPolicy};

    let pool = pool().await;
    let store = store(&pool);
    let day = NaiveDate::from_ymd_opt(2020, 2, 2).unwrap();
    let noon = day.and_hms_opt(12, 0, 0).unwrap().and_utc();
    let policy = SettlementPolicy::default();
    let (a, b) = (AccountId::from_uuid(Uuid::now_v7()), AccountId::from_uuid(Uuid::now_v7()));
    let hot = StakeTarget::Post(format!("hot-{}", Uuid::now_v7()));
    let settle = |account: AccountId, target: &StakeTarget, at, observed| {
        let position = DuePosition { account, target: target.clone(), points: 250, first_at: noon - TimeDelta::days(1) };
        policy.settle(position, observed, at)
    };
    let early = Observed { total: 250, count_on_arrival: Some(0), count_now: 1_250 };
    let late = Observed { total: 250, count_on_arrival: Some(1_000), count_now: 1_250 };
    store.record_settlement(&settle(a, &hot, noon, early)).await.unwrap();
    store.record_settlement(&settle(b, &hot, noon, late)).await.unwrap();
    // The next day's settlement waits for its own day to end.
    let tomorrow = settle(a, &StakeTarget::Comment(format!("c-{}", Uuid::now_v7())), noon + TimeDelta::days(1), early);
    store.record_settlement(&tomorrow).await.unwrap();

    let wallets = |store: &Arc<PgWalletStore>| Wallets::new(Arc::clone(store) as _, WalletConfig::default());
    let (w1, w2) = (wallets(&store), wallets(&store));
    let after_midnight = (day + TimeDelta::days(1)).and_hms_opt(0, 11, 0).unwrap().and_utc();
    let (x, y) = tokio::join!(w1.run_envelopes(after_midnight), w2.run_envelopes(after_midnight));
    assert_eq!(x.unwrap() + y.unwrap(), 1, "one replica computes the day");
    assert_eq!(w1.run_envelopes(after_midnight).await.unwrap(), 0, "once");

    let shares: Vec<(Uuid, i16, i64, Option<NaiveDate>)> = sqlx::query_as(
        "SELECT account_id, outcome, provisional_gems, envelope_day FROM settlements WHERE target_id = $1 ORDER BY provisional_gems DESC",
    )
    .bind(hot.id())
    .fetch_all(&pool)
    .await
    .unwrap();
    // The day's only target: its top. a came first (earliness 1), b last
    // (earliness 0): the target's 20 gems (2 % of 1000) go to a.
    assert_eq!(shares, vec![(a.as_uuid(), 1, 20, Some(day)), (b.as_uuid(), 1, 0, Some(day))]);
    let (allocated, positions): (i64, i64) =
        sqlx::query_as("SELECT allocated, positions FROM envelope_days WHERE day = $1 AND computed_at IS NOT NULL")
            .bind(day)
            .fetch_one(&pool)
            .await
            .unwrap();
    assert_eq!((allocated, positions), (20, 2));
    let (pending,): (i64,) = sqlx::query_as("SELECT COUNT(*) FROM settlements WHERE target_id = $1 AND envelope_day IS NULL")
        .bind(tomorrow.target.id())
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(pending, 1, "the next day is not over");
}

/// The GDPR export (#653, #665): a wallet is read without being opened, and
/// the stake positions come with their settlements, page by page.
#[tokio::test]
async fn the_export_reads_without_opening_and_joins_the_settlements() {
    use chrono::TimeDelta;
    use wallet::domain::{DuePosition, Observed, SettlementPolicy};

    let pool = pool().await;
    let store = store(&pool);
    let never = AccountId::from_uuid(Uuid::now_v7());
    assert!(store.peek(&never).await.unwrap().is_none());
    let (opened,): (i64,) = sqlx::query_as("SELECT COUNT(*) FROM wallets WHERE account_id = $1")
        .bind(never.as_uuid())
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(opened, 0, "peeking opens nothing");

    let account = AccountId::from_uuid(Uuid::now_v7());
    fund(&pool, &store, &account, 500).await;
    let targets = [
        StakeTarget::Comment(format!("c-{}", Uuid::now_v7())),
        StakeTarget::Post(format!("p1-{}", Uuid::now_v7())),
        StakeTarget::Post(format!("p2-{}", Uuid::now_v7())),
    ];
    for target in &targets {
        let key = IdempotencyKey::for_operation(Operation::Stake, &Uuid::now_v7().to_string()).unwrap();
        store
            .stake(&account, &key, target, StakeAsk::Points(7), &announcement(), &StakePolicy::default(), &StakePackPolicy::default(), 0, Utc::now())
            .await
            .unwrap();
    }
    assert_eq!(store.peek(&account).await.unwrap().unwrap().points, 500 - 21);

    let settled = DuePosition { account, target: targets[1].clone(), points: 7, first_at: Utc::now() };
    let observed = Observed { total: 7, count_on_arrival: Some(3), count_now: 30 };
    store.record_settlement(&SettlementPolicy::default().settle(settled, observed, Utc::now() + TimeDelta::hours(25))).await.unwrap();

    let first = store.stake_positions(&account, None, 2).await.unwrap();
    assert_eq!(first.len(), 2);
    assert_eq!(first[0].target, targets[0], "comments sort before posts");
    let rest = store.stake_positions(&account, Some(&first[1].target), 2).await.unwrap();
    assert_eq!(rest.len(), 1);
    let all: Vec<_> = first.into_iter().chain(rest).collect();
    let with = all.iter().find(|p| p.target == targets[1]).unwrap();
    let settlement = with.settlement.as_ref().unwrap();
    assert_eq!((settlement.points, settlement.count_on_arrival, settlement.count_at_settlement), (7, Some(3), 30));
    assert_eq!((settlement.outcome, settlement.provisional_gems, settlement.envelope_day), (None, None, None), "no envelope yet");
    assert!(all.iter().filter(|p| p.target != targets[1]).all(|p| p.settlement.is_none() && p.points == 7));
}
