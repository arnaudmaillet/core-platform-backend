//! #665 over live Redis and Scylla: likes are each account's total on a
//! target — applied idempotently and in any order (the wallet's outbox
//! delivers at least once), read per target and per reader, and kept durably.

use std::sync::Arc;

use scylla_storage::{ScyllaConfig, ScyllaSessionBuilder};
use uuid::Uuid;

use engagement::application::erasure::{anonymous_liker, LikeEraser};
use engagement::application::likes;
use engagement::application::port::{ForgottenLike, LikeLedger, Position};
use engagement::domain::value_object::LikeTarget;
use engagement::infrastructure::persistence::ScyllaLikeLedger;

use crate::engagement_it::harness::TestHarness;

/// A total recorded without an arrival (the scenarios that predate it).
fn pos(total: i64) -> Position {
    Position { total, arrival: None }
}

#[tokio::test]
async fn totals_apply_once_in_any_order_and_concurrently() {
    let h = TestHarness::start().await;
    let post = LikeTarget::Post(Uuid::now_v7().to_string());
    let (me, you) = (Uuid::now_v7().to_string(), Uuid::now_v7().to_string());

    assert_eq!(h.like_store.apply_total(&post, &me, 30).await.unwrap().map(|a| a.added), Some(30));
    assert_eq!(h.like_store.apply_total(&post, &me, 30).await.unwrap().map(|a| a.added), Some(0), "a redelivery adds nothing");
    assert_eq!(h.like_store.apply_total(&post, &me, 35).await.unwrap().map(|a| a.added), Some(5));
    assert_eq!(h.like_store.apply_total(&post, &me, 30).await.unwrap().map(|a| a.added), Some(0), "a late, older total adds nothing");

    // The same events, delivered many times at once.
    let deliveries = (0..20).map(|i| {
        let (store, post, you) = (Arc::clone(&h.like_store), post.clone(), you.clone());
        tokio::spawn(async move { store.apply_total(&post, &you, 10 + (i % 3)).await.unwrap().unwrap().added })
    });
    let mut added = 0;
    for d in deliveries {
        added += d.await.unwrap();
    }
    assert_eq!(added, 12, "you end at your highest total, counted once");

    let comment = LikeTarget::Comment(Uuid::now_v7().to_string());
    let targets = [post.clone(), comment.clone()];
    assert_eq!(h.like_store.counts(&targets).await.unwrap(), vec![47, 0]);
    assert_eq!(h.like_store.mine(&me, &targets).await.unwrap(), vec![Some(35), Some(0)]);
    assert_eq!(h.like_store.mine(&Uuid::now_v7().to_string(), &targets).await.unwrap(), vec![Some(0), Some(0)]);
}

#[tokio::test]
async fn the_durable_copy_keeps_the_newest_total() {
    let contact = test_support::containers::scylla_ready("engagement", concat!(env!("CARGO_MANIFEST_DIR"), "/migrations")).await;
    let client = Arc::new(
        ScyllaSessionBuilder::new(ScyllaConfig { contact_points: vec![contact], keyspace: None, ..ScyllaConfig::default() })
            .build()
            .await
            .expect("scylla"),
    );
    let ledger = ScyllaLikeLedger::new(Arc::clone(&client));
    let target = LikeTarget::Comment(Uuid::now_v7().to_string());
    let account = Uuid::now_v7().to_string();
    ledger.record(&target, &account, "liker", pos(40), 2_000_000).await.unwrap();
    // An older event landing late does not win.
    ledger.record(&target, &account, "liker", pos(10), 1_000_000).await.unwrap();

    let rows = client
        .session
        .execute_unpaged(
            "SELECT total FROM engagement.likes_by_target WHERE target_kind = ? AND target_id = ? AND account_id = ?",
            (target.kind(), target.id(), Uuid::parse_str(&account).unwrap()),
        )
        .await
        .unwrap()
        .into_rows_result()
        .unwrap();
    let (total,): (i64,) = rows.single_row().unwrap();
    assert_eq!(total, 40);
    let rows = client
        .session
        .execute_unpaged(
            "SELECT total FROM engagement.likes_by_account WHERE account_id = ?",
            (Uuid::parse_str(&account).unwrap(),),
        )
        .await
        .unwrap()
        .into_rows_result()
        .unwrap();
    let (total,): (i64,) = rows.single_row().unwrap();
    assert_eq!(total, 40);
}

/// The GDPR export's read (#653, #665): what an account liked, paged by target.
#[tokio::test]
async fn an_account_lists_what_it_liked_page_by_page() {
    let contact = test_support::containers::scylla_ready("engagement", concat!(env!("CARGO_MANIFEST_DIR"), "/migrations")).await;
    let client = Arc::new(
        ScyllaSessionBuilder::new(ScyllaConfig { contact_points: vec![contact], keyspace: None, ..ScyllaConfig::default() })
            .build()
            .await
            .expect("scylla"),
    );
    let ledger = ScyllaLikeLedger::new(client);
    let account = Uuid::now_v7().to_string();
    let mut targets: Vec<LikeTarget> = (0..3)
        .map(|i| if i % 2 == 0 { LikeTarget::Post(Uuid::now_v7().to_string()) } else { LikeTarget::Comment(Uuid::now_v7().to_string()) })
        .collect();
    for (i, target) in targets.iter().enumerate() {
        ledger.record(target, &account, "liker", pos(10 * (i as i64 + 1)), 1_000_000).await.unwrap();
    }
    ledger.record(&LikeTarget::Post(Uuid::now_v7().to_string()), &Uuid::now_v7().to_string(), "other", pos(5), 1_000_000).await.unwrap();

    let first = ledger.list_by_account(&account, 2, None).await.unwrap();
    let rest = ledger.list_by_account(&account, 2, Some(&first[1].target)).await.unwrap();
    assert_eq!((first.len(), rest.len()), (2, 1));
    let mut listed: Vec<_> = first.iter().chain(&rest).map(|l| (l.target.clone(), l.total, l.profile_id.clone())).collect();
    let key = |t: &LikeTarget| (t.kind().to_owned(), t.id().to_owned());
    listed.sort_by_key(|(t, _, _)| key(t));
    let mut expected: Vec<_> = targets.drain(..).enumerate().map(|(i, t)| (t, 10 * (i as i64 + 1), "liker".to_owned())).collect();
    expected.sort_by_key(|(t, _, _)| key(t));
    assert_eq!(listed, expected, "every like once, none of another account's");
    assert_eq!(first[0].liked_at.timestamp_micros(), 1_000_000);
}

/// A deleted account's likes (#665): who liked goes — in Redis and Scylla —
/// the counts stay, and a stake made before the deletion, landing late, does
/// not write the account back.
#[tokio::test]
async fn a_deleted_accounts_likes_are_forgotten_the_counts_kept() {
    let h = TestHarness::start().await;
    let contact = test_support::containers::scylla_ready("engagement", concat!(env!("CARGO_MANIFEST_DIR"), "/migrations")).await;
    let client = Arc::new(
        ScyllaSessionBuilder::new(ScyllaConfig { contact_points: vec![contact], keyspace: None, ..ScyllaConfig::default() })
            .build()
            .await
            .expect("scylla"),
    );
    let ledger = Arc::new(ScyllaLikeLedger::new(Arc::clone(&client)));
    let (gone, stays) = (Uuid::now_v7().to_string(), Uuid::now_v7().to_string());
    let targets = [LikeTarget::Post(Uuid::now_v7().to_string()), LikeTarget::Comment(Uuid::now_v7().to_string())];
    for t in &targets {
        for (account, total) in [(&gone, 7), (&stays, 3)] {
            h.like_store.apply_total(t, account, total).await.unwrap().unwrap();
            ledger.record(t, account, "liker", pos(total), 1_000_000).await.unwrap();
        }
    }

    let eraser = LikeEraser { store: Arc::clone(&h.like_store), ledger: ledger.clone() };
    assert_eq!(eraser.erase(&gone, 1_800_000, 2_000_000).await.unwrap(), 2);

    assert_eq!(ledger.erased_at(&gone).await.unwrap(), Some(1_800_000));
    assert_eq!(ledger.erased_at(&stays).await.unwrap(), None);
    assert_eq!(h.like_store.mine(&gone, &targets).await.unwrap(), vec![Some(0), Some(0)]);
    assert_eq!(h.like_store.counts(&targets).await.unwrap(), vec![10, 10], "the points are kept");
    assert!(ledger.list_by_account(&gone, 10, None).await.unwrap().is_empty());
    assert_eq!(ledger.list_by_account(&stays, 10, None).await.unwrap().len(), 2);

    // A stake made before the deletion, written after it: still deleted.
    ledger.record(&targets[0], &gone, "liker", pos(9), 1_500_000).await.unwrap();
    assert!(ledger.list_by_account(&gone, 10, None).await.unwrap().is_empty());
    let rows = client
        .session
        .execute_unpaged(
            "SELECT account_id FROM engagement.likes_by_target WHERE target_kind = ? AND target_id = ?",
            (targets[0].kind(), targets[0].id()),
        )
        .await
        .unwrap()
        .into_rows_result()
        .unwrap();
    let mut likers: Vec<Uuid> = rows.rows::<(Uuid,)>().unwrap().map(|r| r.unwrap().0).collect();
    likers.sort();
    let mut expected = vec![Uuid::parse_str(&stays).unwrap(), anonymous_liker(&gone, &targets[0], 1_800_000)];
    expected.sort();
    assert_eq!(likers, expected, "who stays, and the deleted account's points under an anonymous id");

    // Forgetting again (a replayed batch) rewrites the same anonymous row.
    let again = ForgottenLike { target: targets[0].clone(), total: 7, anonymous_id: anonymous_liker(&gone, &targets[0], 1_800_000) };
    ledger.forget(&gone, &[again], 2_100_000).await.unwrap();
    let rows = client
        .session
        .execute_unpaged(
            "SELECT total FROM engagement.likes_by_target WHERE target_kind = ? AND target_id = ?",
            (targets[0].kind(), targets[0].id()),
        )
        .await
        .unwrap()
        .into_rows_result()
        .unwrap();
    let sum: i64 = rows.rows::<(i64,)>().unwrap().map(|r| r.unwrap().0).sum();
    assert_eq!(sum, 10, "the durable copy still sums to the count");
}

/// A target's likers expire from Redis 30 days after its last like (its count
/// never does): an account is then unknown, not zero, until the stake path
/// rehydrates them from Scylla — the next stake adds only the difference.
#[tokio::test]
async fn expired_likers_come_back_from_the_durable_copy() {
    use fred::interfaces::KeysInterface;

    let h = TestHarness::start().await;
    let contact = test_support::containers::scylla_ready("engagement", concat!(env!("CARGO_MANIFEST_DIR"), "/migrations")).await;
    let client = Arc::new(
        ScyllaSessionBuilder::new(ScyllaConfig { contact_points: vec![contact], keyspace: None, ..ScyllaConfig::default() })
            .build()
            .await
            .expect("scylla"),
    );
    let ledger = ScyllaLikeLedger::new(client);
    let post = LikeTarget::Post(Uuid::now_v7().to_string());
    let (a, b) = (Uuid::now_v7().to_string(), Uuid::now_v7().to_string());
    for (account, total) in [(&a, 10), (&b, 4)] {
        assert_eq!(likes::apply_total(h.like_store.as_ref(), &ledger, &post, account, total).await.unwrap().added, total);
        ledger.record(&post, account, "liker", pos(total), 1_000_000).await.unwrap();
    }
    let likers = format!("engagement:{{{post}}}:likers");
    let ttl: i64 = h.redis.inner.ttl(&likers).await.unwrap();
    assert!(ttl > 29 * 24 * 3600, "the likers expire 30 days after the last like ({ttl} s)");

    // Time passes: the likers expire, the count stays.
    let _: i64 = h.redis.inner.del(&likers).await.unwrap();
    let one = std::slice::from_ref(&post);
    assert_eq!(h.like_store.mine(&a, one).await.unwrap(), vec![None], "unknown, not 0");
    assert_eq!(h.like_store.apply_total(&post, &a, 15).await.unwrap(), None);

    assert_eq!(likes::apply_total(h.like_store.as_ref(), &ledger, &post, &a, 15).await.unwrap().added, 5, "only the difference");
    ledger.record(&post, &a, "liker", pos(15), 2_000_000).await.unwrap();
    assert_eq!(h.like_store.counts(one).await.unwrap(), vec![19]);
    assert_eq!(h.like_store.mine(&b, one).await.unwrap(), vec![Some(4)], "every liker is back");
    assert_eq!(h.like_store.mine(&Uuid::now_v7().to_string(), one).await.unwrap(), vec![Some(0)], "whole again");

    // A deleted account whose row is still in the durable copy (an erasure
    // racing the rehydration) is not loaded back.
    let gone = Uuid::now_v7().to_string();
    ledger.record(&post, &gone, "liker", pos(3), 1_000_000).await.unwrap();
    ledger.mark_erased(&gone, 1_900_000).await.unwrap();
    let _: i64 = h.redis.inner.del(&likers).await.unwrap();
    likes::rehydrate(h.like_store.as_ref(), &ledger, &post).await.unwrap();
    assert_eq!(h.like_store.mine(&gone, one).await.unwrap(), vec![Some(0)]);
    assert_eq!(h.like_store.mine(&a, one).await.unwrap(), vec![Some(15)]);

    // One rehydration at a time from the read path.
    assert!(h.like_store.claim_rehydration(&post).await.unwrap());
    assert!(!h.like_store.claim_rehydration(&post).await.unwrap());
}

/// Stake settlement (#665): each account's arrival on a target — the count
/// just before its first like — is kept with its total in Redis and in
/// Scylla, survives the likers' expiry, and a later stake does not move it.
#[tokio::test]
async fn an_accounts_arrival_is_kept_with_its_total() {
    use fred::interfaces::{HashesInterface, KeysInterface};

    let h = TestHarness::start().await;
    let contact = test_support::containers::scylla_ready("engagement", concat!(env!("CARGO_MANIFEST_DIR"), "/migrations")).await;
    let client = Arc::new(
        ScyllaSessionBuilder::new(ScyllaConfig { contact_points: vec![contact], keyspace: None, ..ScyllaConfig::default() })
            .build()
            .await
            .expect("scylla"),
    );
    let ledger = ScyllaLikeLedger::new(client);
    let post = LikeTarget::Post(Uuid::now_v7().to_string());
    let (a, b) = (Uuid::now_v7().to_string(), Uuid::now_v7().to_string());
    for (account, total, arrival) in [(&a, 10, 0), (&b, 4, 10), (&a, 15, 0)] {
        let applied = likes::apply_total(h.like_store.as_ref(), &ledger, &post, account, total).await.unwrap();
        assert_eq!(applied.arrival, Some(arrival));
        ledger.record(&post, account, "liker", Position { total, arrival: applied.arrival }, 1_000_000).await.unwrap();
    }
    // A redelivery returns the arrival again (so it can be recorded again).
    let again = h.like_store.apply_total(&post, &b, 4).await.unwrap().unwrap();
    assert_eq!((again.added, again.arrival), (0, Some(10)));

    let one = std::slice::from_ref(&post);
    let held = h.like_store.positions(&b, one).await.unwrap();
    assert_eq!(held, vec![Some(Position { total: 4, arrival: Some(10) })]);
    assert_eq!(ledger.position_of(&post, &b).await.unwrap(), Some(Position { total: 4, arrival: Some(10) }));
    // A null is never written over a known arrival.
    ledger.record(&post, &b, "liker", pos(4), 2_000_000).await.unwrap();
    assert_eq!(ledger.position_of(&post, &b).await.unwrap(), Some(Position { total: 4, arrival: Some(10) }));

    // The likers expire; rehydrated, the arrivals come back with them.
    let likers = format!("engagement:{{{post}}}:likers");
    let _: i64 = h.redis.inner.del(&likers).await.unwrap();
    likes::rehydrate(h.like_store.as_ref(), &ledger, &post).await.unwrap();
    assert_eq!(h.like_store.positions(&a, one).await.unwrap(), vec![Some(Position { total: 15, arrival: Some(0) })]);
    assert_eq!(h.like_store.mine(&b, one).await.unwrap(), vec![Some(4)]);

    // A value written before arrivals were kept reads as a total alone.
    let legacy = Uuid::now_v7().to_string();
    let _: i64 = h.redis.inner.hset(&likers, (legacy.as_str(), "7")).await.unwrap();
    assert_eq!(h.like_store.positions(&legacy, one).await.unwrap(), vec![Some(Position { total: 7, arrival: None })]);
}
