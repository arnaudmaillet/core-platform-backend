//! #665 over live Redis and Scylla: likes are each account's total on a
//! target — applied idempotently and in any order (the wallet's outbox
//! delivers at least once), read per target and per reader, and kept durably.

use std::sync::Arc;

use scylla_storage::{ScyllaConfig, ScyllaSessionBuilder};
use uuid::Uuid;

use engagement::application::port::LikeLedger;
use engagement::domain::value_object::LikeTarget;
use engagement::infrastructure::persistence::ScyllaLikeLedger;

use crate::engagement_it::harness::TestHarness;

#[tokio::test]
async fn totals_apply_once_in_any_order_and_concurrently() {
    let h = TestHarness::start().await;
    let post = LikeTarget::Post(Uuid::now_v7().to_string());
    let (me, you) = (Uuid::now_v7().to_string(), Uuid::now_v7().to_string());

    assert_eq!(h.like_store.apply_total(&post, &me, 30).await.unwrap(), 30);
    assert_eq!(h.like_store.apply_total(&post, &me, 30).await.unwrap(), 0, "a redelivery adds nothing");
    assert_eq!(h.like_store.apply_total(&post, &me, 35).await.unwrap(), 5);
    assert_eq!(h.like_store.apply_total(&post, &me, 30).await.unwrap(), 0, "a late, older total adds nothing");

    // The same events, delivered many times at once.
    let deliveries = (0..20).map(|i| {
        let (store, post, you) = (Arc::clone(&h.like_store), post.clone(), you.clone());
        tokio::spawn(async move { store.apply_total(&post, &you, 10 + (i % 3)).await.unwrap() })
    });
    let mut added = 0;
    for d in deliveries {
        added += d.await.unwrap();
    }
    assert_eq!(added, 12, "you end at your highest total, counted once");

    let comment = LikeTarget::Comment(Uuid::now_v7().to_string());
    let targets = [post.clone(), comment.clone()];
    assert_eq!(h.like_store.counts(&targets).await.unwrap(), vec![47, 0]);
    assert_eq!(h.like_store.mine(&me, &targets).await.unwrap(), vec![35, 0]);
    assert_eq!(h.like_store.mine(&Uuid::now_v7().to_string(), &targets).await.unwrap(), vec![0, 0]);
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
    ledger.record(&target, &account, "liker", 40, 2_000_000).await.unwrap();
    // An older event landing late does not win.
    ledger.record(&target, &account, "liker", 10, 1_000_000).await.unwrap();

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
        ledger.record(target, &account, "liker", 10 * (i as i64 + 1), 1_000_000).await.unwrap();
    }
    ledger.record(&LikeTarget::Post(Uuid::now_v7().to_string()), &Uuid::now_v7().to_string(), "other", 5, 1_000_000).await.unwrap();

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
