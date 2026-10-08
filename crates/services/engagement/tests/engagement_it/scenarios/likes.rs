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
