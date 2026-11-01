//! Scenario — unread counter under concurrency + claim-gated idempotency.
//!
//! The unread badge must be exact under a burst of concurrent notification
//! creates (no lost increments), and the claim-gated `increment_once` must admit
//! exactly one increment per dedupe key even when a stampede of identical events
//! races — the Redis `SET NX` claim that protects the at-least-once Kafka workers
//! from double-counting a redelivery. This is the concurrency axis.

use std::sync::Arc;

use crate::notification_it::harness::{self, TestHarness, DEADLINE};

const FANOUT: usize = 10;

/// Concurrent creates for one target produce an exact unread count.
#[tokio::test]
async fn concurrent_creates_produce_exact_unread_count() {
    let h = TestHarness::start().await;
    let target = harness::random_profile();

    // Fire FANOUT concurrent creates, each from a distinct sender.
    let mut handles = Vec::new();
    for _ in 0..FANOUT {
        let bus = Arc::clone(&h.command_bus);
        let target_str = target.as_str();
        let sender_str = harness::random_profile().as_str();
        handles.push(tokio::spawn(harness::dispatch_create(bus, target_str, sender_str)));
    }
    for handle in handles {
        handle.await.expect("join").expect("create_notification");
    }

    let counter = h.counter.clone();
    harness::await_until("unread count reaches the exact fan-out total", DEADLINE, || {
        let counter = counter.clone();
        async move { counter.get(&target).await.map(|c| c == FANOUT as i64).unwrap_or(false) }
    })
    .await;

    // Mark-all-read resets the badge.
    h.handler
        .mark_all_read(tonic::Request::new(harness::proto::MarkAllReadRequest {
            profile_id: target.as_str(),
        }))
        .await
        .expect("mark_all_read");

    harness::await_until("unread count cleared by mark-all-read", DEADLINE, || {
        let counter = counter.clone();
        async move { counter.get(&target).await.map(|c| c == 0).unwrap_or(false) }
    })
    .await;
}

/// A stampede of `increment_once` calls with the same dedupe key admits exactly
/// one increment; a distinct key increments again.
#[tokio::test]
async fn increment_once_is_idempotent_under_concurrent_claims() {
    let h = TestHarness::start().await;
    let target = harness::random_profile();

    // Race FANOUT identical claims on the same key.
    let mut handles = Vec::new();
    for _ in 0..FANOUT {
        let counter = h.counter.clone();
        handles.push(tokio::spawn(async move {
            counter.increment_once(&target, "dedupe-key-A").await
        }));
    }
    let mut winners = 0;
    for handle in handles {
        if handle.await.expect("join").expect("increment_once") {
            winners += 1;
        }
    }
    assert_eq!(winners, 1, "exactly one concurrent claim must win the dedupe key");

    let count = h.counter.get(&target).await.expect("get");
    assert_eq!(count, 1, "the claim-gated counter must reflect a single increment");

    // A distinct dedupe key is a fresh increment.
    let incremented = h
        .counter
        .increment_once(&target, "dedupe-key-B")
        .await
        .expect("increment_once");
    assert!(incremented, "a new dedupe key must increment");
    assert_eq!(h.counter.get(&target).await.expect("get"), 2);
}

/// A badge Redis does not hold is read from the Scylla `counter` column (and
/// re-cached): 0 for a profile never notified, the durable count otherwise.
/// (It used to fail: the column was decoded as a bare i64.)
#[tokio::test]
async fn a_badge_missing_from_redis_is_read_from_scylla() {
    let h = TestHarness::start().await;

    let never = harness::random_profile();
    assert_eq!(h.counter.get(&never).await.expect("read a never-notified badge"), 0);

    let durable = harness::random_profile();
    h.repository.increment_counter(&durable).await.unwrap();
    h.repository.increment_counter(&durable).await.unwrap();
    assert_eq!(h.counter.get(&durable).await.expect("read through to scylla"), 2);
    assert_eq!(h.counter.get(&durable).await.unwrap(), 2, "re-cached");
}


/// #971: clear all up to the newest one shown — older ones go, a newer one
/// survives, the badge counts what is left, and a second clear changes
/// nothing.
#[tokio::test]
async fn clear_all_deletes_up_to_the_bound_and_recounts_the_badge() {
    let h = TestHarness::start().await;
    let target = harness::random_profile();
    for _ in 0..3 {
        harness::dispatch_create(Arc::clone(&h.command_bus), target.as_str(), harness::random_profile().as_str())
            .await
            .expect("create_notification");
        tokio::time::sleep(std::time::Duration::from_millis(5)).await;
    }
    let list = || {
        h.handler.list_notifications(tonic::Request::new(harness::proto::ListNotificationsRequest {
            profile_id: target.as_str(),
            limit:      50,
            page_token: String::new(),
        }))
    };
    let shown = list().await.expect("list").into_inner().notifications;
    assert_eq!(shown.len(), 3);
    // The holder was shown the two older ones only (newest first: index 1).
    let bound = shown[1].created_at_ms;
    let clear = |up_to_ms| {
        h.handler.clear_notifications(tonic::Request::new(harness::proto::ClearNotificationsRequest {
            profile_id: target.as_str(),
            up_to_ms,
        }))
    };
    clear(bound).await.expect("clear");
    let left = list().await.expect("list").into_inner().notifications;
    assert_eq!(left.iter().map(|n| n.notification_id.clone()).collect::<Vec<_>>(), vec![shown[0].notification_id.clone()]);
    assert_eq!(h.counter.get(&target).await.expect("unread"), 1, "the one left is unread");

    clear(bound).await.expect("idempotent");
    assert_eq!(list().await.expect("list").into_inner().notifications.len(), 1);
    assert!(clear(0).await.is_err(), "a bound is required");
}
