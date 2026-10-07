//! Scenario — family supervision notices through the real worker, against the
//! live stores (#670): a pairing tells both sides (from the other side, about
//! the other side's account); an end tells the other side only; a redelivered
//! event counts once.

use std::sync::Arc;

use chrono::Utc;
use transport::kafka::config::client::KafkaClientConfig;
use uuid::Uuid;

use notification::application::port::NotificationRepository;
use notification::domain::value_object::{NotificationKind, SubjectKind};
use notification::infrastructure::cache::RedisUnreadCounter;
use notification::infrastructure::persistence::ScyllaNotificationRepository;
use notification::infrastructure::streaming::BroadcastRegistry;
use notification::infrastructure::worker::supervision_worker::{AccountEventPayload, SupervisionNotificationWorker};

use crate::notification_it::harness::{ProfileId, TestHarness};

type Worker = SupervisionNotificationWorker<
    ScyllaNotificationRepository,
    RedisUnreadCounter<ScyllaNotificationRepository>,
    BroadcastRegistry,
>;

fn worker(h: &TestHarness) -> (Worker, Arc<ScyllaNotificationRepository>) {
    let repository = Arc::new(ScyllaNotificationRepository::new(Arc::clone(&h.scylla)));
    let worker = SupervisionNotificationWorker::new(
        KafkaClientConfig::default(),
        Arc::clone(&repository),
        Arc::new(RedisUnreadCounter::new(h.redis.clone(), Arc::clone(&repository), Arc::clone(&h.config))),
        Arc::clone(&h.stream_registry),
        "it-supervision-notifications",
    );
    (worker, repository)
}

fn profile() -> ProfileId {
    ProfileId::try_from(Uuid::now_v7().to_string().as_str()).unwrap()
}

fn event(kind: &str, ended_by: &str, teen: (Uuid, &ProfileId), parent: (Uuid, &ProfileId)) -> AccountEventPayload {
    serde_json::from_value(serde_json::json!({
        "type": kind,
        "account_id": teen.0.to_string(),
        "supervisor_id": parent.0.to_string(),
        "ended_by": ended_by,
        "teen_profile_ids": [teen.1.as_str()],
        "supervisor_profile_ids": [parent.1.as_str()],
        "occurred_at": Utc::now(),
        "correlation_id": Uuid::now_v7().to_string()
    }))
    .unwrap()
}

#[tokio::test]
async fn a_pairing_tells_both_sides_and_an_end_the_other_side() {
    let h = TestHarness::start().await;
    let (w, repository) = worker(&h);
    let (teen_account, parent_account) = (Uuid::now_v7(), Uuid::now_v7());
    let (teen, parent) = (profile(), profile());

    let started = event("supervision_started", "", (teen_account, &teen), (parent_account, &parent));
    w.process(&started).await.expect("started");
    w.process(&started).await.expect("redelivered");
    assert_eq!(h.counter.get(&teen).await.unwrap(), 1, "the teen is told, once");
    assert_eq!(h.counter.get(&parent).await.unwrap(), 1);

    let (rows, _) = repository.list_paginated(&teen, 10, None).await.unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!((rows[0].kind, rows[0].subject_kind), (NotificationKind::SupervisionStarted, SubjectKind::Account));
    assert_eq!(rows[0].sender_profile_id, parent.as_uuid(), "from the parent");
    assert_eq!(rows[0].subject_id, parent_account, "about the parent's account");

    // The parent ends it: only the teen hears of it.
    w.process(&event("supervision_ended", "by_supervisor", (teen_account, &teen), (parent_account, &parent)))
        .await
        .expect("ended");
    assert_eq!(h.counter.get(&teen).await.unwrap(), 2);
    assert_eq!(h.counter.get(&parent).await.unwrap(), 1);
    let (rows, _) = repository.list_paginated(&teen, 10, None).await.unwrap();
    assert!(rows.iter().any(|r| r.kind == NotificationKind::SupervisionEnded));

    // Other account events tell nobody.
    w.process(&AccountEventPayload::Other).await.expect("ignored");
}
