//! Scenario — appeal outcomes through the real appeal worker, against the live
//! stores (#744): every profile moderation names is told once, as a platform
//! notice (no sender) about the appeal; a redelivered event counts once; other
//! moderation events tell nobody.

use std::sync::Arc;

use chrono::Utc;
use transport::kafka::config::client::KafkaClientConfig;
use uuid::Uuid;

use notification::application::port::NotificationRepository;
use notification::domain::value_object::{NotificationKind, SubjectKind};
use notification::infrastructure::cache::RedisUnreadCounter;
use notification::infrastructure::persistence::ScyllaNotificationRepository;
use notification::infrastructure::streaming::BroadcastRegistry;
use notification::infrastructure::worker::appeal_worker::{
    AppealNotificationWorker, AppealResolvedPayload, ModerationEventPayload,
};

use crate::notification_it::harness::{ProfileId, TestHarness};

type Worker = AppealNotificationWorker<
    ScyllaNotificationRepository,
    RedisUnreadCounter<ScyllaNotificationRepository>,
    BroadcastRegistry,
>;

fn worker(h: &TestHarness) -> (Worker, Arc<ScyllaNotificationRepository>) {
    let repository = Arc::new(ScyllaNotificationRepository::new(Arc::clone(&h.scylla)));
    let worker = AppealNotificationWorker::new(
        KafkaClientConfig::default(),
        Arc::clone(&repository),
        Arc::new(RedisUnreadCounter::new(h.redis.clone(), Arc::clone(&repository), Arc::clone(&h.config))),
        Arc::clone(&h.stream_registry),
        "it-appeal-notifications",
    );
    (worker, repository)
}

fn profile() -> ProfileId {
    ProfileId::try_from(Uuid::now_v7().to_string().as_str()).unwrap()
}

fn resolved(appeal: Uuid, overturned: bool, profiles: &[&ProfileId]) -> ModerationEventPayload {
    ModerationEventPayload::AppealResolved(AppealResolvedPayload {
        appeal_id: appeal.to_string(),
        overturned,
        profile_ids: profiles.iter().map(|p| p.as_str()).collect(),
        occurred_at: Utc::now(),
    })
}

#[tokio::test]
async fn every_profile_of_the_appellant_is_told_once_by_the_platform() {
    let h = TestHarness::start().await;
    let (w, repository) = worker(&h);
    let (main, brand, stranger) = (profile(), profile(), profile());
    let appeal = Uuid::now_v7();

    let event = resolved(appeal, true, &[&main, &brand]);
    w.process(&event).await.expect("outcome");
    w.process(&event).await.expect("redelivered");
    assert_eq!(h.counter.get(&main).await.unwrap(), 1, "told once");
    assert_eq!(h.counter.get(&brand).await.unwrap(), 1, "every profile of the account");
    assert_eq!(h.counter.get(&stranger).await.unwrap(), 0);

    let (rows, _) = repository.list_paginated(&brand, 10, None).await.unwrap();
    assert_eq!(rows.len(), 1, "one row despite the redelivery");
    let row = &rows[0];
    assert_eq!(row.kind, NotificationKind::AppealOverturned);
    assert_eq!(row.subject_kind, SubjectKind::Appeal);
    assert_eq!(row.subject_id, appeal, "the app opens the appeal");
    assert!(row.sender_profile_id.is_nil() && row.sample_sender_ids.is_empty() && row.sender_count == 0);
}

#[tokio::test]
async fn an_upheld_appeal_is_told_as_such_and_other_events_tell_nobody() {
    let h = TestHarness::start().await;
    let (w, repository) = worker(&h);
    let holder = profile();

    w.process(&ModerationEventPayload::Other).await.expect("ignored");
    assert_eq!(h.counter.get(&holder).await.unwrap(), 0);

    w.process(&resolved(Uuid::now_v7(), false, &[&holder])).await.expect("upheld");
    let (rows, _) = repository.list_paginated(&holder, 10, None).await.unwrap();
    assert_eq!(rows.iter().map(|r| r.kind).collect::<Vec<_>>(), vec![NotificationKind::AppealUpheld]);
}
