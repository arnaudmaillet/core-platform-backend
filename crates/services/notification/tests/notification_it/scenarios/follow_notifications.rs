//! Scenario — follows, follow requests and their approval through the real
//! follow worker, against the live stores (#755): the owner of a private
//! profile is told of a request, the requester of its approval (not the owner
//! again), a public profile of a new follower; a blocked sender tells nobody,
//! and a redelivered event counts once.

use std::sync::Arc;

use chrono::Utc;
use fred::interfaces::KeysInterface;
use transport::kafka::config::client::KafkaClientConfig;
use uuid::Uuid;

use notification::infrastructure::cache::{RedisBlockCache, RedisUnreadCounter};
use notification::infrastructure::persistence::ScyllaNotificationRepository;
use notification::infrastructure::streaming::BroadcastRegistry;
use notification::infrastructure::worker::follow_worker::{FollowEventPayload, FollowNotificationWorker};

use crate::notification_it::harness::{ProfileId, TestHarness};

type Worker = FollowNotificationWorker<
    ScyllaNotificationRepository,
    RedisBlockCache,
    RedisUnreadCounter<ScyllaNotificationRepository>,
    BroadcastRegistry,
>;

fn worker(h: &TestHarness) -> Worker {
    let repository = Arc::new(ScyllaNotificationRepository::new(Arc::clone(&h.scylla)));
    FollowNotificationWorker::new(
        KafkaClientConfig::default(),
        Arc::clone(&repository),
        Arc::new(RedisBlockCache::new(h.redis.clone(), Arc::clone(&h.config))),
        Arc::new(RedisUnreadCounter::new(h.redis.clone(), repository, Arc::clone(&h.config))),
        Arc::clone(&h.stream_registry),
        "it-follow-notifications",
    )
}

fn profile() -> ProfileId {
    ProfileId::try_from(Uuid::now_v7().to_string().as_str()).unwrap()
}

fn event(actor: &ProfileId, target: &ProfileId, requested: bool, via_request: bool) -> FollowEventPayload {
    let now = Some(Utc::now());
    FollowEventPayload {
        actor_id: actor.as_str(),
        target_id: target.as_str(),
        followed_at: if requested { None } else { now },
        requested_at: if requested { now } else { None },
        via_request,
    }
}

#[tokio::test]
async fn a_request_tells_the_owner_and_its_approval_tells_the_requester() {
    let h = TestHarness::start().await;
    let w = worker(&h);
    let (requester, owner) = (profile(), profile());

    let request = event(&requester, &owner, true, false);
    w.process(&request).await.expect("request");
    w.process(&request).await.expect("redelivered");
    assert_eq!(h.counter.get(&owner).await.unwrap(), 1, "the owner is told once");
    assert_eq!(h.counter.get(&requester).await.unwrap(), 0);

    w.process(&event(&requester, &owner, false, true)).await.expect("approval");
    assert_eq!(h.counter.get(&requester).await.unwrap(), 1, "the requester is told");
    assert_eq!(h.counter.get(&owner).await.unwrap(), 1, "not the owner again");
}

#[tokio::test]
async fn a_follow_tells_the_followee_unless_blocked() {
    let h = TestHarness::start().await;
    let w = worker(&h);
    let (fan, star, blocked) = (profile(), profile(), profile());

    w.process(&event(&fan, &star, false, false)).await.expect("follow");
    assert_eq!(h.counter.get(&star).await.unwrap(), 1);

    // The star blocked this profile (as social-graph populates the cache).
    let key = format!("notification:block:{}:{}", blocked.as_str(), star.as_str());
    let _: () = h.redis.inner.set(key, "1", None, None, false).await.unwrap();
    w.process(&event(&blocked, &star, false, false)).await.expect("blocked follow");
    assert_eq!(h.counter.get(&star).await.unwrap(), 1, "a blocked profile tells nobody");
}
