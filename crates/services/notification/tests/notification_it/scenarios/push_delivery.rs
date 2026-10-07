//! Scenario — push delivery over the real tables (#654): a notification goes to
//! the recipient's registered devices with the sender's name and the unread
//! badge; turning its category off stops that push; a redelivered event pushes
//! once.

use std::sync::Arc;
use std::time::Duration;

use chrono::Utc;
use tonic::Request;
use transport::kafka::config::client::KafkaClientConfig;
use uuid::Uuid;

use notification::application::port::PushNotifier;
use notification::application::push_dispatcher::PushDispatcher;
use notification::infrastructure::cache::{RedisBlockCache, RedisUnreadCounter};
use notification::infrastructure::persistence::{ScyllaNotificationRepository, ScyllaPushSettings};
use notification::infrastructure::worker::follow_worker::{FollowEventPayload, FollowNotificationWorker};

use crate::notification_it::harness::{
    await_until, proto, random_profile, AliceNames, ProfileId, TestHarness, DEADLINE,
};

async fn register(h: &TestHarness, profile: &ProfileId) -> String {
    let token = Uuid::now_v7().to_string();
    h.handler
        .register_device(Request::new(proto::RegisterDeviceRequest {
            profile_id:  profile.as_str(),
            device_id:   "phone".into(),
            token:       token.clone(),
            platform:    proto::DevicePlatform::Ios as i32,
            environment: proto::PushEnvironment::Production as i32,
            timezone:    "UTC".into(),
        }))
        .await
        .expect("register");
    token
}

async fn likes(h: &TestHarness, profile: &ProfileId, on: bool) {
    h.handler
        .update_notification_preferences(Request::new(proto::UpdateNotificationPreferencesRequest {
            profile_id: profile.as_str(),
            categories: vec![proto::CategoryChannels {
                category: proto::PushCategory::Likes as i32,
                push:     on,
                email:    false,
            }],
            ..Default::default()
        }))
        .await
        .expect("update");
}

#[tokio::test]
async fn a_like_is_pushed_with_the_name_and_badge_until_likes_are_off() {
    let h = TestHarness::start().await;
    let (target, sender) = (random_profile(), random_profile());
    let token = register(&h, &target).await;

    h.create(&target, &sender).await;
    await_until("the like is pushed", DEADLINE, || async { !h.pushes.sent_to(&token).is_empty() }).await;
    let push = h.pushes.sent_to(&token).remove(0);
    assert_eq!(push.loc_key, "NTF_PUSH_REACTION");
    assert_eq!(push.loc_args, vec!["Alice".to_owned()]);
    assert_eq!(push.badge, Some(1));

    // Likes off: the next like is in the feed, not pushed.
    likes(&h, &target, false).await;
    h.create(&target, &sender).await;
    assert_eq!(h.counter.get(&target).await.unwrap(), 2, "in the feed");
    tokio::time::sleep(Duration::from_millis(500)).await;
    assert_eq!(h.pushes.sent_to(&token).len(), 1, "not pushed");
}

#[tokio::test]
async fn a_redelivered_follow_is_pushed_once() {
    let h = TestHarness::start().await;
    let (follower, owner) = (random_profile(), random_profile());
    let token = register(&h, &owner).await;

    let repository = Arc::new(ScyllaNotificationRepository::new(Arc::clone(&h.scylla)));
    let settings = Arc::new(ScyllaPushSettings::new(Arc::clone(&h.scylla)));
    let push: Arc<dyn PushNotifier> = Arc::new(PushDispatcher {
        devices:     Arc::clone(&settings) as _,
        preferences: settings as _,
        counter:     Arc::clone(&h.counter),
        names:       Arc::new(AliceNames),
        sender:      Arc::clone(&h.pushes) as _,
    });
    let worker = FollowNotificationWorker::new(
        KafkaClientConfig::default(),
        Arc::clone(&repository),
        Arc::new(RedisBlockCache::new(h.redis.clone(), Arc::clone(&h.config))),
        Arc::new(RedisUnreadCounter::new(h.redis.clone(), repository, Arc::clone(&h.config))),
        Arc::clone(&h.stream_registry),
        "it-push-follow",
    )
    .with_push(push);

    let followed = FollowEventPayload {
        actor_id:     follower.as_str(),
        target_id:    owner.as_str(),
        followed_at:  Some(Utc::now()),
        requested_at: None,
        via_request:  false,
        withdrawn_at: None,
    };
    worker.process(&followed).await.expect("follow");
    worker.process(&followed).await.expect("redelivered");
    await_until("the follow is pushed", DEADLINE, || async { !h.pushes.sent_to(&token).is_empty() }).await;
    tokio::time::sleep(Duration::from_millis(500)).await;
    let pushes = h.pushes.sent_to(&token);
    assert_eq!(pushes.len(), 1, "once");
    assert_eq!(pushes[0].loc_key, "NTF_PUSH_FOLLOW");
}
