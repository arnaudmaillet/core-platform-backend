//! Scenario — `comment.created` through the real comment worker, against the
//! live stores: a comment notifies the post's author and a reply the parent's,
//! but a `quiet` one (the post's owner restricted its author, #659) notifies
//! nobody.

use std::sync::Arc;

use fred::interfaces::KeysInterface;
use transport::kafka::config::client::KafkaClientConfig;
use uuid::Uuid;

use notification::infrastructure::cache::{RedisBlockCache, RedisUnreadCounter};
use notification::infrastructure::persistence::ScyllaNotificationRepository;
use notification::infrastructure::worker::comment_worker::{CommentEventPayload, CommentNotificationWorker};

use crate::notification_it::harness::{ProfileId, TestHarness};
use notification::infrastructure::streaming::BroadcastRegistry;

type Worker = CommentNotificationWorker<
    ScyllaNotificationRepository,
    RedisBlockCache,
    RedisUnreadCounter<ScyllaNotificationRepository>,
    BroadcastRegistry,
>;

fn worker(h: &TestHarness) -> Worker {
    let repository = Arc::new(ScyllaNotificationRepository::new(Arc::clone(&h.scylla)));
    CommentNotificationWorker::new(
        KafkaClientConfig::default(),
        h.redis.clone(),
        Arc::clone(&repository),
        Arc::new(RedisBlockCache::new(h.redis.clone(), Arc::clone(&h.config))),
        Arc::new(RedisUnreadCounter::new(h.redis.clone(), repository, Arc::clone(&h.config))),
        Arc::clone(&h.stream_registry),
        Arc::clone(&h.config),
        "it-comment-notifications",
    )
}

fn profile() -> ProfileId {
    ProfileId::try_from(Uuid::now_v7().to_string().as_str()).unwrap()
}

fn event(post: &str, author: &ProfileId, parent: Option<&str>, quiet: bool) -> (String, CommentEventPayload) {
    let comment_id = Uuid::now_v7().to_string();
    let payload = CommentEventPayload {
        comment_id:    comment_id.clone(),
        post_id:       post.to_owned(),
        author_id:     author.as_str(),
        parent_id:     parent.map(str::to_owned),
        created_at_ms: chrono::Utc::now().timestamp_millis(),
        quiet,
    };
    (comment_id, payload)
}

#[tokio::test]
async fn a_restricted_authors_comments_and_replies_notify_nobody() {
    let h = TestHarness::start().await;
    let w = worker(&h);
    let (owner, friend, restricted) = (profile(), profile(), profile());
    let post = Uuid::now_v7().to_string();
    // The post's author, as the mention worker caches it from post.published.
    let _: () = h.redis.inner.set(format!("notification:pa:{post}"), owner.as_str(), None, None, false).await.unwrap();

    // A restricted profile comments: the owner hears nothing.
    let (_, quiet_comment) = event(&post, &restricted, None, true);
    w.process(&quiet_comment).await.expect("quiet comment");
    assert_eq!(h.counter.get(&owner).await.unwrap(), 0, "a restricted comment notifies nobody");

    // A friend comments: the owner is notified (the control).
    let (friend_comment, loud) = event(&post, &friend, None, false);
    w.process(&loud).await.expect("comment");
    assert_eq!(h.counter.get(&owner).await.unwrap(), 1);

    // The restricted profile replies to the friend: the friend hears nothing.
    let (_, quiet_reply) = event(&post, &restricted, Some(&friend_comment), true);
    w.process(&quiet_reply).await.expect("quiet reply");
    assert_eq!(h.counter.get(&friend).await.unwrap(), 0, "a restricted reply notifies nobody");

    // The owner replies to the friend: the friend is notified (the control).
    let (_, owner_reply) = event(&post, &owner, Some(&friend_comment), false);
    w.process(&owner_reply).await.expect("reply");
    assert_eq!(h.counter.get(&friend).await.unwrap(), 1);
}
