//! Scenario — multi-table adjacency consistency under concurrency.
//!
//! A follow writes the `followers` adjacency (target → follower) and the
//! `following` adjacency (follower → target). The two projections of the same
//! edge must never diverge: after a burst of concurrent follows of one target,
//! the target's follower list and each follower's following list must agree. This
//! is the concurrency / logged-batch-atomicity axis.

use std::sync::Arc;

use crate::social_graph_it::harness::{self, TestHarness, DEADLINE};

const FOLLOWERS: usize = 6;

#[tokio::test]
async fn concurrent_follows_keep_followers_and_following_in_agreement() {
    let h = TestHarness::start().await;

    let target = harness::random_profile();
    let actors: Vec<_> = (0..FOLLOWERS).map(|_| harness::random_profile()).collect();

    // Fire FOLLOWERS concurrent follows of the same target.
    let mut handles = Vec::new();
    for actor in &actors {
        let bus = Arc::clone(&h.command_bus);
        let actor = *actor;
        handles.push(tokio::spawn(harness::dispatch_follow(bus, actor.as_str(), target.as_str())));
    }
    for handle in handles {
        handle.await.expect("join").expect("follow_profile");
    }

    // The `followers` table must list every actor.
    harness::await_until("followers table lists every follower", DEADLINE, || {
        let h = &h;
        let actors = &actors;
        async move {
            let followers = h.followers(&target).await;
            followers.len() == actors.len()
                && actors.iter().all(|a| harness::contains(&followers, a))
        }
    })
    .await;

    // The reciprocal `following` adjacency must agree for every actor.
    for actor in &actors {
        let following = h.following(actor).await;
        assert!(
            harness::contains(&following, &target),
            "following table disagrees with followers for one edge",
        );
    }
}
