//! Scenario — restricted commenters (#659): the comment of a profile the post's
//! owner restricted is announced quietly (no notification for anyone), held
//! first or not; everyone else's is announced as usual.

use cqrs::{CommandBus, Envelope};
use uuid::Uuid;

use comment::application::command::review_held::ReviewHeldCommentCommand;

use crate::comment_it::harness::{self, ProfileId, TestHarness};

fn quiet(h: &TestHarness, comment_id: &str) -> bool {
    h.published.created(comment_id).expect("announced").quiet
}

#[tokio::test]
async fn a_restricted_commenters_comment_is_announced_quietly() {
    let h = TestHarness::start().await;
    let (owner, restricted, other) = (harness::random_author(), harness::random_author(), harness::random_author());
    let post = harness::random_post();
    h.gate.set_post_author(&post, &ProfileId::try_from(owner.as_str()).unwrap());
    h.gate.restrict(&owner, &restricted);

    let top = h.create(&post, None, &restricted).await;
    let reply = h.create(&post, Some(&top), &restricted).await;
    let from_other = h.create(&post, Some(&top), &other).await;
    let from_owner = h.create(&post, None, &owner).await;

    assert!(quiet(&h, &top) && quiet(&h, &reply), "nobody is told about a restricted profile's comments");
    assert!(!quiet(&h, &from_other));
    assert!(!quiet(&h, &from_owner));
}

#[tokio::test]
async fn approving_a_held_comment_keeps_a_restriction_quiet() {
    let h = TestHarness::start().await;
    let (owner, restricted) = (harness::random_author(), harness::random_author());
    let post = harness::random_post();
    h.gate.set_post_author(&post, &ProfileId::try_from(owner.as_str()).unwrap());
    h.gate.limit(&post);
    h.gate.restrict(&owner, &restricted);

    let held = h.create(&post, None, &restricted).await;
    assert!(h.published.created(&held).is_none(), "held: not announced");

    let cmd = ReviewHeldCommentCommand { comment_id: held.clone(), owner_id: owner, approve: true };
    h.command_bus.dispatch(Envelope::new(Uuid::now_v7(), cmd)).await.expect("approve");
    assert!(quiet(&h, &held));
}
