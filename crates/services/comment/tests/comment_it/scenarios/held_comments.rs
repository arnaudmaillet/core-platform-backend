//! Scenario — held comments (#669): while the post owner's temporary limit
//! covers a commenter, the comment is stored but held — seen by its author and
//! the owner only, not announced — until the owner approves (it shows, and is
//! announced) or declines it (it goes).

use cqrs::{CommandBus, Envelope};
use uuid::Uuid;

use comment::application::command::review_held::ReviewHeldCommentCommand;

use crate::comment_it::harness::{self, ProfileId, TestHarness, Viewer};

fn viewer(id: &str) -> Viewer {
    Viewer::Profiles(vec![ProfileId::try_from(id).unwrap()])
}

async fn review(h: &TestHarness, comment_id: &str, owner: &str, approve: bool) -> Result<(), cqrs::CqrsError> {
    let cmd = ReviewHeldCommentCommand { comment_id: comment_id.to_owned(), owner_id: owner.to_owned(), approve };
    h.command_bus.dispatch(Envelope::new(Uuid::now_v7(), cmd)).await
}

#[tokio::test]
async fn a_held_comment_waits_for_the_owners_review() {
    let h = TestHarness::start().await;
    let (owner, commenter, reader) = (harness::random_author(), harness::random_author(), harness::random_author());
    let post = harness::random_post();
    h.gate.set_post_author(&post, &ProfileId::try_from(owner.as_str()).unwrap());
    h.gate.limit(&post);

    let held = h.create_with_body(&post, &commenter, "first!").await;
    let declined = h.create_with_body(&post, &commenter, "spam").await;
    let from_owner = h.create_with_body(&post, &owner, "thanks all").await;

    // Held: the author and the owner see them; anyone else does not.
    assert_eq!(h.try_list_top_level_as(&post, viewer(&reader)).await.unwrap().len(), 1, "only the owner's own");
    assert!(h.get_as(&held, viewer(&reader)).await.is_err());
    let seen_by_owner = h.try_list_top_level_as(&post, viewer(&owner)).await.unwrap();
    assert_eq!(seen_by_owner.len(), 3);
    assert!(seen_by_owner.iter().filter(|c| c.comment_id.as_str() != from_owner).all(|c| c.held));
    assert_eq!(h.try_list_top_level_as(&post, viewer(&commenter)).await.unwrap().len(), 3);

    // Only the post's owner reviews.
    assert!(review(&h, &held, &reader, true).await.is_err());
    review(&h, &held, &owner, true).await.expect("approve");
    review(&h, &declined, &owner, false).await.expect("decline");

    let seen = h.try_list_top_level_as(&post, viewer(&reader)).await.unwrap();
    let ids: Vec<String> = seen.iter().map(|c| c.comment_id.as_str()).collect();
    assert!(ids.contains(&held) && !ids.contains(&declined), "{ids:?}");
    assert!(seen.iter().all(|c| !c.held));
    assert!(review(&h, &held, &owner, true).await.is_err(), "not held any more");
}
