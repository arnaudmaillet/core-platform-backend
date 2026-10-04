//! Scenario — viewer-aware reads against the real store.
//!
//! A draft or a deleted post belongs to its author: anyone else (another member,
//! an anonymous client) gets "not found" from `GetPost` and does not see it in
//! `ListPostsByProfile`. A published post is visible to all. A trusted internal
//! caller (the mesh) sees everything, as before. A post moderation removed goes
//! back to its author alone, in both tables, until the enforcement is reversed.

use crate::post_it::harness::{
    self, ContentAccess, ModerationRestriction, ProfileId, TestHarness, Viewer,
};

#[tokio::test]
async fn drafts_and_deleted_posts_are_visible_to_their_author_only() {
    let h = TestHarness::start().await;

    let author_id = harness::random_id();
    let post_id = harness::random_id();
    let author = Viewer::Profiles(vec![ProfileId::try_from(author_id.as_str()).unwrap()]);
    let stranger = Viewer::Profiles(vec![ProfileId::try_from(harness::random_id().as_str()).unwrap()]);

    // Draft: the author and the mesh see it; nobody else does.
    h.create(&post_id, &author_id).await;
    assert!(h.get_as(&post_id, author.clone()).await.is_ok());
    assert!(h.get_as(&post_id, Viewer::Internal).await.is_ok());
    assert!(h.get_as(&post_id, stranger.clone()).await.is_err(), "a draft is not found for others");
    assert!(h.get_as(&post_id, Viewer::Anonymous).await.is_err());
    assert_eq!(h.list_as(&author_id, author.clone()).await.len(), 1);
    assert!(h.list_as(&author_id, stranger.clone()).await.is_empty());

    // Published: visible to everyone.
    h.publish(&post_id, &author_id).await;
    assert!(h.get_as(&post_id, Viewer::Anonymous).await.is_ok());
    assert_eq!(h.list_as(&author_id, stranger.clone()).await.len(), 1);

    // Deleted: back to the author alone (the tombstone stays readable to it).
    h.delete(&post_id, &author_id).await;
    assert!(h.get_as(&post_id, author.clone()).await.is_ok());
    assert!(h.get_as(&post_id, stranger.clone()).await.is_err());
    assert!(h.get_as(&post_id, Viewer::Anonymous).await.is_err());
    assert!(h.list_as(&author_id, Viewer::Anonymous).await.is_empty());
    assert_eq!(h.list_as(&author_id, author).await.len(), 1);
}

#[tokio::test]
async fn a_removed_post_is_its_authors_alone_until_reversed() {
    let h = TestHarness::start().await;

    let author_id = harness::random_id();
    let post_id = harness::random_id();
    let author = Viewer::Profiles(vec![ProfileId::try_from(author_id.as_str()).unwrap()]);
    h.create(&post_id, &author_id).await;
    h.publish(&post_id, &author_id).await;

    // Taken down (v1): gone for everyone else, kept (and labelled) for the author.
    h.moderate(&post_id, ModerationRestriction::Removed, 1).await;
    assert!(h.get_as(&post_id, Viewer::Anonymous).await.is_err());
    assert!(h.list_as(&author_id, Viewer::Anonymous).await.is_empty());
    let own = h.get_as(&post_id, author.clone()).await.expect("the author still sees it");
    assert_eq!(own.moderation().restriction, ModerationRestriction::Removed);
    let own_list = h.list_as(&author_id, author.clone()).await;
    assert_eq!(own_list[0].moderation, ModerationRestriction::Removed, "posts_by_profile too");

    // Reversed (v2): visible again.
    h.moderate(&post_id, ModerationRestriction::None, 2).await;
    assert!(h.get_as(&post_id, Viewer::Anonymous).await.is_ok());
    assert_eq!(h.list_as(&author_id, Viewer::Anonymous).await.len(), 1);

    // A redelivered takedown (v1) is stale and changes nothing.
    h.moderate(&post_id, ModerationRestriction::Removed, 1).await;
    assert!(h.get_as(&post_id, Viewer::Anonymous).await.is_ok());

    // Limited stays readable (discovery applies it, not GetPost).
    h.moderate(&post_id, ModerationRestriction::Limited, 3).await;
    let limited = h.get_as(&post_id, Viewer::Anonymous).await.expect("limited is readable");
    assert_eq!(limited.moderation().restriction, ModerationRestriction::Limited);

    // Age-gated: an adult reads it; a guest or a 13–17 reader does not (not in
    // the list either); the author always does.
    h.moderate(&post_id, ModerationRestriction::AgeGated, 4).await;
    let adult = Viewer::Profiles(vec![ProfileId::try_from(harness::random_id().as_str()).unwrap()]);
    assert!(h.get_rated(&post_id, adult.clone(), true).await.is_ok());
    assert!(h.get_rated(&post_id, adult.clone(), false).await.is_err(), "13–17 / guest");
    assert!(h.list_rated(&author_id, adult, false).await.is_empty());
    assert!(h.get_rated(&post_id, author.clone(), false).await.is_ok(), "the author");

    // An outcome for a post that does not exist is a no-op, not an error.
    h.moderate(&harness::random_id(), ModerationRestriction::Removed, 1).await;
}

#[tokio::test]
async fn the_authors_audience_gates_its_posts_and_an_outage_fails_closed() {
    let h = TestHarness::start().await;

    let author_id = harness::random_id();
    let post_id = harness::random_id();
    let author = Viewer::Profiles(vec![ProfileId::try_from(author_id.as_str()).unwrap()]);
    let reader = Viewer::Profiles(vec![ProfileId::try_from(harness::random_id().as_str()).unwrap()]);
    h.create(&post_id, &author_id).await;
    h.publish(&post_id, &author_id).await;

    // A private author the reader does not follow, or a block: no posts.
    for access in [ContentAccess::HeaderOnly, ContentAccess::Hidden] {
        h.gate.set(&author_id, access);
        assert!(h.get_as(&post_id, reader.clone()).await.is_err(), "{access:?}");
        assert!(h.get_as(&post_id, Viewer::Anonymous).await.is_err(), "{access:?}");
        assert!(h.list_as(&author_id, reader.clone()).await.is_empty(), "{access:?}");
        // The author and the mesh never depend on the check.
        assert!(h.get_as(&post_id, author.clone()).await.is_ok());
        assert!(h.get_as(&post_id, Viewer::Internal).await.is_ok());
    }

    // Visible (public, or a follower of a private author).
    h.gate.set(&author_id, ContentAccess::Visible);
    assert!(h.get_as(&post_id, reader.clone()).await.is_ok());
    assert_eq!(h.list_as(&author_id, reader.clone()).await.len(), 1);

    // social-graph down: fail closed (an error, never the post), except for
    // the author and the mesh.
    h.gate.set_down(true);
    let Err(err) = h.get_as(&post_id, reader.clone()).await else {
        panic!("an outage must not serve the post");
    };
    assert!(err.to_string().contains("audience check unavailable"), "{err}");
    assert!(h.get_as(&post_id, author).await.is_ok());
    assert!(h.get_as(&post_id, Viewer::Internal).await.is_ok());
}
