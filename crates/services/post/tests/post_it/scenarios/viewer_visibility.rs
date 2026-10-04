//! Scenario — viewer-aware reads against the real store.
//!
//! A draft or a deleted post belongs to its author: anyone else (another member,
//! an anonymous client) gets "not found" from `GetPost` and does not see it in
//! `ListPostsByProfile`. A published post is visible to all. A trusted internal
//! caller (the mesh) sees everything, as before.

use crate::post_it::harness::{self, ProfileId, TestHarness, Viewer};

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
