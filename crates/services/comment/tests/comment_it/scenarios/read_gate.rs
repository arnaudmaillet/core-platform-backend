//! Scenario — viewer-aware comment reads over the real store, with a scripted
//! read gate standing in for post GetPost + social-graph CheckAccess.

use crate::comment_it::harness::{self, ProfileId, TestHarness, Viewer};

fn reader() -> Viewer {
    Viewer::Profiles(vec![ProfileId::try_from(harness::random_author().as_str()).unwrap()])
}

#[tokio::test]
async fn the_post_and_its_authors_gate_the_comments_and_an_outage_fails_closed() {
    let h = TestHarness::start().await;
    let post = harness::random_post();
    let (alice, mallory) = (harness::random_author(), harness::random_author());
    let from_alice = h.create(&post, None, &alice).await;
    let from_mallory = h.create(&post, None, &mallory).await;

    // Readable post, no hidden author: everything.
    assert_eq!(h.try_list_top_level_as(&post, reader()).await.unwrap().len(), 2);

    // A hidden (blocked / suspended) author's comments disappear, list and point read.
    h.gate.hide_author(&mallory);
    let seen = h.try_list_top_level_as(&post, reader()).await.unwrap();
    assert!(harness::summaries_contain(&seen, &from_alice));
    assert!(!harness::summaries_contain(&seen, &from_mallory));
    assert!(h.get_as(&from_mallory, reader()).await.is_err());
    assert!(h.get_as(&from_alice, reader()).await.is_ok());

    // An unreadable post (draft, takedown, private/blocked/hidden author): nothing.
    h.gate.post_unreadable(&post);
    assert!(h.try_list_top_level_as(&post, reader()).await.unwrap().is_empty());
    assert!(h.try_list_top_level_as(&post, Viewer::Profiles(vec![])).await.unwrap().is_empty());
    assert!(h.get_as(&from_alice, reader()).await.is_err());

    // The mesh is never gated.
    assert_eq!(h.list_top_level(&post).await.len(), 2);

    // The gate is down: an error, never the comments.
    h.gate.set_down(true);
    let Err(err) = h.try_list_top_level_as(&post, reader()).await else {
        panic!("an outage must not serve the comments");
    };
    assert!(err.to_string().contains("audience check unavailable"), "{err}");
    assert_eq!(h.list_top_level(&post).await.len(), 2, "the mesh still reads");
}

/// Writing is gated too (#656): the post must be readable to the commenter and
/// its author must take their comments; the gate failing refuses the write.
#[tokio::test]
async fn comments_are_refused_where_the_author_or_the_post_does_not_allow_them() {
    let h = TestHarness::start().await;
    let (open, restricted, unreadable) = (harness::random_post(), harness::random_post(), harness::random_post());
    let commenter = harness::random_author();
    h.gate.restrict_comments(&restricted);
    h.gate.post_unreadable(&unreadable);

    h.try_create(&open, &commenter).await.expect("an open post takes comments");

    let err = h.try_create(&restricted, &commenter).await.unwrap_err();
    assert!(err.to_string().contains("does not take comments"), "{err}");
    let err = h.try_create(&unreadable, &commenter).await.unwrap_err();
    assert!(err.to_string().contains("post not found"), "{err}");
    assert!(h.list_top_level(&restricted).await.is_empty(), "nothing written");

    h.gate.set_down(true);
    let err = h.try_create(&open, &commenter).await.unwrap_err();
    assert!(err.to_string().contains("unavailable"), "{err}");
}
