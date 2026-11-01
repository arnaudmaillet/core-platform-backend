//! #976 over live Scylla: the first top-level comments of many posts in one
//! call — per post the start of the viewer's ListTopLevel, through the same
//! read gate — and its latency at a page of tiles.

use std::time::Instant;

use comment::domain::comment_filter::CommentFilter;

use crate::comment_it::harness::{self, ProfileId, TestHarness, Viewer};

fn viewer(id: &str) -> Viewer {
    Viewer::Profiles(vec![ProfileId::try_from(id).unwrap()])
}

fn ids(comments: &[comment::application::port::CommentSummary]) -> Vec<String> {
    comments.iter().map(|c| c.comment_id.as_str()).collect()
}

#[tokio::test]
async fn previews_are_the_start_of_each_viewers_list_top_level() {
    let h = TestHarness::start().await;
    let (author, reader) = (harness::random_author(), harness::random_author());
    let (busy, quiet, silent) = (harness::random_post(), harness::random_post(), harness::random_post());
    for _ in 0..8 {
        h.create(&busy, None, &author).await;
    }
    let only = h.create(&quiet, None, &author).await;
    h.create(&quiet, Some(&only), &author).await;

    let previews = h.previews(&[busy.clone(), quiet.clone(), silent.clone(), busy.clone()], 3, viewer(&reader)).await;
    let posts: Vec<String> = previews.iter().map(|p| p.post_id.as_str()).collect();
    assert_eq!(posts, vec![busy.clone(), quiet.clone(), silent.clone()], "the request's order, each post once");
    let listed = h.try_list_top_level_as(&busy, viewer(&reader)).await.unwrap();
    assert_eq!(ids(&previews[0].comments), ids(&listed[..3]), "a prefix of ListTopLevel, in its order");
    assert_eq!(previews[1].comments[0].reply_count, 1, "reply counts filled");
    assert!(previews[2].comments.is_empty(), "a readable post without comments has an empty preview");
}

#[tokio::test]
async fn the_read_gate_applies_per_viewer_and_the_margin_fills_the_preview() {
    let h = TestHarness::start().await;
    let (owner, commenter, mallory, reader) =
        (harness::random_author(), harness::random_author(), harness::random_author(), harness::random_author());
    let owner_id = ProfileId::try_from(owner.as_str()).unwrap();
    let post = harness::random_post();
    h.gate.set_post_author(&post, &owner_id);
    h.filters
        .set(&owner_id, &CommentFilter { hidden_words: vec!["spoiler".into()], filter_offensive: false })
        .await
        .expect("filters");
    let fine = h.create_with_body(&post, &commenter, "great shot").await;
    // Newer and hidden from the reader: a blocked author's, a hidden word.
    for _ in 0..3 {
        h.create_with_body(&post, &mallory, "hello").await;
    }
    h.create_with_body(&post, &commenter, "spoiler inside").await;
    h.gate.hide_author(&mallory);

    let preview = h.previews(std::slice::from_ref(&post), 1, viewer(&reader)).await;
    assert_eq!(ids(&preview[0].comments), vec![fine.clone()], "past the hidden ones, the margin still fills it");
    let listed = h.try_list_top_level_as(&post, viewer(&reader)).await.unwrap();
    assert_eq!(ids(&listed)[0], fine, "the same first comment as ListTopLevel");

    let unreadable = harness::random_post();
    h.create(&unreadable, None, &commenter).await;
    h.gate.post_unreadable(&unreadable);
    let previews = h.previews(&[unreadable, post.clone()], 5, viewer(&reader)).await;
    assert_eq!(previews.len(), 1, "an unreadable post has no entry");
    assert_eq!(previews[0].post_id.as_str(), post);
}

#[tokio::test]
async fn a_held_comment_previews_to_its_author_and_the_owner_only() {
    let h = TestHarness::start().await;
    let (owner, commenter, reader) = (harness::random_author(), harness::random_author(), harness::random_author());
    let post = harness::random_post();
    h.gate.set_post_author(&post, &ProfileId::try_from(owner.as_str()).unwrap());
    h.gate.limit(&post);
    let held = h.create_with_body(&post, &commenter, "first!").await;

    assert!(h.previews(std::slice::from_ref(&post), 5, viewer(&reader)).await[0].comments.is_empty());
    assert_eq!(ids(&h.previews(std::slice::from_ref(&post), 5, viewer(&owner)).await[0].comments), vec![held.clone()]);
    assert_eq!(ids(&h.previews(std::slice::from_ref(&post), 5, viewer(&commenter)).await[0].comments), vec![held]);
}

/// A page of tiles: 50 posts × 5 comments each, against the local Scylla.
#[tokio::test]
async fn fifty_posts_preview_within_budget() {
    let h = TestHarness::start().await;
    let (author, reader) = (harness::random_author(), harness::random_author());
    let posts: Vec<String> = (0..50).map(|_| harness::random_post()).collect();
    for post in &posts {
        for _ in 0..6 {
            h.create(post, None, &author).await;
        }
    }
    // Warm the prepared statements, then measure.
    h.previews(&posts[..1], 5, viewer(&reader)).await;
    let started = Instant::now();
    let previews = h.previews(&posts, 5, viewer(&reader)).await;
    let took = started.elapsed();
    eprintln!("ListPreviews: 50 posts x 5 comments in {took:?}");
    assert_eq!(previews.len(), 50);
    assert!(previews.iter().all(|p| p.comments.len() == 5));
    assert!(took.as_millis() < 1_000, "{took:?}");
}
