//! #832 over live Scylla: a top-level comment's reply count (written with
//! each reply, gone with its purge), and its replies by popularity — most
//! liked first, ties to the newest, paged without repeats — falling back to
//! the newest order when engagement does not answer, and carrying on past the
//! ranked window newest first.

use comment::application::query::list_replies::{ReplySort, POPULARITY_WINDOW};

use crate::comment_it::harness::{self, TestHarness};

fn count_of(h: &[comment::application::port::CommentSummary], id: &str) -> u64 {
    h.iter().find(|c| c.comment_id.as_str() == id).map(|c| c.reply_count).expect("listed")
}

#[tokio::test]
async fn top_level_comments_carry_their_reply_count() {
    let h = TestHarness::start().await;
    let (post, author) = (harness::random_post(), harness::random_author());
    let busy = h.create(&post, None, &author).await;
    let quiet = h.create(&post, None, &author).await;
    let mut replies = Vec::new();
    for _ in 0..3 {
        replies.push(h.create(&post, Some(&busy), &author).await);
    }

    let listed = h.list_top_level(&post).await;
    assert_eq!(count_of(&listed, &busy), 3);
    assert_eq!(count_of(&listed, &quiet), 0, "no replies call needed");

    // A deleted reply is purged, and stops counting.
    h.delete(&replies[0], &author).await;
    assert_eq!(count_of(&h.list_top_level(&post).await, &busy), 2);
}

#[tokio::test]
async fn replies_by_popularity_most_liked_first_ties_to_the_newest() {
    let h = TestHarness::start().await;
    let (post, author) = (harness::random_post(), harness::random_author());
    let top = h.create(&post, None, &author).await;
    // r[0] oldest … r[4] newest.
    let mut r = Vec::new();
    for _ in 0..5 {
        r.push(h.create(&post, Some(&top), &author).await);
        tokio::time::sleep(std::time::Duration::from_millis(2)).await;
    }
    h.likes.set(&r[2], 9);
    h.likes.set(&r[0], 2);
    h.likes.set(&r[4], 2);

    let expected = vec![r[2].clone(), r[4].clone(), r[0].clone(), r[3].clone(), r[1].clone()];
    // The thread's top two, as the stream shows them.
    let (first, next) = h.replies_page(&post, &top, ReplySort::Popularity, 2, None).await;
    assert_eq!(first.iter().map(|c| c.comment_id.as_str()).collect::<Vec<_>>(), expected[..2]);
    assert!(next.is_some());
    // Paged in the same order, each reply once, until the token is empty.
    assert_eq!(h.all_replies(&post, &top, ReplySort::Popularity, 2).await, expected);

    // NEWEST stays the default order, and pages one at a time without gaps.
    let newest: Vec<String> = r.iter().rev().cloned().collect();
    assert_eq!(h.all_replies(&post, &top, ReplySort::Newest, 1).await, newest);

    // Engagement down: popularity pages come back newest first.
    h.likes.set_down(true);
    assert_eq!(h.all_replies(&post, &top, ReplySort::Popularity, 2).await, newest);
}

#[tokio::test]
async fn popularity_carries_on_past_its_window_newest_first() {
    let h = TestHarness::start().await;
    let (post, author) = (harness::random_post(), harness::random_author());
    let top = h.create(&post, None, &author).await;
    let total = POPULARITY_WINDOW as usize + 2;
    let mut r = Vec::with_capacity(total);
    for _ in 0..total {
        r.push(h.create(&post, Some(&top), &author).await);
    }
    // The oldest reply is past the window: its likes do not rank it.
    h.likes.set(&r[0], 50);
    h.likes.set(&r[total - 1], 3);

    let paged = h.all_replies(&post, &top, ReplySort::Popularity, 100).await;
    assert_eq!(paged.len(), total, "every reply, once");
    let unique: std::collections::HashSet<_> = paged.iter().collect();
    assert_eq!(unique.len(), total);
    assert_eq!(paged[0], r[total - 1], "the window's most liked first");
    assert_eq!(paged[total - 2..], [r[1].clone(), r[0].clone()], "then the older ones, newest first");

    assert_eq!(count_of(&h.list_top_level(&post).await, &top), total as u64);
}

/// The real engagement adapter over tonic (#832): comment ids as
/// `BatchGetLikes` comment targets, at most 100 per call, counts back in order.
#[tokio::test]
async fn likes_come_from_engagement_batch_get_likes() {
    use comment::application::port::CommentLikes;
    use comment::domain::value_object::CommentId;
    use comment::infrastructure::client::GrpcCommentLikes;

    let (engagement, channel) = crate::comment_it::harness::peers::start_engagement().await;
    let ids: Vec<CommentId> = (0..150).map(|_| CommentId::try_from(uuid::Uuid::now_v7().to_string().as_str()).unwrap()).collect();
    engagement.likes.lock().unwrap().insert(ids[3].as_str(), 7);
    engagement.likes.lock().unwrap().insert(ids[120].as_str(), 2);

    let counts = GrpcCommentLikes::new(channel).counts(&ids).await.expect("engagement answers");
    assert_eq!(counts.len(), 150);
    assert_eq!(counts[&ids[3]], 7);
    assert_eq!(counts[&ids[120]], 2);
    assert_eq!(counts[&ids[0]], 0);
    assert_eq!(*engagement.calls.lock().unwrap(), vec![100, 50], "capped at 100 targets a call");
}
