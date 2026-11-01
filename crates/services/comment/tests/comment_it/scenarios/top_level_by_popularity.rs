//! #977 over live Scylla: a post's top-level comments by popularity — most
//! liked first, ties to the newest, paged without repeats — with their reply
//! counts, the newest order by default and when engagement does not answer,
//! and the read gate in both orders.

use comment::application::query::popularity::CommentSort;

use crate::comment_it::harness::{self, ProfileId, TestHarness, Viewer};

fn reader() -> Viewer {
    Viewer::Profiles(vec![ProfileId::try_from(harness::random_author().as_str()).unwrap()])
}

#[tokio::test]
async fn top_level_comments_by_popularity_most_liked_first_ties_to_the_newest() {
    let h = TestHarness::start().await;
    let (post, author) = (harness::random_post(), harness::random_author());
    // c[0] oldest … c[4] newest.
    let mut c = Vec::new();
    for _ in 0..5 {
        c.push(h.create(&post, None, &author).await);
        tokio::time::sleep(std::time::Duration::from_millis(2)).await;
    }
    h.create(&post, Some(&c[0]), &author).await;
    h.likes.set(&c[2], 9);
    h.likes.set(&c[0], 2);
    h.likes.set(&c[4], 2);

    let expected = vec![c[2].clone(), c[4].clone(), c[0].clone(), c[3].clone(), c[1].clone()];
    let (first, next) = h.top_level_page(&post, CommentSort::Popularity, 2, None, Viewer::Internal).await;
    assert_eq!(first.iter().map(|x| x.comment_id.as_str()).collect::<Vec<_>>(), expected[..2]);
    assert!(next.is_some());
    assert_eq!(h.all_top_level(&post, CommentSort::Popularity, 2, Viewer::Internal).await, expected);
    // Reply counts filled in this order too.
    let (page, _) = h.top_level_page(&post, CommentSort::Popularity, 5, None, Viewer::Internal).await;
    let oldest = page.iter().find(|x| x.comment_id.as_str() == c[0]).unwrap();
    assert_eq!(oldest.reply_count, 1);

    // NEWEST stays the default order.
    let newest: Vec<String> = c.iter().rev().cloned().collect();
    assert_eq!(h.all_top_level(&post, CommentSort::Newest, 1, Viewer::Internal).await, newest);

    // Engagement down: popularity pages come back newest first.
    h.likes.set_down(true);
    assert_eq!(h.all_top_level(&post, CommentSort::Popularity, 2, Viewer::Internal).await, newest);
}

#[tokio::test]
async fn the_read_gate_applies_in_the_popularity_order() {
    let h = TestHarness::start().await;
    let post = harness::random_post();
    let (alice, mallory) = (harness::random_author(), harness::random_author());
    let from_alice = h.create(&post, None, &alice).await;
    let from_mallory = h.create(&post, None, &mallory).await;
    h.likes.set(&from_mallory, 50);
    h.gate.hide_author(&mallory);

    let seen = h.all_top_level(&post, CommentSort::Popularity, 10, reader()).await;
    assert_eq!(seen, vec![from_alice.clone()], "the most liked, hidden author's comment never ranks for the reader");
    h.gate.post_unreadable(&post);
    assert!(h.all_top_level(&post, CommentSort::Popularity, 10, reader()).await.is_empty());
}
