//! The author's post history window (#664) against the live engine: posts older
//! than the window leave search without being deleted, so widening the window
//! brings them back; a stale window change is ignored.

use chrono::{Duration, Utc};

use crate::search_it::harness::Harness;

async fn found(h: &Harness, query: &str) -> Vec<String> {
    let mut ids = h.search_ids(query).await;
    ids.sort();
    ids
}

fn sorted(ids: &[&str]) -> Vec<String> {
    let mut ids: Vec<String> = ids.iter().map(|s| (*s).to_owned()).collect();
    ids.sort();
    ids
}

#[tokio::test]
async fn posts_outside_the_authors_window_leave_search_until_it_widens() {
    let h = Harness::start().await;
    let year_ago = Utc::now() - Duration::days(365);
    let yesterday = Utc::now() - Duration::days(1);
    h.index_post_at("old", "alice", "sunset at sea", year_ago, None).await;
    h.index_post_at("recent", "alice", "sunset again", yesterday, None).await;
    h.index_post_at("other", "bob", "sunset too", year_ago, None).await;
    h.refresh().await;
    assert_eq!(found(&h, "sunset").await, sorted(&["old", "recent", "other"]));

    // One month: alice's year-old post leaves; bob's is not hers to hide.
    h.post_window("alice", Some(30), 1_000).await;
    h.refresh().await;
    assert_eq!(found(&h, "sunset").await, sorted(&["recent", "other"]));

    // A stale change (older event) does not bring it back.
    h.post_window("alice", None, 500).await;
    h.refresh().await;
    assert_eq!(found(&h, "sunset").await, sorted(&["recent", "other"]));

    // All posts again: nothing was deleted.
    h.post_window("alice", None, 2_000).await;
    h.refresh().await;
    assert_eq!(found(&h, "sunset").await, sorted(&["old", "recent", "other"]));
}

#[tokio::test]
async fn a_post_indexed_with_its_window_end_leaves_search_once_it_passes() {
    let h = Harness::start().await;
    let month_ago = Utc::now() - Duration::days(40);
    let yesterday = Utc::now() - Duration::days(1);
    // As hydrated from post's mesh view under a 30-day window.
    h.index_post_at("past", "carol", "harbour lights", month_ago, Some(month_ago + Duration::days(30))).await;
    h.index_post_at("within", "carol", "harbour view", yesterday, Some(yesterday + Duration::days(30))).await;
    h.refresh().await;
    assert_eq!(found(&h, "harbour").await, sorted(&["within"]));
}
