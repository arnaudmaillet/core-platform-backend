//! Scenario — mutes (#659): the following feed leaves out the posts of authors
//! the reader muted, whether they arrive by fan-out (Standard) or by the
//! read-time VIP merge, on both the cold and the warm path.

use crate::timeline_it::harness::{self, HarnessOptions, TestHarness};

#[tokio::test]
async fn muted_authors_leave_the_following_feed() {
    let h = TestHarness::start(HarnessOptions::default()).await;

    let reader = harness::random_profile();
    let (kept, muted, muted_vip) = (harness::random_author(), harness::random_author(), harness::random_author());
    for author in [kept, muted, muted_vip] {
        h.social_graph.add_follow(reader, author);
    }
    h.social_graph.mute(muted);
    h.social_graph.mute(muted_vip);

    h.ingest_post(&kept, harness::TIER_STANDARD, 3_000).await;
    h.ingest_post(&muted, harness::TIER_STANDARD, 2_000).await;
    h.ingest_post(&muted_vip, harness::TIER_VIP, 1_000).await;

    // Twice: the first read may be served cold, the second warm.
    for _ in 0..2 {
        let page = h.get_following_feed(&reader).await;
        let times: Vec<i64> = page.items.iter().map(|e| e.published_at_ms).collect();
        assert_eq!(times, vec![3_000], "only the unmuted author's post");
        assert!(page.items.iter().all(|e| e.author_id == kept));
    }
}
