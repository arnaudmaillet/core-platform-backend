//! #831 over a live OpenSearch: hashtag documents derived from the post
//! documents — post counts that follow deletions and takedowns, Search and
//! Suggest ranked by popularity, and trending hashtags by recent use.

use chrono::{Duration, Utc};
use uuid::Uuid;

use crate::search_it::harness::{Harness, HASHTAG};
use search::domain::EntityKind;

fn post_count(resp: &search::infrastructure::grpc::proto::SearchResponse, tag: &str) -> Option<i64> {
    resp.hits.iter().find(|h| h.id == tag).and_then(|h| match &h.display {
        Some(search::infrastructure::grpc::proto::search_hit::Display::Hashtag(t)) => Some(t.post_count),
        _ => None,
    })
}

#[tokio::test]
async fn hashtags_count_rank_and_trend_from_the_post_documents() {
    let h = Harness::start().await;
    let author = Uuid::now_v7().to_string();
    let now = Utc::now();
    let old = now - Duration::days(30);
    let id = || Uuid::now_v7().to_string();

    // "classic": five old posts. "rising": two today. "oneoff": one old post.
    let classic: Vec<String> = (0..5).map(|_| id()).collect();
    for post in &classic {
        h.index_post_tagged_at(post, &author, &["classic"], old).await;
    }
    for _ in 0..2 {
        h.index_post_tagged_at(&id(), &author, &["rising"], now - Duration::hours(1)).await;
    }
    h.index_post_tagged_at(&id(), &author, &["classone"], old).await;

    let report = h.refresh_hashtags().await;
    assert_eq!(report.written, 3);

    // Search [HASHTAG]: the tag with its post count.
    let found = h.search_opts("classic", vec![HASHTAG], vec![]).await;
    assert_eq!(post_count(&found, "classic"), Some(5));

    // Suggest: the more popular tag first.
    assert_eq!(h.suggest_tags("class").await[..2], ["classic".to_owned(), "classone".to_owned()]);

    // Trending: recent use first; the larger, older tag is not trending.
    let trending = h.trending(10).await;
    assert_eq!(trending, vec![("rising".to_owned(), 2)]);

    // A deletion through the real post decoder, and a takedown, lower the count.
    h.post_wire(&format!(
        r#"{{"type":"PostDeleted","post_id":"{}","profile_id":"{author}","deleted_at_ms":1}}"#,
        classic[0]
    ))
    .await;
    h.hide(EntityKind::Post, &classic[1], Utc::now().timestamp_millis()).await;
    h.refresh_hashtags().await;
    let found = h.search_opts("classic", vec![HASHTAG], vec![]).await;
    assert_eq!(post_count(&found, "classic"), Some(3));

    // A tag whose posts are all gone loses its document.
    for post in &classic[2..] {
        h.hide(EntityKind::Post, post, Utc::now().timestamp_millis()).await;
    }
    let report = h.refresh_hashtags().await;
    assert_eq!(report.removed, 1);
    assert_eq!(post_count(&h.search_opts("classic", vec![HASHTAG], vec![]).await, "classic"), None);
}
