//! #663 over real OpenSearch: a profile's recent searches — newest first, an
//! identical search (whatever its case) moves up rather than repeats, one is
//! deleted, all are cleared; at most 50, none older than 90 days.

use chrono::{Duration, Utc};
use tonic::Request;

use search::application::port::RecentSearches;
use search::infrastructure::grpc::handler::proto;

use crate::search_it::harness::Harness;

fn queries(r: &proto::RecentSearchesResponse) -> Vec<String> {
    r.searches.iter().map(|s| s.query.clone()).collect()
}

#[tokio::test]
async fn recent_searches_are_kept_newest_first_deduplicated_deletable_and_clearable() {
    let h = Harness::start().await;
    let me = uuid::Uuid::now_v7().to_string();
    let record = |query: &str| {
        let (h, me, query) = (&h, me.clone(), query.to_owned());
        async move {
            h.handler
                .record_recent_search(Request::new(proto::RecordRecentSearchRequest { profile_id: me, query }))
                .await
                .expect("record")
                .into_inner()
        }
    };

    record("paris").await;
    record("  Tokyo   ramen ").await;
    let listed = record("PARIS").await;
    assert_eq!(queries(&listed), vec!["PARIS", "Tokyo ramen"], "newest first, normalized, deduplicated");

    let after = h
        .handler
        .delete_recent_search(Request::new(proto::DeleteRecentSearchRequest {
            profile_id: me.clone(),
            query: "tokyo RAMEN".into(),
        }))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(queries(&after), vec!["PARIS"], "deleted, whatever its case");

    // At most 50 kept.
    for i in 0..55 {
        record(&format!("q{i}")).await;
    }
    let all = h
        .handler
        .list_recent_searches(Request::new(proto::ListRecentSearchesRequest { profile_id: me.clone() }))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(all.searches.len(), 50);
    assert_eq!(all.searches[0].query, "q54");

    let cleared = h
        .handler
        .clear_search_history(Request::new(proto::ClearSearchHistoryRequest { profile_id: me.clone() }))
        .await
        .unwrap()
        .into_inner();
    assert!(cleared.searches.is_empty());

    // Nothing older than 90 days is listed.
    let store: &dyn RecentSearches = h.recent();
    let now = Utc::now();
    store.record(&me, "long ago", now - Duration::days(91)).await.unwrap();
    store.record(&me, "lately", now).await.unwrap();
    let kept: Vec<String> = store.list(&me, now).await.unwrap().into_iter().map(|s| s.query).collect();
    assert_eq!(kept, vec!["lately"]);
    // Another profile's list is its own.
    assert!(store.list(&uuid::Uuid::now_v7().to_string(), now).await.unwrap().is_empty());
}

/// GDPR Art. 17: a deleted profile's recent searches go with it, as do a
/// purged author's — the document itself, not just what a read shows.
#[tokio::test]
async fn erasure_takes_the_recent_searches_with_it() {
    let h = Harness::start().await;
    let now = Utc::now();
    let (deleted, purged) = (uuid::Uuid::now_v7().to_string(), uuid::Uuid::now_v7().to_string());
    for profile in [&deleted, &purged] {
        h.recent().record(profile, "something personal", now).await.unwrap();
        assert_eq!(h.recent().list(profile, now).await.unwrap().len(), 1);
    }
    h.delete_profile(&deleted).await;
    h.purge(&purged).await;
    for profile in [&deleted, &purged] {
        assert!(h.recent().list(profile, now).await.unwrap().is_empty(), "erased");
        assert!(!h.recent_document_exists(profile).await, "the document itself is gone");
    }
}
