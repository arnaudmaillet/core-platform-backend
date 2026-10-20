//! The query path against the live engine: typo-tolerant federated match across
//! kinds, entity-type filtering, block-author exclusion, and deep GDPR purge.

use crate::search_it::harness::{HASHTAG, Harness, POST, PROFILE, ids};

#[tokio::test]
async fn federated_typo_tolerant_match_and_kind_filter() {
    let h = Harness::start().await;
    h.index_profile("prof-1", "alice", "Alice A.", "distributed systems nerd", 1)
        .await;
    h.index_post("post-1", "acct-9", "scaling distributed systems", 1)
        .await;
    h.index_hashtag("distributed", 42, 1).await;
    h.refresh().await;

    // Typo tolerance: "systen" fuzzily matches "systems" (fuzziness AUTO).
    let typo = ids(&h.search("systen").await);
    assert!(
        typo.contains(&"post-1".to_owned()) && typo.contains(&"prof-1".to_owned()),
        "a one-edit typo should still match across kinds, got {typo:?}"
    );

    // Federated: all three kinds match "distributed".
    let federated = ids(&h.search("distributed").await);
    assert_eq!(
        federated,
        vec!["distributed".to_owned(), "post-1".to_owned(), "prof-1".to_owned()]
    );

    // Entity-type filter narrows to one index.
    let only_hashtags = ids(&h.search_opts("distributed", vec![HASHTAG], vec![]).await);
    assert_eq!(only_hashtags, vec!["distributed".to_owned()]);

    let only_people = ids(&h.search_opts("distributed", vec![PROFILE, POST], vec![]).await);
    assert_eq!(only_people, vec!["post-1".to_owned(), "prof-1".to_owned()]);

    // #913: a profile hit carries the avatar frame it was indexed with.
    let profiles = h.search_opts("distributed", vec![PROFILE], vec![]).await;
    let frame = profiles.hits.iter().find_map(|hit| match &hit.display {
        Some(search::infrastructure::grpc::proto::search_hit::Display::Profile(p)) => Some(p.avatar_frame_id.clone()),
        _ => None,
    });
    assert_eq!(frame.as_deref(), Some("frame.gold"));
}

#[tokio::test]
async fn excludes_blocked_authors() {
    let h = Harness::start().await;
    h.index_post("p-blocked", "blocked-acct", "shared keyword here", 1)
        .await;
    h.index_post("p-ok", "ok-acct", "shared keyword here", 1).await;
    h.refresh().await;

    let all = ids(&h.search("keyword").await);
    assert_eq!(all, vec!["p-blocked".to_owned(), "p-ok".to_owned()]);

    let filtered = ids(&h.search_opts("keyword", vec![], vec!["blocked-acct"]).await);
    assert_eq!(
        filtered,
        vec!["p-ok".to_owned()],
        "the blocked author's post is excluded at query time"
    );
}

#[tokio::test]
async fn gdpr_purge_removes_all_of_an_authors_documents() {
    let h = Harness::start().await;
    h.index_post("a-1", "acct-A", "widget review", 1).await;
    h.index_post("a-2", "acct-A", "widget unboxing", 1).await;
    h.index_post("b-1", "acct-B", "widget teardown", 1).await;
    assert_eq!(
        h.search_ids("widget").await,
        vec!["a-1".to_owned(), "a-2".to_owned(), "b-1".to_owned()]
    );

    h.purge("acct-A").await;
    assert_eq!(
        h.search_ids("widget").await,
        vec!["b-1".to_owned()],
        "every document authored by the purged actor is gone; others remain"
    );
}

/// #949 against the live engine: the kind filter excludes the other kind only;
/// a post indexed before its kind was kept, and a profile, still match.
#[tokio::test]
async fn a_post_kind_filter_keeps_its_kind_the_unknown_and_other_entities() {
    use search::domain::value_object::PostFormat;
    use search::infrastructure::grpc::proto::PostKindFilter;

    let h = Harness::start().await;
    h.index_post_kind("kind-media", "lighthouse at dawn", Some(PostFormat::Media)).await;
    h.index_post_kind("kind-text", "lighthouse thoughts", Some(PostFormat::TextOnly)).await;
    h.index_post_kind("kind-legacy", "lighthouse legacy", None).await;
    h.index_profile("prof-lighthouse", "lighthouse", "Lighthouse Keeper", "keeps the lighthouse", 1).await;
    h.refresh().await;

    let mut media = ids(&h.search_kind("lighthouse", vec![], vec![], PostKindFilter::Media).await);
    media.sort();
    assert_eq!(media, vec!["kind-legacy", "kind-media", "prof-lighthouse"]);
    let mut text = ids(&h.search_kind("lighthouse", vec![POST], vec![], PostKindFilter::TextOnly).await);
    text.sort();
    assert_eq!(text, vec!["kind-legacy", "kind-text"]);
    assert_eq!(ids(&h.search_kind("lighthouse", vec![POST], vec![], PostKindFilter::Unspecified).await).len(), 3);
}
