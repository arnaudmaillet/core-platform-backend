//! Scenario — the discovery pool against a live Redis: the three indices, the
//! moderation and deletion paths, and the read path's filters and paging.
//!
//! One test, because the pool is a single shared set of keys (`{disc}`): the
//! assertions only look at the posts this test created.

use std::collections::HashSet;
use std::time::{SystemTime, UNIX_EPOCH};

use cqrs::{CommandBus, Envelope, QueryBus};
use uuid::Uuid;

use timeline::application::command::apply_discovery_signal::{ApplyDiscoverySignalCommand, DiscoverySignal};
use timeline::application::query::get_discovery_feed::{DiscoveryPage, GetDiscoveryFeedQuery};
use timeline::domain::value_object::{ContentLevel, DiscoveryRanking, PostId, Restriction, Viewer};

use crate::timeline_it::harness::{self, HarnessOptions, TestHarness};

fn now_ms() -> i64 {
    SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_millis() as i64
}

async fn signal(h: &TestHarness, signal: DiscoverySignal) {
    h.command_bus
        .dispatch(Envelope::new(Uuid::now_v7(), ApplyDiscoverySignalCommand { signal }))
        .await
        .expect("discovery signal");
}

async fn publish(h: &TestHarness, author: &harness::AuthorId, published_at_ms: i64) -> String {
    let post_id = Uuid::now_v7().to_string();
    signal(h, DiscoverySignal::Published {
        post_id:   post_id.clone(),
        author_id: author.as_uuid().to_string(),
        published_at_ms,
        tags: Vec::new(),
        format: Default::default(),
    })
    .await;
    post_id
}

async fn page(
    h: &TestHarness,
    ranking: DiscoveryRanking,
    content_level: ContentLevel,
    page_token: Option<String>,
) -> DiscoveryPage {
    tagged_page(h, ranking, content_level, page_token, None, 50).await
}

/// A page narrowed to `hashtag` (#830).
async fn tagged_page(
    h: &TestHarness,
    ranking: DiscoveryRanking,
    content_level: ContentLevel,
    page_token: Option<String>,
    hashtag: Option<&str>,
    limit: i32,
) -> DiscoveryPage {
    let query = GetDiscoveryFeedQuery {
        ranking,
        viewer: Viewer::Profiles(vec![]),
        guest: None,
        content_level,
        lat: Some(48.85),
        lng: Some(2.35),
        limit,
        page_token,
        personalize_for: None,
        hashtag: hashtag.map(str::to_owned),
        post_kind: Default::default(),
    };
    h.query_bus.dispatch(Envelope::new(Uuid::now_v7(), query)).await.expect("discovery page")
}

/// Every post of `mine` the ranking shows, all pages, in order.
async fn all_of(h: &TestHarness, ranking: DiscoveryRanking, level: ContentLevel, mine: &HashSet<String>) -> Vec<String> {
    let mut token = None;
    let mut out = Vec::new();
    for _ in 0..1_000 {
        let p = page(h, ranking, level, token).await;
        out.extend(p.items.iter().map(|e| e.post_id.to_string()).filter(|id| mine.contains(id)));
        token = p.next_page_token;
        if token.is_none() {
            break;
        }
    }
    out
}

#[tokio::test]
async fn the_discovery_pool_ranks_filters_and_pages() {
    let h = TestHarness::start(HarnessOptions::default()).await;
    let (author, private) = (harness::random_author(), harness::random_author());
    h.social_graph.make_private(private);
    let t = now_ms() - 60_000;

    let old = publish(&h, &author, t).await;
    let popular = publish(&h, &author, t + 1_000).await;
    let gated = publish(&h, &author, t + 2_000).await;
    let removed = publish(&h, &author, t + 3_000).await;
    let deleted = publish(&h, &author, t + 4_000).await;
    let hidden = publish(&h, &private, t + 5_000).await;
    let newest = publish(&h, &author, t + 6_000).await;
    // Out of the window: never pooled.
    let ancient = publish(&h, &author, t - 4 * 86_400_000).await;
    // A takedown that arrives before the publication (a tombstone).
    let early = Uuid::now_v7().to_string();
    signal(&h, DiscoverySignal::Restricted { post_id: early.clone(), restriction: Restriction::Removed, version: 1 }).await;
    signal(&h, DiscoverySignal::Published {
        post_id: early.clone(), author_id: author.as_uuid().to_string(), published_at_ms: t + 7_000, tags: Vec::new(),
        format: Default::default(),
    })
    .await;
    let mine: HashSet<String> =
        [&old, &popular, &gated, &removed, &deleted, &hidden, &newest, &ancient, &early].into_iter().cloned().collect();

    signal(&h, DiscoverySignal::Popularity { post_id: popular.clone(), score: 1_000.0 }).await;
    signal(&h, DiscoverySignal::Popularity { post_id: old.clone(), score: 2.0 }).await;
    signal(&h, DiscoverySignal::Restricted { post_id: gated.clone(), restriction: Restriction::AgeGated, version: 1 }).await;
    signal(&h, DiscoverySignal::Restricted { post_id: removed.clone(), restriction: Restriction::Removed, version: 2 }).await;
    // A stale reversal (older version) changes nothing.
    signal(&h, DiscoverySignal::Restricted { post_id: removed.clone(), restriction: Restriction::None, version: 1 }).await;
    signal(&h, DiscoverySignal::Deleted { post_id: deleted.clone() }).await;

    let r = ContentLevel::Restricted;
    assert_eq!(all_of(&h, DiscoveryRanking::Recent, r, &mine).await, vec![newest.clone(), popular.clone(), old.clone()]);
    assert_eq!(
        all_of(&h, DiscoveryRanking::Recent, ContentLevel::Standard, &mine).await,
        vec![newest.clone(), gated.clone(), popular.clone(), old.clone()]
    );
    // Hot: only posts with popularity, the more popular first.
    assert_eq!(all_of(&h, DiscoveryRanking::Trending, r, &mine).await, vec![popular.clone(), old.clone()]);
    // For You: hot and fresh together, each once.
    let for_you = all_of(&h, DiscoveryRanking::ForYou, r, &mine).await;
    assert_eq!(for_you.iter().collect::<HashSet<_>>(), [&popular, &old, &newest].into_iter().collect::<HashSet<_>>());
    assert_eq!(for_you.len(), 3);

    // A newer reversal brings the removed post back.
    signal(&h, DiscoverySignal::Restricted { post_id: removed.clone(), restriction: Restriction::None, version: 3 }).await;
    assert_eq!(
        all_of(&h, DiscoveryRanking::Recent, r, &mine).await,
        vec![newest.clone(), removed.clone(), popular.clone(), old.clone()]
    );

    // Nearby: geo's candidates, ranked by hot score, same filters.
    *h.nearby.posts.lock().unwrap() = [&old, &popular, &gated, &deleted, &hidden]
        .into_iter()
        .map(|id| PostId::try_from(id.as_str()).unwrap())
        .collect();
    assert_eq!(all_of(&h, DiscoveryRanking::Nearby, r, &mine).await, vec![popular, old]);
}

/// #830 over live Redis: a hashtag's posts, trending (the time-decayed hot
/// score: the same likes rank a newer post first) or recent, with the
/// discovery filters, paged to the end.
#[tokio::test]
async fn a_hashtag_narrows_trending_and_recent() {
    let h = TestHarness::start(HarnessOptions::default()).await;
    let (author, private) = (harness::random_author(), harness::random_author());
    h.social_graph.make_private(private);
    let tag = format!("t{}", Uuid::now_v7().simple());
    let t = now_ms() - 60_000;
    let publish_tagged = |author: harness::AuthorId, at: i64, tags: Vec<String>| {
        let h = &h;
        async move {
            let post_id = Uuid::now_v7().to_string();
            signal(h, DiscoverySignal::Published {
                post_id: post_id.clone(),
                author_id: author.as_uuid().to_string(),
                published_at_ms: at,
                tags,
                format: Default::default(),
            })
            .await;
            post_id
        }
    };
    let older = publish_tagged(author, t, vec![tag.clone()]).await;
    let newer = publish_tagged(author, t + 1_000, vec!["other".to_owned(), tag.clone()]).await;
    let untagged = publish_tagged(author, t + 2_000, vec!["other".to_owned()]).await;
    let deleted = publish_tagged(author, t + 3_000, vec![tag.clone()]).await;
    let hidden = publish_tagged(private, t + 4_000, vec![tag.clone()]).await;
    let quiet = publish_tagged(author, t + 5_000, vec![tag.clone()]).await;
    // The same likes on both: the newer ranks first. The untagged one, far
    // more popular, is not in the tag's feed.
    for post in [&older, &newer] {
        signal(&h, DiscoverySignal::Popularity { post_id: post.clone(), score: 50.0 }).await;
    }
    signal(&h, DiscoverySignal::Popularity { post_id: untagged.clone(), score: 5_000.0 }).await;
    signal(&h, DiscoverySignal::Popularity { post_id: deleted.clone(), score: 900.0 }).await;
    signal(&h, DiscoverySignal::Popularity { post_id: hidden.clone(), score: 900.0 }).await;
    signal(&h, DiscoverySignal::Deleted { post_id: deleted.clone() }).await;

    let all_tagged = |ranking| {
        let (h, tag) = (&h, tag.clone());
        async move {
            let (mut token, mut out) = (None, Vec::new());
            loop {
                let p = tagged_page(h, ranking, ContentLevel::Restricted, token, Some(&tag), 1).await;
                out.extend(p.items.iter().map(|e| e.post_id.to_string()));
                match p.next_page_token {
                    Some(next) => token = Some(next),
                    None => return out,
                }
            }
        }
    };
    // Trending: only posts with likes (the hot stream), the tag's only.
    assert_eq!(all_tagged(DiscoveryRanking::Trending).await, vec![newer.clone(), older.clone()]);
    // Recent: every visible post of the tag, newest first.
    assert_eq!(all_tagged(DiscoveryRanking::Recent).await, vec![quiet, newer, older]);
}

/// #949 over the live pool: the kind is kept in the post's meta and a kind
/// filter keeps that kind and the posts of unknown kind.
#[tokio::test]
async fn a_kind_filter_keeps_its_kind_and_the_unknown() {
    use timeline::domain::value_object::{PostFormat, PostKindFilter};

    let h = TestHarness::start(HarnessOptions::default()).await;
    let author = harness::random_author();
    let t = now_ms() - 30_000;
    let publish_kind = |at: i64, format: PostFormat| {
        let (h, author) = (&h, &author);
        async move {
            let post_id = Uuid::now_v7().to_string();
            signal(h, DiscoverySignal::Published {
                post_id: post_id.clone(),
                author_id: author.as_uuid().to_string(),
                published_at_ms: at,
                tags: Vec::new(),
                format,
            })
            .await;
            post_id
        }
    };
    let media = publish_kind(t, PostFormat::Media).await;
    let text = publish_kind(t + 1, PostFormat::TextOnly).await;
    let unknown = publish_kind(t + 2, PostFormat::Unknown).await;
    let mine: HashSet<String> = [&media, &text, &unknown].into_iter().cloned().collect();

    let seen = |post_kind: PostKindFilter| {
        let (h, mine) = (&h, &mine);
        async move {
            let mut seen = HashSet::new();
            let mut token = None;
            loop {
                let query = GetDiscoveryFeedQuery {
                    ranking: DiscoveryRanking::Recent,
                    viewer: Viewer::Profiles(vec![]),
                    guest: None,
                    content_level: ContentLevel::Restricted,
                    lat: None,
                    lng: None,
                    limit: 50,
                    page_token: token,
                    personalize_for: None,
                    hashtag: None,
                    post_kind,
                };
                let page: DiscoveryPage = h.query_bus.dispatch(Envelope::new(Uuid::now_v7(), query)).await.expect("page");
                seen.extend(page.items.iter().map(|e| e.post_id.to_string()).filter(|id| mine.contains(id)));
                match page.next_page_token {
                    Some(next) if seen.len() < mine.len() => token = Some(next),
                    _ => return seen,
                }
            }
        }
    };
    let set = |ids: &[&String]| ids.iter().map(|id| (*id).clone()).collect::<HashSet<_>>();
    assert_eq!(seen(PostKindFilter::Any).await, mine);
    assert_eq!(seen(PostKindFilter::Media).await, set(&[&media, &unknown]));
    assert_eq!(seen(PostKindFilter::TextOnly).await, set(&[&text, &unknown]));
}
