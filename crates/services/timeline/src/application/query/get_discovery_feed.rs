use std::collections::{HashMap, HashSet, VecDeque};
use std::sync::Arc;

use cqrs::{Envelope, Query, QueryHandler};
use validate_core::{FieldViolation, Validate};

use crate::application::port::{muted_authors, visible_authors, DiscoveryPool, NearbyPosts, PoolEntry, SocialGraphClient};
use crate::domain::aggregate::FeedEntry;
use crate::domain::value_object::{
    hot_score, ContentLevel, DiscoveryCursor, DiscoveryRanking, DiscoveryStream, PostId,
    StreamPosition, Viewer, TRENDING_PER_FRESH,
};
use crate::error::TimelineError;

/// Page size when the request asks for none.
const DEFAULT_PAGE_SIZE: usize = 20;

/// Pool reads per stream and per request. A page whose candidates are mostly
/// filtered out (moderated posts, private authors) comes back short, with a
/// cursor past what was scanned, rather than scanning without bound.
const MAX_REFILLS: usize = 4;

/// A page of the discovery feed. `ranking`, `viewer` and `content_level` are
/// resolved by the caller (the guest rule lives at the edge).
pub struct GetDiscoveryFeedQuery {
    pub ranking:       DiscoveryRanking,
    pub viewer:        Viewer,
    /// The guest principal (token `sub`) when the reader is a guest session:
    /// NEARBY then only reaches the country granted to it (geo-discovery).
    pub guest:         Option<String>,
    pub content_level: ContentLevel,
    pub lat:           Option<f64>,
    pub lng:           Option<f64>,
    pub limit:         i32,
    pub page_token:    Option<String>,
}

#[derive(Debug)]
pub struct DiscoveryPage {
    pub items:           Vec<FeedEntry>,
    pub next_page_token: Option<String>,
}

impl Query for GetDiscoveryFeedQuery {
    type Response = DiscoveryPage;
}

impl Validate for GetDiscoveryFeedQuery {
    fn validate(&self) -> Result<(), Vec<FieldViolation>> {
        if self.ranking != DiscoveryRanking::Nearby {
            return Ok(());
        }
        let mut v = Vec::new();
        if !self.lat.is_some_and(|lat| lat.is_finite() && (-90.0..=90.0).contains(&lat)) {
            v.push(FieldViolation::new("lat", "TML-VAL-030", "NEARBY needs lat in [-90, 90]"));
        }
        if !self.lng.is_some_and(|lng| lng.is_finite() && (-180.0..=180.0).contains(&lng)) {
            v.push(FieldViolation::new("lng", "TML-VAL-031", "NEARBY needs lng in [-180, 180]"));
        }
        if v.is_empty() { Ok(()) } else { Err(v) }
    }
}

pub struct GetDiscoveryFeedHandler<SG> {
    pub pool:              Arc<dyn DiscoveryPool>,
    pub social_graph:      Arc<SG>,
    pub nearby:            Arc<dyn NearbyPosts>,
    pub max_page_size:     i32,
    /// At most this many NEARBY candidates are ranked per request.
    pub nearby_candidates: usize,
    pub hot_gravity_secs:  f64,
}

/// What a page shows: the reader's level, and the authors it may see (`None` =
/// all, the mesh) minus those it muted. Audience answers are cached for the
/// request.
struct Filter<'a, SG: ?Sized> {
    social_graph: &'a SG,
    viewer:       &'a Viewer,
    level:        ContentLevel,
    visible:      HashMap<crate::domain::value_object::AuthorId, bool>,
    muted:        HashSet<crate::domain::value_object::AuthorId>,
}

impl<SG: SocialGraphClient + ?Sized> Filter<'_, SG> {
    /// Keeps, per entry, the feed item it may show (or `None`), in order.
    async fn apply(
        &mut self,
        pool:    &dyn DiscoveryPool,
        entries: Vec<PoolEntry>,
    ) -> Result<Vec<(StreamPosition, Option<FeedEntry>)>, TimelineError> {
        let ids: Vec<PostId> = entries.iter().map(|e| e.post_id).collect();
        let meta = pool.meta(&ids).await?;
        let muted = &self.muted;
        let shown = |e: &PoolEntry| {
            meta.get(&e.post_id)
                .filter(|m| m.shown_at(self.level) && m.author_id == Some(e.author_id))
                .filter(|_| !muted.contains(&e.author_id))
        };
        let unknown: Vec<_> = entries
            .iter()
            .filter(|e| shown(e).is_some() && !self.visible.contains_key(&e.author_id))
            .map(|e| e.author_id)
            .collect();
        if !unknown.is_empty() {
            let allowed = visible_authors(self.social_graph, self.viewer, unknown.iter().copied()).await?;
            for author in unknown {
                self.visible.insert(author, allowed.as_ref().is_none_or(|set| set.contains(&author)));
            }
        }
        Ok(entries
            .iter()
            .map(|e| {
                let item = shown(e)
                    .filter(|_| self.visible.get(&e.author_id).copied().unwrap_or(false))
                    .and_then(|m| m.published_at_ms)
                    .map(|t| FeedEntry::new(e.post_id, e.author_id, t));
                (e.position.clone(), item)
            })
            .collect())
    }
}

/// One stream being read for a page: what was fetched and not yet consumed,
/// the position of the last entry fetched (where the next read starts) and of
/// the last one consumed (where the next page starts).
struct StreamReader {
    stream:   DiscoveryStream,
    fetched:  Option<StreamPosition>,
    consumed: Option<StreamPosition>,
    buffer:   VecDeque<(StreamPosition, Option<FeedEntry>)>,
    drained:  bool,
    refills:  usize,
}

impl StreamReader {
    fn new(stream: DiscoveryStream, cursor: &DiscoveryCursor) -> Self {
        let start = cursor.position(stream).cloned();
        Self { stream, fetched: start.clone(), consumed: start, buffer: VecDeque::new(), drained: false, refills: 0 }
    }

    /// Nothing left below the cursor, as of this read.
    fn exhausted(&self) -> bool {
        self.drained && self.buffer.is_empty()
    }
}

impl<SG: SocialGraphClient> GetDiscoveryFeedHandler<SG> {
    fn page_size(&self, limit: i32) -> usize {
        let max = self.max_page_size.max(1) as usize;
        if limit <= 0 { DEFAULT_PAGE_SIZE.min(max) } else { (limit as usize).min(max) }
    }

    /// The next item `reader` may show, reading more of its stream as needed.
    async fn next(
        &self,
        reader: &mut StreamReader,
        filter: &mut Filter<'_, SG>,
        batch:  usize,
    ) -> Result<Option<FeedEntry>, TimelineError> {
        loop {
            if let Some((position, item)) = reader.buffer.pop_front() {
                reader.consumed = Some(position);
                if item.is_some() {
                    return Ok(item);
                }
                continue;
            }
            if reader.drained || reader.refills >= MAX_REFILLS {
                return Ok(None);
            }
            reader.refills += 1;
            let entries = self.pool.range(reader.stream, reader.fetched.as_ref(), batch).await?;
            reader.drained = entries.len() < batch;
            if let Some(last) = entries.last() {
                reader.fetched = Some(last.position.clone());
            }
            reader.buffer.extend(filter.apply(self.pool.as_ref(), entries).await?);
        }
    }

    /// TRENDING, RECENT and FOR_YOU: read the pool's streams in the ranking's
    /// pattern (FOR_YOU: three hot, one fresh; one stream running dry hands
    /// over to the other).
    async fn pool_page(
        &self,
        ranking: DiscoveryRanking,
        cursor:  &DiscoveryCursor,
        filter:  &mut Filter<'_, SG>,
        size:    usize,
    ) -> Result<DiscoveryPage, TimelineError> {
        let pattern: &[DiscoveryStream] = match ranking {
            DiscoveryRanking::Trending => &[DiscoveryStream::Hot],
            DiscoveryRanking::Recent => &[DiscoveryStream::Recent],
            _ => &[DiscoveryStream::Hot, DiscoveryStream::Hot, DiscoveryStream::Hot, DiscoveryStream::Fresh],
        };
        debug_assert!(ranking != DiscoveryRanking::ForYou || pattern.len() == TRENDING_PER_FRESH + 1);
        let mut readers: Vec<StreamReader> = Vec::new();
        for stream in pattern {
            if !readers.iter().any(|r| r.stream == *stream) {
                readers.push(StreamReader::new(*stream, cursor));
            }
        }
        let batch = (size * 2).max(DEFAULT_PAGE_SIZE);

        let mut items = Vec::with_capacity(size);
        let mut seen = HashSet::new();
        let mut slot = 0;
        while items.len() < size {
            let preferred = readers.iter().position(|r| r.stream == pattern[slot % pattern.len()]).unwrap_or(0);
            slot += 1;
            let mut item = None;
            for offset in 0..readers.len() {
                let idx = (preferred + offset) % readers.len();
                if let Some(found) = self.next(&mut readers[idx], filter, batch).await? {
                    item = Some(found);
                    break;
                }
            }
            match item {
                Some(entry) => {
                    if seen.insert(entry.post_id) {
                        items.push(entry);
                    }
                }
                None => break, // every stream is dry or out of reads for this page
            }
        }

        let next_page_token = if readers.iter().all(StreamReader::exhausted) {
            None
        } else {
            let positions = readers
                .into_iter()
                .filter_map(|r| r.consumed.map(|p| (r.stream, p)))
                .collect();
            Some(DiscoveryCursor { positions }.encode(ranking))
        };
        Ok(DiscoveryPage { items, next_page_token })
    }

    /// NEARBY: geo-discovery's posts around the point, ranked by hot score from
    /// the pool's meta (a post the pool does not know is not shown).
    async fn nearby_page(
        &self,
        query:  &GetDiscoveryFeedQuery,
        cursor: &DiscoveryCursor,
        filter: &mut Filter<'_, SG>,
        size:   usize,
    ) -> Result<DiscoveryPage, TimelineError> {
        let (Some(lat), Some(lng)) = (query.lat, query.lng) else {
            return Err(TimelineError::LocationRequired);
        };
        let mut ids = self.nearby.around(lat, lng, query.guest.as_deref()).await?;
        ids.truncate(self.nearby_candidates);
        let meta = self.pool.meta(&ids).await?;
        let after = cursor.position(DiscoveryStream::Nearby);
        let mut ranked: Vec<PoolEntry> = meta
            .values()
            .filter(|m| m.shown_at(filter.level))
            .filter_map(|m| {
                let (author_id, published_at_ms) = (m.author_id?, m.published_at_ms?);
                let score = hot_score(m.popularity, published_at_ms, self.hot_gravity_secs);
                let member = format!("{}:{}", m.post_id, author_id);
                Some(PoolEntry { post_id: m.post_id, author_id, position: StreamPosition { score, member } })
            })
            .filter(|e| after.is_none_or(|p| p.is_before(e.position.score, &e.position.member)))
            .collect();
        ranked.sort_by(|a, b| {
            b.position.score.total_cmp(&a.position.score).then_with(|| b.position.member.cmp(&a.position.member))
        });

        let mut items = Vec::with_capacity(size);
        let mut last = after.cloned();
        let mut more = false;
        let mut rest = ranked.into_iter().peekable();
        while items.len() < size && rest.peek().is_some() {
            let chunk: Vec<PoolEntry> = rest.by_ref().take(size * 2).collect();
            let mut checked = filter.apply(self.pool.as_ref(), chunk).await?.into_iter();
            for (position, item) in checked.by_ref() {
                last = Some(position);
                items.extend(item);
                if items.len() == size {
                    break;
                }
            }
            more = checked.next().is_some();
        }
        more |= rest.peek().is_some();
        let next_page_token = last.filter(|_| more).map(|position| {
            DiscoveryCursor { positions: vec![(DiscoveryStream::Nearby, position)] }.encode(DiscoveryRanking::Nearby)
        });
        Ok(DiscoveryPage { items, next_page_token })
    }
}

impl<SG: SocialGraphClient> QueryHandler<GetDiscoveryFeedQuery> for GetDiscoveryFeedHandler<SG> {
    type Error = TimelineError;

    async fn handle(&self, envelope: Envelope<GetDiscoveryFeedQuery>) -> Result<DiscoveryPage, TimelineError> {
        let query = &envelope.payload;
        let cursor = match query.page_token.as_deref().filter(|t| !t.is_empty()) {
            Some(token) => DiscoveryCursor::decode(token, query.ranking)?,
            None => DiscoveryCursor::default(),
        };
        let size = self.page_size(query.limit);
        let muted = match &query.viewer {
            Viewer::Profiles(own) => muted_authors(self.social_graph.as_ref(), own).await,
            Viewer::Internal => HashSet::new(),
        };
        let mut filter = Filter {
            social_graph: self.social_graph.as_ref(),
            viewer:       &query.viewer,
            level:        query.content_level,
            visible:      HashMap::new(),
            muted,
        };
        match query.ranking {
            DiscoveryRanking::Nearby => self.nearby_page(query, &cursor, &mut filter, size).await,
            ranking => self.pool_page(ranking, &cursor, &mut filter, size).await,
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Mutex;

    use async_trait::async_trait;
    use uuid::Uuid;

    use super::*;
    use crate::domain::value_object::{AuthorId, ContentAccess, DiscoveryMeta, ProfileId, Restriction};

    /// An in-memory pool: one ordered map per stream, plus the meta.
    #[derive(Default)]
    struct MemPool {
        streams: Mutex<HashMap<DiscoveryStream, Vec<PoolEntry>>>,
        meta:    Mutex<HashMap<PostId, DiscoveryMeta>>,
    }

    impl MemPool {
        fn put(&self, stream: DiscoveryStream, author: AuthorId, score: f64, restriction: Restriction) -> PostId {
            let post_id = PostId::from_uuid(Uuid::now_v7());
            let member = format!("{post_id}:{author}");
            self.streams.lock().unwrap().entry(stream).or_default().push(PoolEntry {
                post_id,
                author_id: author,
                position: StreamPosition { score, member },
            });
            self.meta.lock().unwrap().insert(post_id, DiscoveryMeta {
                post_id,
                author_id: Some(author),
                published_at_ms: Some(1_760_000_000_000),
                popularity: score,
                restriction,
                deleted: false,
            });
            post_id
        }
    }

    #[async_trait]
    impl DiscoveryPool for MemPool {
        async fn record_published(&self, _: &PostId, _: &AuthorId, _: i64) -> Result<(), TimelineError> { Ok(()) }
        async fn record_popularity(&self, _: &PostId, _: f64) -> Result<(), TimelineError> { Ok(()) }
        async fn record_restriction(&self, _: &PostId, _: Restriction, _: i64) -> Result<(), TimelineError> { Ok(()) }
        async fn record_deleted(&self, _: &PostId) -> Result<(), TimelineError> { Ok(()) }

        async fn range(
            &self,
            stream: DiscoveryStream,
            after:  Option<&StreamPosition>,
            count:  usize,
        ) -> Result<Vec<PoolEntry>, TimelineError> {
            let mut all = self.streams.lock().unwrap().get(&stream).cloned().unwrap_or_default();
            all.sort_by(|a, b| {
                b.position.score.total_cmp(&a.position.score).then_with(|| b.position.member.cmp(&a.position.member))
            });
            Ok(all
                .into_iter()
                .filter(|e| after.is_none_or(|p| p.is_before(e.position.score, &e.position.member)))
                .take(count)
                .collect())
        }

        async fn meta(&self, posts: &[PostId]) -> Result<HashMap<PostId, DiscoveryMeta>, TimelineError> {
            let meta = self.meta.lock().unwrap();
            Ok(posts.iter().filter_map(|p| meta.get(p).map(|m| (*p, m.clone()))).collect())
        }
    }

    /// Every author is visible except those listed; `down` fails every check.
    #[derive(Default)]
    struct Graph {
        hidden: Mutex<HashSet<AuthorId>>,
        muted:  Mutex<HashSet<AuthorId>>,
        down:   bool,
        mutes_down: bool,
        calls:  Mutex<usize>,
    }

    #[async_trait]
    impl SocialGraphClient for Graph {
        async fn list_all_followers(&self, _: &AuthorId, _: i32) -> Result<Vec<ProfileId>, TimelineError> { Ok(vec![]) }
        async fn list_all_following(&self, _: &ProfileId, _: i32) -> Result<Vec<AuthorId>, TimelineError> { Ok(vec![]) }
        async fn check_access(&self, _: &[String], authors: &[AuthorId]) -> Result<HashMap<AuthorId, ContentAccess>, TimelineError> {
            *self.calls.lock().unwrap() += 1;
            if self.down {
                return Err(TimelineError::AccessCheckUnavailable { reason: "down".into() });
            }
            let hidden = self.hidden.lock().unwrap();
            Ok(authors
                .iter()
                .map(|a| (*a, if hidden.contains(a) { ContentAccess::HeaderOnly } else { ContentAccess::Visible }))
                .collect())
        }
        async fn muted_authors(&self, _: &[String]) -> Result<HashSet<AuthorId>, TimelineError> {
            if self.down || self.mutes_down {
                return Err(TimelineError::AccessCheckUnavailable { reason: "down".into() });
            }
            Ok(self.muted.lock().unwrap().clone())
        }
    }

    struct Around(Vec<PostId>);

    #[async_trait]
    impl NearbyPosts for Around {
        async fn around(&self, _: f64, _: f64, _: Option<&str>) -> Result<Vec<PostId>, TimelineError> { Ok(self.0.clone()) }
    }

    fn handler(pool: Arc<MemPool>, graph: Arc<Graph>, around: Vec<PostId>) -> GetDiscoveryFeedHandler<Graph> {
        GetDiscoveryFeedHandler {
            pool,
            social_graph: graph,
            nearby: Arc::new(Around(around)),
            max_page_size: 50,
            nearby_candidates: 300,
            hot_gravity_secs: 45_000.0,
        }
    }

    fn query(ranking: DiscoveryRanking, limit: i32, page_token: Option<String>) -> Envelope<GetDiscoveryFeedQuery> {
        Envelope::new(Uuid::now_v7(), GetDiscoveryFeedQuery {
            ranking,
            viewer: Viewer::Profiles(vec![]),
            guest: None,
            content_level: ContentLevel::Restricted,
            lat: Some(48.85),
            lng: Some(2.35),
            limit,
            page_token,
        })
    }

    fn author() -> AuthorId {
        AuthorId::from_uuid(Uuid::now_v7())
    }

    #[tokio::test]
    async fn for_you_mixes_three_hot_for_one_fresh_and_pages_without_repeats() {
        let pool = Arc::new(MemPool::default());
        let a = author();
        let hot: Vec<PostId> = (0..6).map(|i| pool.put(DiscoveryStream::Hot, a, 100.0 - i as f64, Restriction::None)).collect();
        let fresh: Vec<PostId> = (0..3).map(|i| pool.put(DiscoveryStream::Fresh, a, 50.0 - i as f64, Restriction::None)).collect();
        let h = handler(Arc::clone(&pool), Arc::new(Graph::default()), vec![]);

        let first = h.handle(query(DiscoveryRanking::ForYou, 4, None)).await.unwrap();
        let ids: Vec<PostId> = first.items.iter().map(|e| e.post_id).collect();
        assert_eq!(ids, vec![hot[0], hot[1], hot[2], fresh[0]]);

        let second = h.handle(query(DiscoveryRanking::ForYou, 4, first.next_page_token.clone())).await.unwrap();
        let ids: Vec<PostId> = second.items.iter().map(|e| e.post_id).collect();
        assert_eq!(ids, vec![hot[3], hot[4], hot[5], fresh[1]]);

        // Hot runs dry: fresh fills the page; then the pool is exhausted.
        let third = h.handle(query(DiscoveryRanking::ForYou, 4, second.next_page_token)).await.unwrap();
        assert_eq!(third.items.iter().map(|e| e.post_id).collect::<Vec<_>>(), vec![fresh[2]]);
        assert_eq!(third.next_page_token, None);

        // A token is bound to its ranking.
        let err = h.handle(query(DiscoveryRanking::Recent, 4, first.next_page_token)).await.unwrap_err();
        assert!(matches!(err, TimelineError::InvalidPageToken { .. }));
    }

    #[tokio::test]
    async fn moderated_age_gated_and_private_posts_are_left_out() {
        let pool = Arc::new(MemPool::default());
        let (open, private) = (author(), author());
        let shown = pool.put(DiscoveryStream::Recent, open, 9.0, Restriction::None);
        pool.put(DiscoveryStream::Recent, open, 8.0, Restriction::Limited);
        let gated = pool.put(DiscoveryStream::Recent, open, 7.0, Restriction::AgeGated);
        pool.put(DiscoveryStream::Recent, private, 6.0, Restriction::None);
        let graph = Arc::new(Graph::default());
        graph.hidden.lock().unwrap().insert(private);
        let h = handler(Arc::clone(&pool), Arc::clone(&graph), vec![]);

        let page = h.handle(query(DiscoveryRanking::Recent, 10, None)).await.unwrap();
        assert_eq!(page.items.iter().map(|e| e.post_id).collect::<Vec<_>>(), vec![shown]);
        assert_eq!(page.next_page_token, None);

        let mut standard = query(DiscoveryRanking::Recent, 10, None);
        standard.payload.content_level = ContentLevel::Standard;
        let page = h.handle(standard).await.unwrap();
        assert_eq!(page.items.iter().map(|e| e.post_id).collect::<Vec<_>>(), vec![shown, gated]);

        // The mesh is unfiltered by audience (moderation still applies).
        let mut internal = query(DiscoveryRanking::Recent, 10, None);
        internal.payload.viewer = Viewer::Internal;
        let calls = *graph.calls.lock().unwrap();
        assert_eq!(h.handle(internal).await.unwrap().items.len(), 2);
        assert_eq!(*graph.calls.lock().unwrap(), calls);
    }

    #[tokio::test]
    async fn muted_authors_are_left_out_and_a_mute_outage_fails_open() {
        let pool = Arc::new(MemPool::default());
        let (kept, muted) = (author(), author());
        let shown = pool.put(DiscoveryStream::Recent, kept, 9.0, Restriction::None);
        let skipped = pool.put(DiscoveryStream::Recent, muted, 8.0, Restriction::None);
        let member = || {
            let mut q = query(DiscoveryRanking::Recent, 10, None);
            q.payload.viewer = Viewer::Profiles(vec![Uuid::now_v7().to_string()]);
            q
        };
        let ids = |page: DiscoveryPage| page.items.iter().map(|e| e.post_id).collect::<Vec<_>>();

        let graph = Arc::new(Graph::default());
        graph.muted.lock().unwrap().insert(muted);
        let h = handler(Arc::clone(&pool), graph, vec![]);
        assert_eq!(ids(h.handle(member()).await.unwrap()), vec![shown]);

        let down = Arc::new(Graph { mutes_down: true, ..Graph::default() });
        down.muted.lock().unwrap().insert(muted);
        let h = handler(pool, down, vec![]);
        assert_eq!(ids(h.handle(member()).await.unwrap()), vec![shown, skipped], "fails open");
    }

    #[tokio::test]
    async fn the_audience_check_fails_closed() {
        let pool = Arc::new(MemPool::default());
        pool.put(DiscoveryStream::Hot, author(), 1.0, Restriction::None);
        let h = handler(pool, Arc::new(Graph { down: true, ..Graph::default() }), vec![]);
        let err = h.handle(query(DiscoveryRanking::Trending, 10, None)).await.unwrap_err();
        assert!(matches!(err, TimelineError::AccessCheckUnavailable { .. }));
    }

    #[tokio::test]
    async fn a_filtered_out_run_returns_a_short_page_with_a_cursor_past_it() {
        let pool = Arc::new(MemPool::default());
        let a = author();
        for i in 0..200 {
            pool.put(DiscoveryStream::Recent, a, 1_000.0 - i as f64, Restriction::Removed);
        }
        let last = pool.put(DiscoveryStream::Recent, a, 1.0, Restriction::None);
        let h = handler(Arc::clone(&pool), Arc::new(Graph::default()), vec![]);

        let mut token = None;
        let mut found = Vec::new();
        for _ in 0..10 {
            let page = h.handle(query(DiscoveryRanking::Recent, 5, token)).await.unwrap();
            found.extend(page.items.iter().map(|e| e.post_id));
            token = page.next_page_token;
            if token.is_none() {
                break;
            }
        }
        assert_eq!(found, vec![last]);
        assert_eq!(token, None);
    }

    #[tokio::test]
    async fn nearby_ranks_known_posts_by_hot_score() {
        let pool = Arc::new(MemPool::default());
        let a = author();
        let low = pool.put(DiscoveryStream::Recent, a, 1.0, Restriction::None);
        let high = pool.put(DiscoveryStream::Recent, a, 500.0, Restriction::None);
        let removed = pool.put(DiscoveryStream::Recent, a, 900.0, Restriction::Removed);
        let unknown = PostId::from_uuid(Uuid::now_v7());
        let h = handler(Arc::clone(&pool), Arc::new(Graph::default()), vec![low, unknown, removed, high]);

        let first = h.handle(query(DiscoveryRanking::Nearby, 1, None)).await.unwrap();
        assert_eq!(first.items.iter().map(|e| e.post_id).collect::<Vec<_>>(), vec![high]);
        let second = h.handle(query(DiscoveryRanking::Nearby, 1, first.next_page_token)).await.unwrap();
        assert_eq!(second.items.iter().map(|e| e.post_id).collect::<Vec<_>>(), vec![low]);
        assert_eq!(second.next_page_token, None);

        let mut no_location = query(DiscoveryRanking::Nearby, 1, None);
        no_location.payload.lat = None;
        assert!(matches!(h.handle(no_location).await.unwrap_err(), TimelineError::LocationRequired));
    }
}
