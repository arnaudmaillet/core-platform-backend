use std::collections::{HashMap, HashSet, VecDeque};
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use cqrs::{Envelope, Query, QueryHandler};
use validate_core::{FieldViolation, Validate};

use crate::application::port::{
    muted_authors, visible_authors, DiscoveryPool, InterestStore, NearbyPosts, PoolEntry, SocialGraphClient,
};
use crate::domain::aggregate::FeedEntry;
use crate::domain::value_object::interest::{affinity, BOOST_INTERESTS};
use crate::domain::value_object::{
    hot_score, ContentLevel, DiscoveryCursor, DiscoveryRanking, DiscoveryStream, PostId, PostKindFilter, ProfileId,
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
    /// The reading profile whose interest tags rank a FOR_YOU page (#662);
    /// `None` for a guest or a non-personalised feed. Bound to the caller at
    /// the edge.
    pub personalize_for: Option<ProfileId>,
    /// Only the posts carrying this hashtag, normalized (#830): TRENDING and
    /// RECENT over the pool's per-tag streams.
    pub hashtag:       Option<String>,
    /// Only posts of this kind (#949).
    pub post_kind:     PostKindFilter,
}

#[derive(Debug)]
pub struct DiscoveryPage {
    pub items:           Vec<FeedEntry>,
    pub next_page_token: Option<String>,
    /// Whether the reader's interest tags ranked the page.
    pub personalized:    bool,
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
    pub interests:         Arc<dyn InterestStore>,
    /// The pool's window (#970): the first page fixes its lower bound.
    pub window_secs:       u64,
    /// Whether a page carries on with the archive once the window is
    /// exhausted (#970); without one the feed ends with the window.
    pub backfill:          bool,
}

/// Which posts a phase of the feed serves (#970), by publication time: the
/// window's (at or after the bound the first page fixed) or the archive's
/// (before it) — so none is served twice.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Bound {
    Since(i64),
    Before(i64),
}

impl Bound {
    fn keeps(self, published_at_ms: i64) -> bool {
        match self {
            Self::Since(bound) => published_at_ms >= bound,
            Self::Before(bound) => published_at_ms < bound,
        }
    }
}

fn now_ms() -> i64 {
    SystemTime::now().duration_since(UNIX_EPOCH).map(|d| d.as_millis() as i64).unwrap_or_default()
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
    /// The hashtags of every post read, for the interest ranking.
    tags:         HashMap<PostId, Vec<String>>,
    /// The kind of post a page keeps (#949).
    post_kind:    PostKindFilter,
    /// The phase being read (#970).
    bound:        Bound,
    /// The hot score of every post read, to rank back-filled posts (#970).
    hot:          HashMap<PostId, f64>,
    gravity_secs: f64,
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
        for m in meta.values() {
            if !m.tags.is_empty() {
                self.tags.insert(m.post_id, m.tags.clone());
            }
            if let Some(published_at_ms) = m.published_at_ms {
                self.hot.insert(m.post_id, hot_score(m.popularity, published_at_ms, self.gravity_secs));
            }
        }
        let (muted, bound) = (&self.muted, self.bound);
        let shown = |e: &PoolEntry| {
            meta.get(&e.post_id)
                .filter(|m| m.shown_at(self.level) && m.author_id == Some(e.author_id))
                .filter(|m| m.published_at_ms.is_some_and(|t| bound.keeps(t)))
                .filter(|m| self.post_kind.keeps(m.format))
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
    /// The hashtag whose stream this is (#830).
    tag:      Option<String>,
    fetched:  Option<StreamPosition>,
    consumed: Option<StreamPosition>,
    buffer:   VecDeque<(StreamPosition, Option<FeedEntry>)>,
    drained:  bool,
    refills:  usize,
}

impl StreamReader {
    fn new(stream: DiscoveryStream, tag: Option<String>, cursor: &DiscoveryCursor) -> Self {
        Self::at(stream, tag, cursor.position(stream).cloned())
    }

    fn at(stream: DiscoveryStream, tag: Option<String>, start: Option<StreamPosition>) -> Self {
        Self { stream, tag, fetched: start.clone(), consumed: start, buffer: VecDeque::new(), drained: false, refills: 0 }
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
            let entries = self.pool.range(reader.stream, reader.tag.as_deref(), reader.fetched.as_ref(), batch).await?;
            reader.drained = entries.len() < batch;
            if let Some(last) = entries.last() {
                reader.fetched = Some(last.position.clone());
            }
            reader.buffer.extend(filter.apply(self.pool.as_ref(), entries).await?);
        }
    }

    /// Fills `items` (up to `size`) from `readers` in `pattern` (one stream
    /// running dry hands over to the next), skipping posts already `seen`.
    async fn read_pattern(
        &self,
        pattern: &[DiscoveryStream],
        readers: &mut [StreamReader],
        filter:  &mut Filter<'_, SG>,
        size:    usize,
        items:   &mut Vec<FeedEntry>,
        seen:    &mut HashSet<PostId>,
    ) -> Result<(), TimelineError> {
        let batch = (size * 2).max(DEFAULT_PAGE_SIZE);
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
        Ok(())
    }

    /// TRENDING, RECENT and FOR_YOU: read the pool's streams in the ranking's
    /// pattern (FOR_YOU: three hot, one fresh; one stream running dry hands
    /// over to the other) — the posts published since the window's bound —
    /// then, once they are all exhausted, the archive (#970).
    async fn pool_page(
        &self,
        ranking: DiscoveryRanking,
        hashtag: Option<&str>,
        cursor:  &DiscoveryCursor,
        since:   i64,
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
                readers.push(StreamReader::new(*stream, hashtag.map(str::to_owned), cursor));
            }
        }
        filter.bound = Bound::Since(since);
        let (mut items, mut seen) = (Vec::with_capacity(size), HashSet::new());
        self.read_pattern(pattern, &mut readers, filter, size, &mut items, &mut seen).await?;

        if !readers.iter().all(StreamReader::exhausted) {
            let positions = readers
                .into_iter()
                .filter_map(|r| r.consumed.map(|p| (r.stream, p)))
                .collect();
            let token = DiscoveryCursor { positions, since_ms: Some(since), backfill: false }.encode(ranking);
            return Ok(DiscoveryPage { items, next_page_token: Some(token), personalized: false });
        }
        self.archive_page(ranking, hashtag, None, since, filter, size, items).await
    }

    /// The archive (#970): the posts published before the window's bound,
    /// newest first from `start` (the bound itself when `None`), appended to
    /// `items`. TRENDING, FOR_YOU and NEARBY rank what it adds by hot score;
    /// RECENT keeps it newest first. Without an archive, the feed ends here.
    #[allow(clippy::too_many_arguments)]
    async fn archive_page(
        &self,
        ranking:   DiscoveryRanking,
        hashtag:   Option<&str>,
        start:     Option<StreamPosition>,
        since:     i64,
        filter:    &mut Filter<'_, SG>,
        size:      usize,
        mut items: Vec<FeedEntry>,
    ) -> Result<DiscoveryPage, TimelineError> {
        if !self.backfill {
            return Ok(DiscoveryPage { items, next_page_token: None, personalized: false });
        }
        // Nothing at or after the bound: a position at the bound itself.
        let start = start.unwrap_or(StreamPosition { score: since as f64, member: String::new() });
        let mut readers = [StreamReader::at(DiscoveryStream::Archive, hashtag.map(str::to_owned), Some(start))];
        filter.bound = Bound::Before(since);
        let from = items.len();
        let mut seen: HashSet<PostId> = items.iter().map(|e| e.post_id).collect();
        self.read_pattern(&[DiscoveryStream::Archive], &mut readers, filter, size, &mut items, &mut seen).await?;
        if ranking != DiscoveryRanking::Recent {
            let hot = |e: &FeedEntry| filter.hot.get(&e.post_id).copied().unwrap_or(f64::MIN);
            items[from..].sort_by(|a, b| hot(b).total_cmp(&hot(a)));
        }
        let [reader] = readers;
        let next_page_token = (!reader.exhausted()).then(|| {
            // Nothing consumed yet: the bound is the start again (no position).
            let positions = reader
                .consumed
                .filter(|p| !p.member.is_empty())
                .map(|p| vec![(DiscoveryStream::Archive, p)])
                .unwrap_or_default();
            DiscoveryCursor { positions, since_ms: Some(since), backfill: true }.encode(ranking)
        });
        Ok(DiscoveryPage { items, next_page_token, personalized: false })
    }

    /// NEARBY: geo-discovery's posts around the point, ranked by hot score from
    /// the pool's meta (a post the pool does not know is not shown).
    async fn nearby_page(
        &self,
        query:  &GetDiscoveryFeedQuery,
        cursor: &DiscoveryCursor,
        since:  i64,
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
        filter.bound = Bound::Since(since);
        let mut ranked: Vec<PoolEntry> = meta
            .values()
            .filter(|m| m.shown_at(filter.level) && m.published_at_ms.is_some_and(|t| filter.bound.keeps(t)))
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
        if more {
            let positions = last.map(|p| vec![(DiscoveryStream::Nearby, p)]).unwrap_or_default();
            let token = DiscoveryCursor { positions, since_ms: Some(since), backfill: false }.encode(DiscoveryRanking::Nearby);
            return Ok(DiscoveryPage { items, next_page_token: Some(token), personalized: false });
        }
        // geo keeps its pins a short while: past them, the archive (#970).
        self.archive_page(DiscoveryRanking::Nearby, None, None, since, filter, size, items).await
    }

    /// The reader's heaviest interest tags, for a FOR_YOU page only. Fails
    /// open: an unreachable store serves the page unranked.
    async fn interests_for(&self, query: &GetDiscoveryFeedQuery) -> HashMap<String, f64> {
        let (Some(profile), DiscoveryRanking::ForYou) = (&query.personalize_for, query.ranking) else {
            return HashMap::new();
        };
        match self.interests.top(profile, now_ms(), BOOST_INTERESTS).await {
            Ok(top) => top.into_iter().map(|i| (i.tag, i.weight)).collect(),
            Err(e) => {
                tracing::warn!(error = %e, "interest tags unavailable; For You page served unranked");
                HashMap::new()
            }
        }
    }
}

/// Ranks a page by its posts' affinity with the interests, heaviest first; the
/// streams' order holds between equals (the sort is stable). Within the page
/// only, so the cursor (per stream) is untouched.
fn rank_by_interest(items: &mut Vec<FeedEntry>, tags: &HashMap<PostId, Vec<String>>, interests: &HashMap<String, f64>) {
    let mut scored: Vec<(f64, FeedEntry)> = items
        .drain(..)
        .map(|e| (tags.get(&e.post_id).map_or(0.0, |t| affinity(t, interests)), e))
        .collect();
    scored.sort_by(|a, b| b.0.total_cmp(&a.0));
    items.extend(scored.into_iter().map(|(_, e)| e));
}

impl<SG: SocialGraphClient> QueryHandler<GetDiscoveryFeedQuery> for GetDiscoveryFeedHandler<SG> {
    type Error = TimelineError;

    async fn handle(&self, envelope: Envelope<GetDiscoveryFeedQuery>) -> Result<DiscoveryPage, TimelineError> {
        let query = &envelope.payload;
        // A hashtag narrows TRENDING and RECENT only (#830).
        if query.hashtag.is_some() && !matches!(query.ranking, DiscoveryRanking::Trending | DiscoveryRanking::Recent) {
            return Err(TimelineError::DomainViolation {
                field:   "hashtag".to_owned(),
                message: "a hashtag narrows TRENDING and RECENT only".to_owned(),
            });
        }
        let cursor = match query.page_token.as_deref().filter(|t| !t.is_empty()) {
            Some(token) => DiscoveryCursor::decode(token, query.ranking)?,
            None => DiscoveryCursor::default(),
        };
        let size = self.page_size(query.limit);
        let muted = match &query.viewer {
            Viewer::Profiles(own) => muted_authors(self.social_graph.as_ref(), own).await,
            Viewer::Internal => HashSet::new(),
        };
        let interests = self.interests_for(query).await;
        let mut filter = Filter {
            social_graph: self.social_graph.as_ref(),
            viewer:       &query.viewer,
            level:        query.content_level,
            visible:      HashMap::new(),
            muted,
            tags:         HashMap::new(),
            post_kind:    query.post_kind,
            bound:        Bound::Since(i64::MIN),
            hot:          HashMap::new(),
            gravity_secs: self.hot_gravity_secs,
        };
        // The window's bound, fixed by the first page (#970); a token minted
        // before it takes the request's time.
        let since = cursor.since_ms.unwrap_or_else(|| now_ms() - (self.window_secs as i64) * 1000);
        let hashtag = query.hashtag.as_deref();
        let mut page = match query.ranking {
            ranking if cursor.backfill => {
                let start = cursor.position(DiscoveryStream::Archive).cloned();
                self.archive_page(ranking, hashtag, start, since, &mut filter, size, Vec::new()).await?
            }
            DiscoveryRanking::Nearby => self.nearby_page(query, &cursor, since, &mut filter, size).await?,
            ranking => self.pool_page(ranking, hashtag, &cursor, since, &mut filter, size).await?,
        };
        if !interests.is_empty() {
            rank_by_interest(&mut page.items, &filter.tags, &interests);
            page.personalized = true;
        }
        Ok(page)
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Mutex;

    use async_trait::async_trait;
    use uuid::Uuid;

    use super::*;
    use crate::domain::value_object::{AuthorId, ContentAccess, DiscoveryMeta, Interest, PostFormat, ProfileId, Restriction};

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
                // In the window, unless archived (below).
                published_at_ms: Some(now_ms() - 60_000),
                popularity: score,
                restriction,
                deleted: false,
                tags: Vec::new(),
                format: PostFormat::Unknown,
            });
            post_id
        }

        /// A post in the archive only (#970), published `days_ago`, with its
        /// popularity.
        fn archived(&self, author: AuthorId, days_ago: i64, popularity: f64) -> PostId {
            let post_id = PostId::from_uuid(Uuid::now_v7());
            let published = now_ms() - days_ago * 86_400_000;
            self.streams.lock().unwrap().entry(DiscoveryStream::Archive).or_default().push(PoolEntry {
                post_id,
                author_id: author,
                position: StreamPosition { score: published as f64, member: format!("{post_id}:{author}") },
            });
            self.meta.lock().unwrap().insert(post_id, DiscoveryMeta {
                post_id,
                author_id: Some(author),
                published_at_ms: Some(published),
                popularity,
                restriction: Restriction::None,
                deleted: false,
                tags: Vec::new(),
                format: PostFormat::Unknown,
            });
            post_id
        }

        fn shape(&self, post_id: PostId, format: PostFormat) {
            self.meta.lock().unwrap().get_mut(&post_id).unwrap().format = format;
        }

        fn tag(&self, post_id: PostId, tags: &[&str]) {
            self.meta.lock().unwrap().get_mut(&post_id).unwrap().tags = tags.iter().map(|t| (*t).to_owned()).collect();
        }
    }

    /// Fixed interests for every profile; `down` fails every read.
    #[derive(Default)]
    struct Interests {
        top:  Vec<Interest>,
        down: bool,
    }

    #[async_trait]
    impl InterestStore for Interests {
        async fn reinforce(&self, _: &ProfileId, _: &PostId, _: &[String], _: i64) -> Result<(), TimelineError> { Ok(()) }
        async fn top(&self, _: &ProfileId, _: i64, limit: usize) -> Result<Vec<Interest>, TimelineError> {
            if self.down {
                return Err(TimelineError::ScriptReturnInvalid { context: "down" });
            }
            Ok(self.top.iter().take(limit).cloned().collect())
        }
        async fn remove(&self, _: &ProfileId, _: &str) -> Result<(), TimelineError> { Ok(()) }
        async fn reset(&self, _: &ProfileId) -> Result<(), TimelineError> { Ok(()) }
        async fn set_personalized(&self, _: &ProfileId, _: bool) -> Result<(), TimelineError> { Ok(()) }
        async fn erase(&self, _: &ProfileId) -> Result<(), TimelineError> { Ok(()) }
    }

    #[async_trait]
    impl DiscoveryPool for MemPool {
        async fn record_published(&self, _: &PostId, _: &AuthorId, _: i64, _: &[String], _: PostFormat) -> Result<(), TimelineError> { Ok(()) }
        async fn record_popularity(&self, _: &PostId, _: f64) -> Result<(), TimelineError> { Ok(()) }
        async fn record_restriction(&self, _: &PostId, _: Restriction, _: i64) -> Result<(), TimelineError> { Ok(()) }
        async fn record_deleted(&self, _: &PostId) -> Result<(), TimelineError> { Ok(()) }

        async fn range(
            &self,
            stream: DiscoveryStream,
            tag:    Option<&str>,
            after:  Option<&StreamPosition>,
            count:  usize,
        ) -> Result<Vec<PoolEntry>, TimelineError> {
            let mut all = self.streams.lock().unwrap().get(&stream).cloned().unwrap_or_default();
            if let Some(tag) = tag {
                let meta = self.meta.lock().unwrap();
                all.retain(|e| meta.get(&e.post_id).is_some_and(|m| m.tags.iter().any(|t| t == tag)));
            }
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
            interests: Arc::new(Interests::default()),
            window_secs: 259_200,
            backfill: false,
        }
    }

    fn with_backfill(pool: Arc<MemPool>, graph: Arc<Graph>, around: Vec<PostId>) -> GetDiscoveryFeedHandler<Graph> {
        GetDiscoveryFeedHandler { backfill: true, ..handler(pool, graph, around) }
    }

    /// Pages `ranking` to its end, `limit` at a time: every page's ids.
    async fn page_all(h: &GetDiscoveryFeedHandler<Graph>, ranking: DiscoveryRanking, limit: i32) -> Vec<Vec<PostId>> {
        let (mut pages, mut token) = (Vec::new(), None);
        for _ in 0..50 {
            let mut ask = query(ranking, limit, token);
            ask.payload.viewer = Viewer::Profiles(vec![Uuid::from_u128(1).to_string()]);
            let page = h.handle(ask).await.unwrap();
            pages.push(page.items.iter().map(|e| e.post_id).collect());
            match page.next_page_token {
                Some(next) => token = Some(next),
                None => return pages,
            }
        }
        panic!("never ended");
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
            personalize_for: None,
            hashtag: None,
            post_kind: PostKindFilter::Any,
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

    /// #949: a kind filter keeps its kind, and the posts of unknown kind
    /// (published before the pool kept it); NEARBY too.
    #[tokio::test]
    async fn a_kind_filter_keeps_its_kind_and_the_unknown() {
        let pool = Arc::new(MemPool::default());
        let a = author();
        let media = pool.put(DiscoveryStream::Recent, a, 9.0, Restriction::None);
        pool.shape(media, PostFormat::Media);
        let text = pool.put(DiscoveryStream::Recent, a, 8.0, Restriction::None);
        pool.shape(text, PostFormat::TextOnly);
        let unknown = pool.put(DiscoveryStream::Recent, a, 7.0, Restriction::None);
        let h = handler(Arc::clone(&pool), Arc::new(Graph::default()), vec![media, text, unknown]);
        let ids = |page: DiscoveryPage| page.items.iter().map(|e| e.post_id).collect::<Vec<_>>();

        for (ranking, filter, expected) in [
            (DiscoveryRanking::Recent, PostKindFilter::Any, vec![media, text, unknown]),
            (DiscoveryRanking::Recent, PostKindFilter::Media, vec![media, unknown]),
            (DiscoveryRanking::Recent, PostKindFilter::TextOnly, vec![text, unknown]),
            (DiscoveryRanking::Nearby, PostKindFilter::TextOnly, vec![text, unknown]),
        ] {
            let mut q = query(ranking, 10, None);
            q.payload.post_kind = filter;
            assert_eq!(ids(h.handle(q).await.unwrap()), expected, "{ranking:?} {filter:?}");
        }
    }

    #[test]
    fn a_published_kind_maps_to_its_format() {
        assert_eq!(PostFormat::from_event_kind("TextOnly"), PostFormat::TextOnly);
        assert_eq!(PostFormat::from_event_kind("Carousel"), PostFormat::Media);
        assert_eq!(PostFormat::from_event_kind("MainVideo"), PostFormat::Media);
        assert_eq!(PostFormat::from_event_kind("image"), PostFormat::Unknown);
        for format in [PostFormat::Unknown, PostFormat::Media, PostFormat::TextOnly] {
            assert_eq!(PostFormat::from_code(format.code()), format);
        }
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

    #[tokio::test]
    async fn for_you_ranks_a_page_by_the_readers_interest_tags() {
        let pool = Arc::new(MemPool::default());
        let a = author();
        let plain = pool.put(DiscoveryStream::Hot, a, 100.0, Restriction::None);
        let go = pool.put(DiscoveryStream::Hot, a, 90.0, Restriction::None);
        let rust = pool.put(DiscoveryStream::Hot, a, 80.0, Restriction::None);
        let both = pool.put(DiscoveryStream::Hot, a, 70.0, Restriction::None);
        pool.tag(go, &["go"]);
        pool.tag(rust, &["rust"]);
        pool.tag(both, &["rust", "go"]);
        let ids = |page: &DiscoveryPage| page.items.iter().map(|e| e.post_id).collect::<Vec<_>>();
        let reader = |ranking| {
            let mut q = query(ranking, 10, None);
            q.payload.personalize_for = Some(ProfileId::from_uuid(Uuid::now_v7()));
            q
        };
        let interests = Interests {
            top:  vec![Interest { tag: "rust".into(), weight: 2.0 }, Interest { tag: "go".into(), weight: 0.5 }],
            down: false,
        };
        let mut h = handler(Arc::clone(&pool), Arc::new(Graph::default()), vec![]);
        h.interests = Arc::new(interests);

        let page = h.handle(reader(DiscoveryRanking::ForYou)).await.unwrap();
        assert_eq!(ids(&page), vec![both, rust, go, plain]);
        assert!(page.personalized);

        // Not personalised: no reader, or another ranking.
        let page = h.handle(query(DiscoveryRanking::ForYou, 10, None)).await.unwrap();
        assert_eq!(ids(&page), vec![plain, go, rust, both]);
        assert!(!page.personalized);
        let page = h.handle(reader(DiscoveryRanking::Trending)).await.unwrap();
        assert_eq!(ids(&page), vec![plain, go, rust, both]);

        // A store outage serves the page unranked.
        h.interests = Arc::new(Interests { top: vec![], down: true });
        let page = h.handle(reader(DiscoveryRanking::ForYou)).await.unwrap();
        assert_eq!(ids(&page), vec![plain, go, rust, both]);
        assert!(!page.personalized);
    }

    /// #830: a hashtag narrows TRENDING to the posts carrying it, in the same
    /// hot order; FOR_YOU and NEARBY refuse one.
    #[tokio::test]
    async fn a_hashtag_narrows_trending_and_recent_only() {
        let pool = Arc::new(MemPool::default());
        let a = author();
        let low = pool.put(DiscoveryStream::Hot, a, 2.0, Restriction::None);
        let high = pool.put(DiscoveryStream::Hot, a, 9.0, Restriction::None);
        let other = pool.put(DiscoveryStream::Hot, a, 50.0, Restriction::None);
        pool.tag(low, &["rust"]);
        pool.tag(high, &["rust", "go"]);
        pool.tag(other, &["go"]);
        let h = handler(Arc::clone(&pool), Arc::new(Graph::default()), vec![]);

        let mut tagged = query(DiscoveryRanking::Trending, 10, None);
        tagged.payload.hashtag = Some("rust".to_owned());
        let page = h.handle(tagged).await.unwrap();
        assert_eq!(page.items.iter().map(|e| e.post_id).collect::<Vec<_>>(), vec![high, low]);

        for ranking in [DiscoveryRanking::ForYou, DiscoveryRanking::Nearby] {
            let mut q = query(ranking, 10, None);
            q.payload.hashtag = Some("rust".to_owned());
            assert!(matches!(h.handle(q).await, Err(TimelineError::DomainViolation { .. })));
        }
    }

    /// #970: once the window is exhausted, every ranking carries on with the
    /// archive — never a post twice, filtered like the window — and ends only
    /// with it. RECENT back-fills newest first; the others rank each
    /// back-filled page by hot score.
    #[tokio::test]
    async fn every_ranking_carries_on_with_the_archive_then_ends() {
        let pool = Arc::new(MemPool::default());
        let (a, hidden, muted) = (author(), author(), author());
        let window: Vec<PostId> = (0..3).map(|i| pool.put(DiscoveryStream::Hot, a, 10.0 - i as f64, Restriction::None)).collect();
        for w in &window {
            pool.streams.lock().unwrap().entry(DiscoveryStream::Recent).or_default().push(PoolEntry {
                post_id: *w,
                author_id: a,
                position: StreamPosition { score: now_ms() as f64, member: format!("{w}:{a}") },
            });
        }
        // Four days back (newest first: 4, 5, 6, 7); 6 days ago is the most popular.
        let old: Vec<PostId> = [(4, 1.0), (5, 2.0), (6, 500.0), (7, 3.0)].iter().map(|(d, p)| pool.archived(a, *d, *p)).collect();
        let removed = pool.archived(a, 8, 1.0);
        pool.meta.lock().unwrap().get_mut(&removed).unwrap().restriction = Restriction::Removed;
        pool.archived(hidden, 9, 1.0);
        pool.archived(muted, 9, 1.0);
        // An archive entry still in the window is the window's, not served twice.
        let recent_in_archive = window[0];
        pool.streams.lock().unwrap().entry(DiscoveryStream::Archive).or_default().push(PoolEntry {
            post_id: recent_in_archive,
            author_id: a,
            position: StreamPosition { score: now_ms() as f64, member: format!("{recent_in_archive}:{a}") },
        });
        let graph = Arc::new(Graph::default());
        graph.hidden.lock().unwrap().insert(hidden);
        graph.muted.lock().unwrap().insert(muted);
        let h = with_backfill(Arc::clone(&pool), graph, window.clone());

        for ranking in [DiscoveryRanking::ForYou, DiscoveryRanking::Trending, DiscoveryRanking::Recent, DiscoveryRanking::Nearby] {
            let pages = page_all(&h, ranking, 2).await;
            let served: Vec<PostId> = pages.concat();
            let distinct: HashSet<PostId> = served.iter().copied().collect();
            assert_eq!(distinct.len(), served.len(), "{ranking:?}: no repeats {pages:?}");
            assert_eq!(distinct, window.iter().chain(&old).copied().collect(), "{ranking:?}");
            let archived: Vec<PostId> = served.iter().filter(|p| old.contains(p)).copied().collect();
            if ranking == DiscoveryRanking::Recent {
                assert_eq!(archived, old, "newest first");
            } else {
                // The page holding the 6-days-old popular post shows it first.
                let page = pages.iter().find(|p| p.contains(&old[2])).unwrap();
                assert_eq!(page.iter().find(|p| old.contains(p)), Some(&old[2]), "{ranking:?}: {pages:?}");
            }
        }

        // Without an archive the feed ends with the window.
        let h = handler(Arc::clone(&pool), Arc::new(Graph::default()), vec![]);
        assert_eq!(page_all(&h, DiscoveryRanking::Trending, 10).await.concat().len(), 3);
    }
}
