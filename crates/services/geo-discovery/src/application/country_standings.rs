//! The country ladder for the shop and the map's badges (#665): every country
//! the atlas draws, ranked by likes over the last days, with the price of
//! unlocking it. Computed at most once per cache period (the same for every
//! reader); the home country's price is the caller's to zero. With covers
//! (#942), each country's Shop card background, cached on its own so the
//! purchases that read prices never wait on it.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use async_trait::async_trait;
use chrono::{DateTime, NaiveDate, TimeDelta, Utc};
use futures::{StreamExt, TryStreamExt};
use uuid::Uuid;

use crate::application::port::{CountryActivityStore, MediaPreviews};
use crate::domain::country_atlas::CountryAtlas;
use crate::domain::country_standing::{rank, CountryCover, CountryStanding, CoverKind, UnlockPricing};
use crate::domain::entity::MapPostCard;
use crate::domain::value_object::CountryCode;
use crate::error::GeoDiscoveryError;

/// A country's most liked posts tried for its cover, most liked first.
pub const COVER_CANDIDATES: usize = 3;
/// Countries whose candidates are read at once.
const COVER_READS: usize = 16;
/// Candidates checked per call.
const COVER_CHECK_BATCH: usize = 100;

/// The ladder at a moment.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Ladder {
    pub standings:   Vec<CountryStanding>,
    pub computed_at: DateTime<Utc>,
}

/// Each country's Shop cover (#942); a country without one is absent.
pub type Covers = HashMap<CountryCode, CountryCover>;

/// The cards a cover may come from (#942).
#[async_trait]
pub trait CoverCards: Send + Sync + 'static {
    /// Of `post_ids`, the cards anyone may see on the map (the radar's rules
    /// for an anonymous reader, everywhere), from authors who share their
    /// precise location: a cover never names a city-level author's place.
    async fn cover_cards(&self, post_ids: Vec<Uuid>) -> Result<Vec<MapPostCard>, GeoDiscoveryError>;
}

pub struct CountryStandings {
    pub activity: Arc<dyn CountryActivityStore>,
    pub atlas:    &'static CountryAtlas,
    pub pricing:  UnlockPricing,
    /// Days of likes the ranking counts (30).
    pub window_days: u32,
    /// How long a computed ladder is served (60 s).
    pub cache_for:   Duration,
    /// Where covers come from (#942); none without.
    covers:          Option<Arc<dyn CoverCards>>,
    /// Where video covers' preview loops come from (#963); none without.
    previews:        Option<Arc<dyn MediaPreviews>>,
    cached:          Mutex<Option<(Instant, Arc<Ladder>)>>,
    /// The covers, per ladder they were picked for (its `computed_at`).
    cached_covers:   Mutex<Option<(DateTime<Utc>, Arc<Covers>)>>,
}

impl CountryStandings {
    pub fn new(
        activity: Arc<dyn CountryActivityStore>,
        atlas: &'static CountryAtlas,
        pricing: UnlockPricing,
        window_days: u32,
        cache_for: Duration,
    ) -> Self {
        Self {
            activity,
            atlas,
            pricing,
            window_days: window_days.max(1),
            cache_for,
            covers: None,
            previews: None,
            cached: Mutex::new(None),
            cached_covers: Mutex::new(None),
        }
    }

    /// With each country's Shop cover (#942).
    pub fn with_covers(mut self, covers: Arc<dyn CoverCards>) -> Self {
        self.covers = Some(covers);
        self
    }

    /// With video covers' preview loops (#963).
    pub fn with_previews(mut self, previews: Arc<dyn MediaPreviews>) -> Self {
        self.previews = Some(previews);
        self
    }

    /// The ladder: the cached one while fresh, else recomputed.
    pub async fn ladder(&self, now: DateTime<Utc>) -> Result<Arc<Ladder>, GeoDiscoveryError> {
        if let Some((at, ladder)) = self.cached.lock().expect("ladder cache").as_ref()
            && at.elapsed() < self.cache_for
        {
            return Ok(Arc::clone(ladder));
        }
        let today = now.date_naive();
        let days: Vec<_> = (0..self.window_days).map(|d| today - TimeDelta::days(i64::from(d))).collect();
        let activity = self.activity.totals(&days).await?;
        let ladder = Arc::new(Ladder { standings: rank(self.atlas.codes(), &activity), computed_at: now });
        *self.cached.lock().expect("ladder cache") = Some((Instant::now(), Arc::clone(&ladder)));
        Ok(ladder)
    }
}

impl CountryStandings {
    /// The covers for `ladder` (#942), picked once per ladder. Fails open: a
    /// cover only decorates, so an outage serves none until the next ladder
    /// (never a retry per read).
    pub async fn covers(&self, ladder: &Ladder) -> Arc<Covers> {
        let Some(cards) = &self.covers else { return Arc::default() };
        if let Some((at, covers)) = self.cached_covers.lock().expect("covers cache").as_ref()
            && *at == ladder.computed_at
        {
            return Arc::clone(covers);
        }
        let today = ladder.computed_at.date_naive();
        let days: Vec<_> = (0..self.window_days).map(|d| today - TimeDelta::days(i64::from(d))).collect();
        let covers = Arc::new(self.pick_covers(cards.as_ref(), &ladder.standings, &days).await.unwrap_or_else(|e| {
            tracing::warn!(error = %e, "country covers unavailable");
            Covers::new()
        }));
        *self.cached_covers.lock().expect("covers cache") = Some((ladder.computed_at, Arc::clone(&covers)));
        covers
    }

    /// Each liked country's cover: its first candidate, most liked first,
    /// that anyone may see and that was published there.
    async fn pick_covers(
        &self,
        cards: &dyn CoverCards,
        standings: &[CountryStanding],
        days: &[NaiveDate],
    ) -> Result<Covers, GeoDiscoveryError> {
        // Collected first: a borrowing iterator held across the reads would
        // keep the future from being `Send` for every lifetime.
        let mut liked: Vec<CountryCode> = Vec::new();
        for standing in standings {
            if standing.likes > 0 {
                liked.push(standing.country);
            }
        }
        let candidates: Vec<(CountryCode, Vec<(Uuid, i64)>)> = futures::stream::iter(liked)
            .map(|country| async move {
                Ok::<_, GeoDiscoveryError>((country, self.activity.top_posts(country, days, COVER_CANDIDATES).await?))
            })
            .buffered(COVER_READS)
            .try_collect()
            .await?;
        let ids: Vec<Uuid> = candidates.iter().flat_map(|(_, posts)| posts.iter().map(|(post, _)| *post)).collect();
        let mut shown: HashMap<Uuid, MapPostCard> = HashMap::new();
        for batch in ids.chunks(COVER_CHECK_BATCH) {
            shown.extend(cards.cover_cards(batch.to_vec()).await?.into_iter().map(|card| (card.post_id, card)));
        }
        let mut covers = HashMap::new();
        for (country, posts) in candidates {
            let cover = posts.iter().find_map(|(post, _)| {
                let card = shown.get(post)?;
                let there = matches!((card.lat, card.lng), (Some(lat), Some(lng)) if self.atlas.country_at(lat, lng) == Some(country));
                there.then(|| CountryCover::of(card)).flatten()
            });
            if let Some(cover) = cover {
                covers.insert(country, cover);
            }
        }
        self.add_previews(&mut covers, &shown).await;
        Ok(covers)
    }

    /// Video covers' preview loops (#963). Fails open: a cover without one
    /// paints its poster.
    async fn add_previews(&self, covers: &mut Covers, cards: &HashMap<Uuid, MapPostCard>) {
        let Some(previews) = &self.previews else { return };
        let assets: Vec<(CountryCode, Uuid)> = covers
            .iter()
            .filter(|(_, cover)| cover.kind == CoverKind::Video)
            .filter_map(|(country, cover)| Some((*country, cards.get(&cover.post_id)?.cover_asset_id?)))
            .collect();
        if assets.is_empty() {
            return;
        }
        let ids: Vec<Uuid> = assets.iter().map(|(_, asset)| *asset).collect();
        match previews.previews(&ids).await {
            Ok(urls) => {
                for (country, asset) in assets {
                    if let (Some(cover), Some(url)) = (covers.get_mut(&country), urls.get(&asset)) {
                        cover.preview_url = url.clone();
                    }
                }
            }
            Err(e) => tracing::warn!(error = %e, "country covers: previews unavailable"),
        }
    }
}

#[cfg(test)]
pub(crate) mod tests {
    use std::collections::HashMap;

    use async_trait::async_trait;
    use chrono::NaiveDate;

    use super::*;
    use crate::domain::country_standing::CountryActivity;
    use crate::domain::value_object::CountryCode;

    #[derive(Default)]
    pub struct MemActivity(
        pub Mutex<HashMap<(CountryCode, NaiveDate), CountryActivity>>,
        Mutex<std::collections::HashSet<(NaiveDate, String)>>,
        /// Each post's likes per country and day (#942).
        pub Mutex<HashMap<(CountryCode, NaiveDate, Uuid), i64>>,
    );

    #[async_trait]
    impl CountryActivityStore for MemActivity {
        async fn add(
            &self,
            country: CountryCode,
            day: NaiveDate,
            likes: i64,
            posts: i64,
            event: &str,
        ) -> Result<bool, GeoDiscoveryError> {
            if !self.1.lock().unwrap().insert((day, event.to_owned())) {
                return Ok(false);
            }
            let mut map = self.0.lock().unwrap();
            let entry = map.entry((country, day)).or_default();
            entry.likes += likes;
            entry.posts += posts;
            Ok(true)
        }
        async fn add_post_likes(&self, country: CountryCode, day: NaiveDate, post: Uuid, likes: i64) -> Result<(), GeoDiscoveryError> {
            *self.2.lock().unwrap().entry((country, day, post)).or_default() += likes;
            Ok(())
        }
        async fn top_posts(&self, country: CountryCode, days: &[NaiveDate], limit: usize) -> Result<Vec<(Uuid, i64)>, GeoDiscoveryError> {
            let mut sums: HashMap<Uuid, i64> = HashMap::new();
            for ((c, day, post), likes) in self.2.lock().unwrap().iter() {
                if *c == country && days.contains(day) {
                    *sums.entry(*post).or_default() += likes;
                }
            }
            let mut top: Vec<(Uuid, i64)> = sums.into_iter().collect();
            top.sort_by(|a, b| b.1.cmp(&a.1).then(a.0.cmp(&b.0)));
            top.truncate(limit);
            Ok(top)
        }
        async fn totals(&self, days: &[NaiveDate]) -> Result<HashMap<CountryCode, CountryActivity>, GeoDiscoveryError> {
            let mut totals: HashMap<CountryCode, CountryActivity> = HashMap::new();
            for ((country, day), a) in self.0.lock().unwrap().iter() {
                if days.contains(day) {
                    let t = totals.entry(*country).or_default();
                    t.likes += a.likes;
                    t.posts += a.posts;
                }
            }
            Ok(totals)
        }
    }

    fn cc(code: &str) -> CountryCode {
        CountryCode::try_from(code).unwrap()
    }

    #[tokio::test]
    async fn the_ladder_counts_the_window_only_and_covers_every_country() {
        let activity = Arc::new(MemActivity::default());
        let now = Utc::now();
        let today = now.date_naive();
        activity.add(cc("JP"), today, 5, 1, "a").await.unwrap();
        activity.add(cc("JP"), today, 5, 1, "a").await.unwrap();
        activity.add(cc("FR"), today - TimeDelta::days(29), 3, 1, "b").await.unwrap();
        // Outside a 30-day window.
        activity.add(cc("US"), today - TimeDelta::days(30), 100, 1, "c").await.unwrap();
        let standings = CountryStandings::new(activity, CountryAtlas::embedded(), UnlockPricing::default(), 30, Duration::ZERO);
        let ladder = standings.ladder(now).await.unwrap();
        assert_eq!(ladder.standings.len(), CountryAtlas::embedded().codes().count());
        assert_eq!((ladder.standings[0].country, ladder.standings[0].likes), (cc("JP"), 5), "an event counts once");
        assert_eq!(ladder.standings[1].country, cc("FR"));
        let us = ladder.standings.iter().find(|s| s.country == cc("US")).unwrap();
        assert_eq!(us.likes, 0, "older than the window");
    }

    #[tokio::test]
    async fn a_fresh_ladder_is_served_from_the_cache() {
        let activity = Arc::new(MemActivity::default());
        let standings =
            CountryStandings::new(Arc::clone(&activity) as _, CountryAtlas::embedded(), UnlockPricing::default(), 30, Duration::from_secs(60));
        let first = standings.ladder(Utc::now()).await.unwrap();
        activity.add(cc("JP"), Utc::now().date_naive(), 5, 1, "a").await.unwrap();
        assert!(Arc::ptr_eq(&first, &standings.ladder(Utc::now()).await.unwrap()));
    }

    /// Cards a fake map shows: those it holds; `down` fails every check.
    #[derive(Default)]
    struct Shown(Mutex<HashMap<Uuid, MapPostCard>>, Mutex<bool>);

    #[async_trait]
    impl CoverCards for Shown {
        async fn cover_cards(&self, post_ids: Vec<Uuid>) -> Result<Vec<MapPostCard>, GeoDiscoveryError> {
            if *self.1.lock().unwrap() {
                return Err(GeoDiscoveryError::SpatialLuaReturnInvalid);
            }
            let shown = self.0.lock().unwrap();
            Ok(post_ids.iter().filter_map(|id| shown.get(id).cloned()).collect())
        }
    }

    fn card_at(lat: f64, lng: f64, thumbnail: &str) -> MapPostCard {
        MapPostCard {
            post_id:           Uuid::now_v7(),
            author_id:         Uuid::now_v7(),
            author_handle:     String::new(),
            author_avatar_url: String::new(),
            thumbnail_url:     thumbnail.into(),
            caption:           "words".into(),
            h3_index_r7:       0,
            virality_score:    0.0,
            published_at_ms:   0,
            author_tier:       0,
            lat:               Some(lat),
            lng:               Some(lng),
            format:            crate::domain::value_object::PostFormat::Photo,
            cover_asset_id:    None,
        }
    }

    /// #942: a country's cover is its most liked post anyone may see that
    /// was published there; none otherwise, and an outage serves no covers.
    #[tokio::test]
    async fn a_cover_is_the_most_liked_post_anyone_may_see_there() {
        let activity = Arc::new(MemActivity::default());
        let shown = Arc::new(Shown::default());
        let today = Utc::now().date_naive();
        let tokyo = (35.68, 139.69);
        let (hidden, liked, less) = (card_at(tokyo.0, tokyo.1, "h.jpg"), card_at(tokyo.0, tokyo.1, "l.jpg"), card_at(tokyo.0, tokyo.1, "s.jpg"));
        // Liked in France, but its card is in Japan: not France's cover.
        let elsewhere = card_at(tokyo.0, tokyo.1, "e.jpg");
        for (card, likes) in [(&liked, 10), (&less, 5)] {
            shown.0.lock().unwrap().insert(card.post_id, card.clone());
            activity.add_post_likes(cc("JP"), today, card.post_id, likes).await.unwrap();
        }
        activity.add_post_likes(cc("JP"), today, hidden.post_id, 50).await.unwrap();
        shown.0.lock().unwrap().insert(elsewhere.post_id, elsewhere.clone());
        activity.add_post_likes(cc("FR"), today, elsewhere.post_id, 7).await.unwrap();
        activity.add(cc("JP"), today, 65, 3, "jp").await.unwrap();
        activity.add(cc("FR"), today, 7, 1, "fr").await.unwrap();

        let standings = |shown: Arc<Shown>| {
            CountryStandings::new(Arc::clone(&activity) as _, CountryAtlas::embedded(), UnlockPricing::default(), 30, Duration::ZERO)
                .with_covers(shown)
        };
        let with_covers = standings(Arc::clone(&shown));
        let ladder = with_covers.ladder(Utc::now()).await.unwrap();
        let covers = with_covers.covers(&ladder).await;
        let jp = covers.get(&cc("JP")).expect("a cover for Japan");
        assert_eq!(jp.post_id, liked.post_id, "the hidden one is skipped, the most liked shown one wins");
        assert_eq!(jp.poster_url, "l.jpg");
        assert!(!covers.contains_key(&cc("FR")), "never a post published elsewhere");
        assert!(!covers.contains_key(&cc("US")), "no likes, no cover");
        assert!(Arc::ptr_eq(&covers, &with_covers.covers(&ladder).await), "picked once per ladder");

        *shown.1.lock().unwrap() = true;
        let failing = standings(shown);
        let ladder = failing.ladder(Utc::now()).await.unwrap();
        assert!(failing.covers(&ladder).await.is_empty());
        assert_eq!(ladder.standings[0].country, cc("JP"), "the ladder still serves");
    }

    /// Previews a fake media delivers: those it holds; `down` fails.
    #[derive(Default)]
    struct Previews(Mutex<HashMap<Uuid, String>>, Mutex<bool>);

    #[async_trait]
    impl MediaPreviews for Previews {
        async fn previews(&self, ids: &[Uuid]) -> Result<HashMap<Uuid, String>, GeoDiscoveryError> {
            if *self.1.lock().unwrap() {
                return Err(GeoDiscoveryError::MediaUnavailable { reason: "down".into() });
            }
            let held = self.0.lock().unwrap();
            Ok(ids.iter().filter_map(|id| Some((*id, held.get(id)?.clone()))).collect())
        }
    }

    /// #963: a video cover plays its asset's preview loop; a photo cover
    /// never asks; media down keeps the poster.
    #[tokio::test]
    async fn a_video_cover_plays_its_preview_loop_and_media_down_keeps_the_poster() {
        let activity = Arc::new(MemActivity::default());
        let shown = Arc::new(Shown::default());
        let previews = Arc::new(Previews::default());
        let today = Utc::now().date_naive();
        let asset = Uuid::now_v7();
        let mut video = card_at(35.68, 139.69, "v.jpg");
        video.format = crate::domain::value_object::PostFormat::Video;
        video.cover_asset_id = Some(asset);
        shown.0.lock().unwrap().insert(video.post_id, video.clone());
        activity.add_post_likes(cc("JP"), today, video.post_id, 10).await.unwrap();
        activity.add(cc("JP"), today, 10, 1, "jp").await.unwrap();
        previews.0.lock().unwrap().insert(asset, "https://cdn/preview.mp4".into());

        let standings = || {
            CountryStandings::new(Arc::clone(&activity) as _, CountryAtlas::embedded(), UnlockPricing::default(), 30, Duration::ZERO)
                .with_covers(Arc::clone(&shown) as _)
                .with_previews(Arc::clone(&previews) as _)
        };
        let with = standings();
        let ladder = with.ladder(Utc::now()).await.unwrap();
        let jp = with.covers(&ladder).await.get(&cc("JP")).cloned().unwrap();
        assert_eq!((jp.kind, jp.poster_url.as_str(), jp.preview_url.as_str()), (CoverKind::Video, "v.jpg", "https://cdn/preview.mp4"));

        *previews.1.lock().unwrap() = true;
        let down = standings();
        let ladder = down.ladder(Utc::now()).await.unwrap();
        let jp = down.covers(&ladder).await.get(&cc("JP")).cloned().unwrap();
        assert_eq!((jp.poster_url.as_str(), jp.preview_url.as_str()), ("v.jpg", ""), "the poster stays");
    }
}
