//! The country ladder for the shop and the map's badges (#665): every country
//! the atlas draws, ranked by likes over the last days, with the price of
//! unlocking it. Computed at most once per cache period (the same for every
//! reader); the home country's price is the caller's to zero.

use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use chrono::{DateTime, TimeDelta, Utc};

use crate::application::port::CountryActivityStore;
use crate::domain::country_atlas::CountryAtlas;
use crate::domain::country_standing::{rank, CountryStanding, UnlockPricing};
use crate::error::GeoDiscoveryError;

/// The ladder at a moment.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Ladder {
    pub standings:   Vec<CountryStanding>,
    pub computed_at: DateTime<Utc>,
}

pub struct CountryStandings {
    pub activity: Arc<dyn CountryActivityStore>,
    pub atlas:    &'static CountryAtlas,
    pub pricing:  UnlockPricing,
    /// Days of likes the ranking counts (30).
    pub window_days: u32,
    /// How long a computed ladder is served (60 s).
    pub cache_for:   Duration,
    cached:          Mutex<Option<(Instant, Arc<Ladder>)>>,
}

impl CountryStandings {
    pub fn new(
        activity: Arc<dyn CountryActivityStore>,
        atlas: &'static CountryAtlas,
        pricing: UnlockPricing,
        window_days: u32,
        cache_for: Duration,
    ) -> Self {
        Self { activity, atlas, pricing, window_days: window_days.max(1), cache_for, cached: Mutex::new(None) }
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

#[cfg(test)]
pub(crate) mod tests {
    use std::collections::HashMap;

    use async_trait::async_trait;
    use chrono::NaiveDate;

    use super::*;
    use crate::domain::country_standing::CountryActivity;
    use crate::domain::value_object::CountryCode;

    #[derive(Default)]
    pub struct MemActivity(pub Mutex<HashMap<(CountryCode, NaiveDate), CountryActivity>>, Mutex<std::collections::HashSet<(NaiveDate, String)>>);

    #[async_trait]
    impl CountryActivityStore for MemActivity {
        async fn add(
            &self,
            country: CountryCode,
            day: NaiveDate,
            likes: i64,
            posts: i64,
            event: &str,
        ) -> Result<(), GeoDiscoveryError> {
            if !self.1.lock().unwrap().insert((day, event.to_owned())) {
                return Ok(());
            }
            let mut map = self.0.lock().unwrap();
            let entry = map.entry((country, day)).or_default();
            entry.likes += likes;
            entry.posts += posts;
            Ok(())
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
}
