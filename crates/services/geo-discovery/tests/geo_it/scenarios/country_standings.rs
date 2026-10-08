//! #665: the country ladder over the real Redis activity store — likes and
//! posts per UTC day, summed over the window, every country ranked.

use std::sync::Arc;
use std::time::Duration;

use chrono::{TimeDelta, Utc};

use geo_discovery::application::country_standings::CountryStandings;
use geo_discovery::domain::country_atlas::CountryAtlas;
use geo_discovery::domain::country_standing::UnlockPricing;
use geo_discovery::domain::value_object::CountryCode;

use crate::geo_it::harness::TestHarness;

fn cc(code: &str) -> CountryCode {
    CountryCode::try_from(code).unwrap()
}

#[tokio::test]
async fn likes_and_posts_add_up_once_per_event_per_country_over_the_window() {
    let h = TestHarness::start().await;
    let activity = Arc::clone(&h.standings.activity);
    // Far-past days, so other runs' "today" never mixes in: the window is
    // counted back from `now`.
    let now = Utc::now() - TimeDelta::days(400 + i64::from(rand_day()));
    let today = now.date_naive();
    let id = |name: &str| format!("{name}-{}", uuid::Uuid::now_v7());
    let nz = id("nz");
    activity.add(cc("NZ"), today, 7, 2, &nz).await.unwrap();
    // A redelivery of the same event counts nothing more.
    activity.add(cc("NZ"), today, 7, 2, &nz).await.unwrap();
    activity.add(cc("NZ"), today - TimeDelta::days(3), 1, 0, &id("nz-old")).await.unwrap();
    activity.add(cc("IS"), today, 5, 1, &id("is")).await.unwrap();
    activity.add(cc("IS"), today, -1, 0, &id("is-removed")).await.unwrap();
    // Outside a 30-day window.
    activity.add(cc("CL"), today - TimeDelta::days(30), 99, 1, &id("cl")).await.unwrap();

    let standings = CountryStandings::new(activity, CountryAtlas::embedded(), UnlockPricing::default(), 30, Duration::ZERO);
    let ladder = standings.ladder(now).await.unwrap();
    let top: Vec<_> = ladder.standings.iter().take(2).map(|s| (s.country, s.rank, s.likes, s.posts)).collect();
    assert_eq!(top, vec![(cc("NZ"), 1, 8, 2), (cc("IS"), 2, 4, 1)]);
    let cl = ladder.standings.iter().find(|s| s.country == cc("CL")).unwrap();
    assert_eq!(cl.likes, 0, "outside the window");
    assert_eq!(ladder.standings.len(), CountryAtlas::embedded().codes().count());
}

/// A day offset per run (0–999), so reruns against a kept container start clean.
fn rand_day() -> u32 {
    (uuid::Uuid::now_v7().as_u128() % 1000) as u32
}
