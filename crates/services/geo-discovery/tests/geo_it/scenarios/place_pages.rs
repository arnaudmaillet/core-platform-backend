//! #833 over live Scylla + Redis: a place's posts page by page — a cell (as
//! a city marker names it by its centre) or a whole country — newest first or
//! by popularity, past the map's per-tile Top-K, with GetGeoTimeline's rules.

use geo_discovery::application::query::list_place_posts::{PlaceSort, POPULAR_WINDOW};
use geo_discovery::domain::value_object::{CountryCode, H3Resolution, Place};
use uuid::Uuid;

use crate::geo_it::harness::{MapScope, TestHarness};

/// A spot of its own per test (shared containers): somewhere in northern
/// Siberia, offset by a random street-sized step.
fn own_spot() -> (f64, f64) {
    let jitter = (Uuid::now_v7().as_u128() % 10_000) as f64 / 10_000.0;
    (71.0 + jitter, 120.0 + jitter)
}

fn now_ms() -> i64 {
    chrono::Utc::now().timestamp_millis()
}

#[tokio::test]
async fn a_cells_posts_newest_first_page_by_page() {
    let h = TestHarness::start().await;
    let (lat, lng) = own_spot();
    let now = now_ms();
    // r[0] oldest … r[4] newest; one of them on the same millisecond as another.
    let mut r = Vec::new();
    for at in [now - 5_000, now - 4_000, now - 3_000, now - 3_000, now - 1_000] {
        r.push(h.index_post_scored_at(lat, lng, 1.0, at).await);
    }
    let place = Place::around(lat, lng, H3Resolution::R9).unwrap();

    let paged = h.all_place_posts(place, PlaceSort::Recent, 2).await;
    assert_eq!(paged.len(), 5, "every post once: {paged:?}");
    assert_eq!(paged[0], r[4]);
    assert_eq!(paged[4], r[0]);
    // The same millisecond across a page boundary: both, once each (ids
    // ascend within a millisecond).
    assert_eq!(paged[1..3], [r[2], r[3]]);
    assert_eq!(paged[3], r[1]);

    // The same cell by its raw index, and its city (R5) too.
    let Place::Cell { index, .. } = place else { unreachable!() };
    assert_eq!(h.all_place_posts(Place::cell(index.as_i64()).unwrap(), PlaceSort::Recent, 10).await, paged);
    let city = h.all_place_posts(Place::around(lat, lng, H3Resolution::R5).unwrap(), PlaceSort::Recent, 50).await;
    assert!(r.iter().all(|post| city.contains(post)));
}

#[tokio::test]
async fn popular_ranks_by_virality_then_the_newest() {
    let h = TestHarness::start().await;
    let (lat, lng) = own_spot();
    let now = now_ms();
    let low = h.index_post_scored_at(lat, lng, 1.0, now - 4_000).await;
    let viral = h.index_post_scored_at(lat, lng, 90.0, now - 3_000).await;
    let older_mid = h.index_post_scored_at(lat, lng, 20.0, now - 2_000).await;
    let newer_mid = h.index_post_scored_at(lat, lng, 20.0, now - 1_000).await;
    let place = Place::around(lat, lng, H3Resolution::R9).unwrap();

    let first = h.place_page(place, PlaceSort::Popular, 2, None, MapScope::All).await;
    assert_eq!(first.cards.iter().map(|c| c.post_id).collect::<Vec<_>>(), vec![viral, newer_mid]);
    assert_eq!(h.all_place_posts(place, PlaceSort::Popular, 2).await, vec![viral, newer_mid, older_mid, low]);
}

#[tokio::test]
async fn popular_carries_on_past_its_window_newest_first() {
    let h = TestHarness::start().await;
    let (lat, lng) = own_spot();
    let now = now_ms();
    let total = POPULAR_WINDOW as i64 + 2;
    let mut r = Vec::new();
    for i in 0..total {
        // The oldest is the most viral: past the window, it does not rank.
        let score = if i == 0 { 500.0 } else { 1.0 };
        r.push(h.index_post_scored_at(lat, lng, score, now - (total - i) * 10).await);
    }
    let place = Place::around(lat, lng, H3Resolution::R9).unwrap();

    let paged = h.all_place_posts(place, PlaceSort::Popular, 50).await;
    assert_eq!(paged.len(), total as usize, "every post once");
    assert_eq!(paged[total as usize - 2..], [r[1], r[0]], "the older ones last, newest first");
}

#[tokio::test]
async fn a_countrys_posts_and_its_rules() {
    let h = TestHarness::start().await;
    // Iceland: no other scenario posts there.
    let now = now_ms();
    // Inland points: coastlines are simplified in the atlas.
    let reykjavik = h.index_post_scored_at(64.0, -20.0, 1.0, now - 2_000).await;
    let akureyri = h.index_post_scored_at(65.2, -18.0, 1.0, now - 1_000).await;
    let elsewhere = h.index_post_scored_at(59.9139, 10.7522, 1.0, now).await; // Oslo
    let iceland = Place::Country(CountryCode::try_from("IS").unwrap());

    let posts = h.all_place_posts(iceland, PlaceSort::Recent, 1).await;
    let mine: Vec<Uuid> = posts.into_iter().filter(|p| [reykjavik, akureyri, elsewhere].contains(p)).collect();
    assert_eq!(mine, vec![akureyri, reykjavik], "Iceland's, newest first; Norway's elsewhere");

    // A guest without a granted country sees nothing, like the rest of the map.
    let guest = MapScope::Guest(format!("guest:{}", Uuid::now_v7()));
    let page = h.place_page(iceland, PlaceSort::Recent, 1, None, guest).await;
    assert!(page.cards.is_empty());
    assert_eq!(page.next_page_token, None, "and nothing to page through");
}
