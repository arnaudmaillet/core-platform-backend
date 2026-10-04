//! Scenario — H3 viewport indexing and spatial filtering.
//!
//! Indexing a post encodes its coordinate into H3 cells (R5/R7/R9) and writes the
//! spatial index + card store. A viewport query resolves the covering cells for
//! the requested zoom and returns the cards within them. The invariant: a viewport
//! that *covers* a post returns it, and a viewport over a *distant* region does
//! not. This is the spatial-partitioning axis.

use crate::geo_it::harness::{self, TestHarness, DEADLINE, ZOOM_R9};

// San Francisco — the indexing location and the covering viewport.
const SF_LAT: f64 = 37.7749;
const SF_LNG: f64 = -122.4194;
const SF_SW_LAT: f64 = 37.770;
const SF_SW_LNG: f64 = -122.422;
const SF_NE_LAT: f64 = 37.780;
const SF_NE_LNG: f64 = -122.417;

// New York — a distant, non-overlapping viewport.
const NY_SW_LAT: f64 = 40.700;
const NY_SW_LNG: f64 = -74.020;
const NY_NE_LAT: f64 = 40.730;
const NY_NE_LNG: f64 = -74.000;

#[tokio::test]
async fn viewport_returns_posts_indexed_within_it() {
    let h = TestHarness::start().await;

    let a = h.index_post(SF_LAT, SF_LNG, 100.0).await;
    let b = h.index_post(SF_LAT, SF_LNG, 250.0).await;

    harness::await_until("both indexed posts appear in the covering viewport", DEADLINE, || {
        let h = &h;
        async move {
            let result = h
                .query_viewport(SF_SW_LAT, SF_SW_LNG, SF_NE_LAT, SF_NE_LNG, ZOOM_R9)
                .await;
            harness::result_contains(&result, &a) && harness::result_contains(&result, &b)
        }
    })
    .await;
}

#[tokio::test]
async fn viewport_excludes_posts_outside_it() {
    let h = TestHarness::start().await;

    let sf_post = h.index_post(SF_LAT, SF_LNG, 100.0).await;

    // Confirm it is indexed and visible in its own viewport first…
    harness::await_until("SF post visible in the SF viewport", DEADLINE, || {
        let h = &h;
        async move {
            let result = h
                .query_viewport(SF_SW_LAT, SF_SW_LNG, SF_NE_LAT, SF_NE_LNG, ZOOM_R9)
                .await;
            harness::result_contains(&result, &sf_post)
        }
    })
    .await;

    // …then assert a distant viewport does not return it.
    let distant = h
        .query_viewport(NY_SW_LAT, NY_SW_LNG, NY_NE_LAT, NY_NE_LNG, ZOOM_R9)
        .await;
    assert!(
        !harness::result_contains(&distant, &sf_post),
        "a viewport over New York must not return a post indexed in San Francisco",
    );
}

/// Regression: below zoom 13 the band has a virality floor (R7: 50 / 5, R5:
/// 500), sent to Redis as a score bound. fred took the old numeric *string* for
/// a LEX bound and Redis refused every such query ("min or max is not a
/// float") — the map only ever worked at street level. Each band now answers,
/// and the floor still filters.
#[tokio::test]
async fn every_zoom_band_answers_and_applies_its_virality_floor() {
    let h = TestHarness::start().await;
    let (lat, lng) = (45.7640, 4.8357); // Lyon
    let viral = h.index_post(lat, lng, 900.0).await;
    let quiet = h.index_post(lat + 0.001, lng + 0.001, 10.0).await;

    for (zoom, d, quiet_shows) in [(3, 0.5, false), (7, 0.1, false), (10, 0.05, true), (15, 0.01, true)] {
        let result = h.query_viewport(lat - d, lng - d, lat + d, lng + d, zoom).await;
        assert!(harness::result_contains(&result, &viral), "zoom {zoom}: a viral post shows");
        assert_eq!(harness::result_contains(&result, &quiet), quiet_shows, "zoom {zoom}: the floor");
    }
}
