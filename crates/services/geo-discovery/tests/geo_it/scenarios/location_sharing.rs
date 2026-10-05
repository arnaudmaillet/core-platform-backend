//! Scenario — the authors' location sharing (#657) on the real map: a ghost's
//! posts leave everyone else's map (the mesh too) but not the author's own;
//! a city-level author's posts show only at the coarse band, at the cell
//! centre.

use cqrs::{Envelope, QueryBus};
use uuid::Uuid;

use geo_discovery::application::port::LocationSettingsStore;
use geo_discovery::application::query::{QueryTileQuery, QueryTileResult};
use geo_discovery::domain::value_object::{LocationAudience, LocationSharing, MapScope, Viewer};

use crate::geo_it::harness::TestHarness;

const LAT: f64 = 43.6045; // Toulouse
const LNG: f64 = 1.4440;

/// A Radar query at `zoom` around the point, as `viewer`.
async fn radar(h: &TestHarness, zoom: i32, viewer: Viewer) -> QueryTileResult {
    let d = if zoom <= 4 { 0.5 } else { 0.01 };
    h.query_bus
        .dispatch(Envelope::new(
            Uuid::now_v7(),
            QueryTileQuery {
                sw_lat: LAT - d, sw_lng: LNG - d, ne_lat: LAT + d, ne_lng: LNG + d,
                zoom_level: zoom, viewer, scope: MapScope::All,
            },
        ))
        .await
        .expect("query_tile")
}

#[tokio::test]
async fn ghosts_leave_others_maps_and_city_level_shows_only_coarsely() {
    let h = TestHarness::start().await;
    let (ghost, citizen, open) = (Uuid::now_v7(), Uuid::now_v7(), Uuid::now_v7());
    // Above the R5 band's virality floor, so all three show at every zoom.
    let ghost_post = h.index_post_by(ghost, LAT, LNG, 900.0, "", "").await;
    let city_post = h.index_post_by(citizen, LAT + 0.001, LNG + 0.001, 900.0, "", "").await;
    let open_post = h.index_post_by(open, LAT - 0.001, LNG - 0.001, 900.0, "", "").await;

    h.location.set(ghost, LocationSharing { ghost: true, ..LocationSharing::default() }).await.unwrap();
    h.location.set(citizen, LocationSharing { city: true, ..LocationSharing::default() }).await.unwrap();

    let reader = Viewer::Profiles(vec![Uuid::now_v7().to_string()]);
    let ids = |r: &QueryTileResult| r.pins.iter().map(|p| p.post_id).collect::<Vec<_>>();

    // Street level: the ghost and the city-level author are gone for others,
    // the mesh included; the ghost still sees their own post.
    for viewer in [reader.clone(), Viewer::Internal] {
        let near = radar(&h, 15, viewer).await;
        assert!(ids(&near).contains(&open_post));
        assert!(!ids(&near).contains(&ghost_post));
        assert!(!ids(&near).contains(&city_post));
    }
    let own = radar(&h, 15, Viewer::Profiles(vec![ghost.to_string()])).await;
    assert!(ids(&own).contains(&ghost_post), "the author sees their own posts");

    // The coarse band: the city-level post shows, at the cell centre.
    let far = radar(&h, 3, reader).await;
    let pin = far.pins.iter().find(|p| p.post_id == city_post).expect("shown at city level");
    assert!((pin.lat, pin.lng) != (LAT + 0.001, LNG + 0.001), "never the post's own point");
    assert!(!ids(&far).contains(&ghost_post));
}

/// #657: an author whose location audience is followers / mutuals stays on
/// the map of a reader who follows / is mutual with them, at their precise
/// point, and leaves everyone else's — strangers, anonymous readers, and the
/// mesh (it reads for no one in particular: NEARBY).
#[tokio::test]
async fn a_restricted_location_audience_keeps_only_whom_it_names() {
    let h = TestHarness::start().await;
    let (followers_only, mutuals_only) = (Uuid::now_v7(), Uuid::now_v7());
    let a = h.index_post_by(followers_only, LAT + 0.002, LNG, 900.0, "", "").await;
    let b = h.index_post_by(mutuals_only, LAT - 0.002, LNG, 900.0, "", "").await;
    let audience = |audience| LocationSharing { audience, ..LocationSharing::default() };
    h.location.set(followers_only, audience(LocationAudience::Followers)).await.unwrap();
    h.location.set(mutuals_only, audience(LocationAudience::Mutuals)).await.unwrap();

    let ids = |r: &QueryTileResult| r.pins.iter().map(|p| p.post_id).collect::<Vec<_>>();
    let reader = || Viewer::Profiles(vec![Uuid::now_v7().to_string()]);

    // A stranger, an anonymous reader, the mesh: neither.
    for viewer in [reader(), Viewer::Profiles(vec![]), Viewer::Internal] {
        let seen = ids(&radar(&h, 15, viewer).await);
        assert!(!seen.contains(&a) && !seen.contains(&b));
    }
    // A follower: the followers-only author, at its own point.
    h.gate.relate(followers_only, true, false);
    h.gate.relate(mutuals_only, true, false);
    let near = radar(&h, 15, reader()).await;
    let pin = near.pins.iter().find(|p| p.post_id == a).expect("a follower sees it");
    assert_eq!((pin.lat, pin.lng), (LAT + 0.002, LNG), "precise: not coarsened");
    assert!(!ids(&near).contains(&b), "not mutual");
    // A mutual: both.
    h.gate.relate(mutuals_only, true, true);
    let seen = ids(&radar(&h, 15, reader()).await);
    assert!(seen.contains(&a) && seen.contains(&b));
}
