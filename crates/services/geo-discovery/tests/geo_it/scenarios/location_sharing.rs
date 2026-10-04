//! Scenario — the authors' location sharing (#657) on the real map: a ghost's
//! posts leave everyone else's map (the mesh too) but not the author's own;
//! a city-level author's posts show only at the coarse band, at the cell
//! centre.

use cqrs::{Envelope, QueryBus};
use uuid::Uuid;

use geo_discovery::application::port::LocationSettingsStore;
use geo_discovery::application::query::{QueryTileQuery, QueryTileResult};
use geo_discovery::domain::value_object::{LocationSharing, MapScope, Viewer};

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

    h.location.set(ghost, LocationSharing { ghost: true, city: false }).await.unwrap();
    h.location.set(citizen, LocationSharing { ghost: false, city: true }).await.unwrap();

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
