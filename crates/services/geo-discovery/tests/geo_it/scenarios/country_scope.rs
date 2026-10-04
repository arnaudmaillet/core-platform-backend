//! Scenario — a guest session's map is limited to the country granted from its
//! location: nothing before a grant, only that country after, and nothing again
//! once location is off. Members (and the mesh) are not limited.

use std::collections::HashSet;

use geo_discovery::domain::value_object::CountryAccessOutcome;

use crate::geo_it::harness::{MapScope, TestHarness};

const PARIS: (f64, f64) = (48.8566, 2.3522);
const MADRID: (f64, f64) = (40.4168, -3.7038);

#[tokio::test]
async fn a_guest_sees_only_its_granted_country() {
    let h = TestHarness::start().await;
    let paris = h.index_post(PARIS.0, PARIS.1, 50.0).await;
    let madrid = h.index_post(MADRID.0, MADRID.1, 50.0).await;
    let guest = format!("guest:{}", uuid::Uuid::now_v7());
    let scope = MapScope::Guest(guest.clone());
    // The harness's GeoIP takes a private address at the device's word.
    let lan = Some("10.0.0.7".parse().unwrap());

    // No grant yet: nothing (and the guest's limit holds whoever else posted).
    assert!(h.pins_near_in(PARIS.0, PARIS.1, scope.clone()).await.is_empty());

    let outcome = h.country_access.handle(&guest, Some("FR"), lan).await.expect("grant");
    assert!(matches!(outcome, CountryAccessOutcome::Granted(c) if c.as_str() == "FR"));
    // Other scenarios index around Paris too (shared Redis): look for ours.
    assert!(h.pins_near_in(PARIS.0, PARIS.1, scope.clone()).await.contains(&paris));
    assert!(h.pins_near_in(MADRID.0, MADRID.1, scope.clone()).await.is_empty());
    assert_eq!(h.cards_in(&[paris, madrid], scope.clone()).await, HashSet::from([paris]));

    // Members are not limited.
    assert!(h.pins_near_in(MADRID.0, MADRID.1, MapScope::All).await.contains(&madrid));
    assert_eq!(h.cards_in(&[paris, madrid], MapScope::All).await, HashSet::from([paris, madrid]));

    // Travelling: Spain opens, France locks again.
    h.country_access.handle(&guest, Some("ES"), lan).await.expect("grant");
    assert!(h.pins_near_in(PARIS.0, PARIS.1, scope.clone()).await.is_empty());
    assert!(h.pins_near_in(MADRID.0, MADRID.1, scope.clone()).await.contains(&madrid));

    // Location off: nothing.
    let outcome = h.country_access.handle(&guest, None, lan).await.expect("clear");
    assert_eq!(outcome, CountryAccessOutcome::NotSent);
    assert!(h.pins_near_in(MADRID.0, MADRID.1, scope.clone()).await.is_empty());
    assert!(h.cards_in(&[paris, madrid], scope).await.is_empty());
}
