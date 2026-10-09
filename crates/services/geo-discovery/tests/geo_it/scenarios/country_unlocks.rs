//! #665: a member's countries over the real Scylla store — the home country
//! recorded once, an unlock charged once, and the member map showing only
//! those countries (and the sea).

use chrono::Utc;
use uuid::Uuid;

use geo_discovery::application::country_unlocks::{Spender, UnlockOutcome};
use geo_discovery::domain::value_object::CountryCode;

use crate::geo_it::harness::{MapScope, TestHarness};

const PARIS: (f64, f64) = (48.8566, 2.3522);
// Inland: a coastal point may fall at sea on the 1:50m borders (shown).
const EVORA: (f64, f64) = (38.5714, -7.9135);
const BERLIN: (f64, f64) = (52.52, 13.405);

fn cc(code: &str) -> CountryCode {
    CountryCode::try_from(code).unwrap()
}

#[tokio::test]
async fn a_member_unlocks_a_country_once_and_its_map_follows() {
    let h = TestHarness::start().await;
    let me = Uuid::now_v7();
    h.residence.0.lock().unwrap().insert(me, cc("FR"));

    let countries = h.unlocking.countries(me).await.unwrap();
    assert_eq!(countries.home, Some(cc("FR")));
    // Recorded for good: a new residence does not move it.
    h.residence.0.lock().unwrap().insert(me, cc("DE"));
    assert_eq!(h.unlocking.countries(me).await.unwrap().home, Some(cc("FR")));

    let paris = h.index_post(PARIS.0, PARIS.1, 5.0).await;
    let evora = h.index_post(EVORA.0, EVORA.1, 5.0).await;
    let berlin = h.index_post(BERLIN.0, BERLIN.1, 5.0).await;
    let scope = || MapScope::Member(me);
    assert!(h.pins_near_in(PARIS.0, PARIS.1, scope()).await.contains(&paris), "home");
    assert!(!h.pins_near_in(EVORA.0, EVORA.1, scope()).await.contains(&evora), "locked");

    // Portugal is quiet on this ladder: 15 gems.
    let price = h.unlocking.unlock(me, "PT", 50, &Spender { adult: true, token: None }, Utc::now()).await.unwrap();
    assert_eq!(price.outcome, UnlockOutcome::PriceChanged);
    let unlocked = h.unlocking.unlock(me, "PT", price.price, &Spender { adult: true, token: None }, Utc::now()).await.unwrap();
    assert_eq!(unlocked.outcome, UnlockOutcome::Unlocked);
    let again = h.unlocking.unlock(me, "PT", price.price, &Spender { adult: true, token: None }, Utc::now()).await.unwrap();
    assert_eq!(again.outcome, UnlockOutcome::AlreadyUnlocked);
    assert_eq!(h.wallet.spent.lock().unwrap().iter().filter(|(a, _, _)| *a == me).count(), 1, "charged once");

    assert!(h.pins_near_in(EVORA.0, EVORA.1, scope()).await.contains(&evora), "unlocked");
    assert_eq!(h.cards_in(&[paris, evora, berlin], scope()).await, [paris, evora].into_iter().collect());
    // Everyone else's map is unchanged.
    assert!(h.pins_near_in(BERLIN.0, BERLIN.1, MapScope::All).await.contains(&berlin));
}

/// The pending row over Scylla: written before the spend, read back with its
/// agreed price, settled by the unlock, dropped when unpaid — and never on
/// the map.
#[tokio::test]
async fn a_pending_purchase_is_kept_settled_or_dropped() {
    let h = TestHarness::start().await;
    let store = &h.unlocking.store;
    let me = Uuid::now_v7();
    store.mark_pending(me, cc("PT"), 15).await.unwrap();
    store.mark_pending(me, cc("ES"), 30).await.unwrap();
    let countries = store.get(me).await.unwrap();
    assert!(countries.unlocked.is_empty(), "pending is not unlocked");
    assert_eq!(countries.pending.len(), 2);

    store.add(me, cc("PT"), 15, Utc::now()).await.unwrap();
    store.clear_pending(me, cc("ES")).await.unwrap();
    // Clearing never undoes an unlock.
    store.clear_pending(me, cc("PT")).await.unwrap();
    let countries = store.get(me).await.unwrap();
    assert_eq!((countries.unlocked, countries.pending), (vec![cc("PT")], vec![]));
}
