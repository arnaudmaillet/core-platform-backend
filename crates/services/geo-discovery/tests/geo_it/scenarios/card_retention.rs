//! Scenario — a card leaves ScyllaDB when its retention ends, whatever wrote it.
//!
//! A score update rewrites one cell of `map_post_cards`; it must carry the
//! card's remaining TTL, or that cell outlives the row and leaves a score-only
//! row behind forever.

use uuid::Uuid;

use geo_discovery::application::port::TileRepository;
use geo_discovery::domain::value_object::PostId;

use crate::geo_it::harness::{await_until, TestHarness, DEADLINE};

const LAT: f64 = 48.8566;
const LNG: f64 = 2.3522;

#[tokio::test]
async fn a_rescored_card_still_expires_with_its_retention() {
    let h = TestHarness::start().await;
    let post = h.index_post_retained(LAT, LNG, 3).await;
    let id = PostId::from(post);

    h.update_score(post, LAT, LNG, 42.0).await;
    let card = h.tiles.get_card(&id).await.expect("get_card").expect("card is live");
    assert_eq!(card.virality_score, 42.0, "the score update landed");

    // Past the retention the whole row is gone — not a score-only remnant.
    await_until("the rescored card expired", DEADLINE, || async {
        matches!(h.tiles.get_card_with_visibility(&id).await, Ok(None))
    })
    .await;
    assert!(h.tiles.get_card(&id).await.expect("get_card on an expired card").is_none());

    // A score for a card that no longer exists (or never did) creates no row.
    h.update_score(post, LAT, LNG, 7.0).await;
    let unknown = Uuid::now_v7();
    h.update_score(unknown, LAT, LNG, 7.0).await;
    assert!(h.tiles.get_card_with_visibility(&id).await.expect("read").is_none());
    assert!(h.tiles.get_card_with_visibility(&PostId::from(unknown)).await.expect("read").is_none());
}
