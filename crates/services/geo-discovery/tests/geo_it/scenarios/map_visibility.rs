//! Scenario — what the map may still show, over the real Scylla + Redis.
//!
//! A deleted post leaves the Radar and Focus paths for good; a moderated one
//! leaves until a newer reversal brings its pin back; a reader only sees posts
//! of authors it may see, and an audience-check outage fails closed.

use uuid::Uuid;

use crate::geo_it::harness::{ContentAccess, TestHarness, Viewer, VisibilityChange};

const LAT: f64 = 48.8566;
const LNG: f64 = 2.3522;

fn reader() -> Viewer {
    Viewer::Profiles(vec![Uuid::now_v7().to_string()])
}

#[tokio::test]
async fn deleted_and_moderated_posts_leave_the_map_and_a_reversal_restores() {
    let h = TestHarness::start().await;
    let deleted = h.index_post_full(LAT, LNG, 5.0, "bye", "").await;
    let moderated = h.index_post_full(LAT, LNG, 5.0, "hmm", "").await;
    let near = || h.pins_near_as(LAT, LNG, Viewer::Internal);
    assert!(near().await.contains(&deleted) && near().await.contains(&moderated));

    // Deleted: gone from Radar and Focus, even for the mesh, and for good.
    h.change_visibility(deleted, VisibilityChange::Deleted).await;
    assert!(!near().await.contains(&deleted));
    assert!(h.get_timeline(&[deleted]).await.cards.is_empty());
    h.change_visibility(deleted, VisibilityChange::Moderation { restricted: false, version: 9 }).await;
    assert!(!near().await.contains(&deleted), "a deleted post never comes back");

    // Moderated (v1): gone; a stale reversal (v0) changes nothing; v2 restores.
    h.change_visibility(moderated, VisibilityChange::Moderation { restricted: true, version: 1 }).await;
    assert!(!near().await.contains(&moderated));
    assert!(h.get_timeline(&[moderated]).await.cards.is_empty());
    h.change_visibility(moderated, VisibilityChange::Moderation { restricted: false, version: 0 }).await;
    assert!(!near().await.contains(&moderated));
    h.change_visibility(moderated, VisibilityChange::Moderation { restricted: false, version: 2 }).await;
    assert!(near().await.contains(&moderated), "the pin is rebuilt from the card");
    assert_eq!(h.get_timeline(&[moderated]).await.cards.len(), 1);

    // An unknown post (no location, or expired) is a no-op.
    h.change_visibility(Uuid::now_v7(), VisibilityChange::Deleted).await;
}

#[tokio::test]
async fn a_reader_only_sees_posts_of_authors_it_may_see() {
    let h = TestHarness::start().await;
    let (open, private, blocked) = (Uuid::now_v7(), Uuid::now_v7(), Uuid::now_v7());
    let from_open = h.index_post_by(open, LAT, LNG, 5.0, "", "").await;
    let from_private = h.index_post_by(private, LAT, LNG, 5.0, "", "").await;
    let from_blocked = h.index_post_by(blocked, LAT, LNG, 5.0, "", "").await;
    h.gate.set(private, ContentAccess::HeaderOnly);
    h.gate.set(blocked, ContentAccess::Hidden);

    for viewer in [reader(), Viewer::Profiles(vec![])] {
        let pins = h.pins_near_as(LAT, LNG, viewer.clone()).await;
        assert!(pins.contains(&from_open));
        assert!(!pins.contains(&from_private) && !pins.contains(&from_blocked));
        let cards = h.try_get_timeline_as(&[from_open, from_private, from_blocked], viewer).await.unwrap();
        assert_eq!(cards.cards.iter().map(|c| c.post_id).collect::<Vec<_>>(), vec![from_open]);
    }
    // One's own posts, and the mesh, are unfiltered.
    let own = h.pins_near_as(LAT, LNG, Viewer::Profiles(vec![blocked.to_string()])).await;
    assert!(own.contains(&from_blocked));
    assert!(h.pins_near_as(LAT, LNG, Viewer::Internal).await.contains(&from_private));

    // Outage: a client read fails closed; the mesh still reads.
    h.gate.set_down(true);
    assert!(h.try_get_timeline_as(&[from_open], reader()).await.is_err());
    assert_eq!(h.get_timeline(&[from_open]).await.cards.len(), 1);
}

#[tokio::test]
async fn a_visibility_event_before_the_index_event_still_wins() {
    let h = TestHarness::start().await;
    let near = || h.pins_near_as(LAT, LNG, Viewer::Internal);

    // Deleted before post.published reached the index consumer.
    let deleted = Uuid::now_v7();
    h.change_visibility(deleted, VisibilityChange::Deleted).await;
    h.index_post_with_id(deleted, LAT, LNG).await;
    assert!(!near().await.contains(&deleted));
    assert!(h.get_timeline(&[deleted]).await.cards.is_empty());

    // Taken down first, then indexed (twice: a redelivery), then reversed.
    let moderated = Uuid::now_v7();
    h.change_visibility(moderated, VisibilityChange::Moderation { restricted: true, version: 1 }).await;
    h.index_post_with_id(moderated, LAT, LNG).await;
    h.index_post_with_id(moderated, LAT, LNG).await;
    assert!(!near().await.contains(&moderated));
    assert!(h.get_timeline(&[moderated]).await.cards.is_empty());
    h.change_visibility(moderated, VisibilityChange::Moderation { restricted: false, version: 2 }).await;
    assert!(near().await.contains(&moderated), "restored: pin + spatial index rebuilt");
    assert_eq!(h.get_timeline(&[moderated]).await.cards.len(), 1);
}
