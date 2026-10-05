//! Scenario — the author's location sharing (#657) on `GetPost`.
//!
//! The author always reads the post's own point. Everyone else (another
//! member, an anonymous client, the mesh) reads it as the author shares it:
//! precise by default, the centre of the post's city cell at city level, and
//! nothing in ghost mode. The setting applies to posts made before it changed.

use crate::post_it::harness::{self, LocationSharing, ProfileId, TestHarness, Viewer};

const PARIS: (f64, f64) = (48.8566, 2.3522);

#[tokio::test]
async fn others_read_a_post_location_as_its_author_shares_it() {
    let h = TestHarness::start().await;

    let author_id = harness::random_id();
    let author_pid = ProfileId::try_from(author_id.as_str()).unwrap();
    let post_id = harness::random_id();
    let author = Viewer::Profiles(vec![author_pid.clone()]);
    let stranger = Viewer::Profiles(vec![ProfileId::try_from(harness::random_id().as_str()).unwrap()]);

    h.create_at(&post_id, &author_id, PARIS.0, PARIS.1).await;
    h.publish(&post_id, &author_id).await;
    let location = |post: harness::Post| post.location().map(|g| (g.lat(), g.lng()));

    // Default: precise for everyone.
    assert_eq!(location(h.get_as(&post_id, stranger.clone()).await.unwrap()), Some(PARIS));

    // City level: one coarse point, never the post's own.
    h.locations.set(&author_pid, LocationSharing { city: true, ..LocationSharing::default() }).await.unwrap();
    for viewer in [stranger.clone(), Viewer::Anonymous, Viewer::Internal] {
        let seen = location(h.get_as(&post_id, viewer).await.unwrap()).expect("a city point");
        assert_ne!(seen, PARIS);
        assert!((seen.0 - PARIS.0).abs() < 0.5 && (seen.1 - PARIS.1).abs() < 0.5, "{seen:?}");
    }

    // Ghost: no location at all for anyone else.
    h.locations.set(&author_pid, LocationSharing { ghost: true, city: true, ..LocationSharing::default() }).await.unwrap();
    for viewer in [stranger.clone(), Viewer::Anonymous, Viewer::Internal] {
        assert_eq!(location(h.get_as(&post_id, viewer).await.unwrap()), None);
    }

    // The author keeps reading their own point.
    assert_eq!(location(h.get_as(&post_id, author).await.unwrap()), Some(PARIS));
}

/// #657: an author's location audience. A follower (followers) or a mutual
/// (mutuals) reads the post's location; a stranger, an anonymous client and
/// the mesh read the post without it; the author always reads it.
#[tokio::test]
async fn the_location_audience_hides_the_place_from_everyone_else() {
    use post::domain::value_object::LocationAudience;

    let h = TestHarness::start().await;
    let author_id = harness::random_id();
    let author_pid = ProfileId::try_from(author_id.as_str()).unwrap();
    let post_id = harness::random_id();
    h.create_at(&post_id, &author_id, PARIS.0, PARIS.1).await;
    h.publish(&post_id, &author_id).await;
    let location = |post: harness::Post| post.location().map(|g| (g.lat(), g.lng()));
    let reader = || Viewer::Profiles(vec![ProfileId::try_from(harness::random_id().as_str()).unwrap()]);

    let audience = |audience| LocationSharing { audience, ..LocationSharing::default() };
    h.locations.set(&author_pid, audience(LocationAudience::Followers)).await.unwrap();
    for viewer in [reader(), Viewer::Anonymous, Viewer::Internal] {
        let post = h.get_as(&post_id, viewer).await.expect("the post itself stays visible");
        assert_eq!(location(post), None, "no place outside the audience");
    }
    h.gate.relate(&author_id, true, false);
    assert_eq!(location(h.get_as(&post_id, reader()).await.unwrap()), Some(PARIS), "a follower");

    h.locations.set(&author_pid, audience(LocationAudience::Mutuals)).await.unwrap();
    assert_eq!(location(h.get_as(&post_id, reader()).await.unwrap()), None, "a follower, not mutual");
    h.gate.relate(&author_id, true, true);
    assert_eq!(location(h.get_as(&post_id, reader()).await.unwrap()), Some(PARIS), "a mutual");

    let author = Viewer::Profiles(vec![author_pid]);
    assert_eq!(location(h.get_as(&post_id, author).await.unwrap()), Some(PARIS), "the author");
}
