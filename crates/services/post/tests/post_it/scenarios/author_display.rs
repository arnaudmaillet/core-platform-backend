//! Scenario — downloads (#809): an author who turns downloads off has their
//! posts marked so for everyone else (the mesh included), never for
//! themselves. Like counts can no longer be hidden (#973): a stored
//! `show_like_counts = false` changes nothing. The mesh learns each post's
//! author in one batch.

use cqrs::{Envelope, QueryBus};
use uuid::Uuid;

use post::application::query::like_visibility::{GetLikeVisibilityQuery, LikeVisibility};

use crate::post_it::harness::{self, ProfileId, ReuseDefaults, TestHarness, Viewer};

#[tokio::test]
async fn an_authors_downloads_apply_to_everyone_else_and_like_counts_never_hide() {
    let h = TestHarness::start().await;
    let (author, open, reader) = (harness::random_id(), harness::random_id(), harness::random_id());
    let (post_id, open_post) = (harness::random_id(), harness::random_id());
    h.create(&post_id, &author).await;
    h.publish(&post_id, &author).await;
    h.create(&open_post, &open).await;
    h.publish(&open_post, &open).await;
    let pid = |id: &str| ProfileId::try_from(id).unwrap();

    // Defaults: downloadable, counts shown.
    let seen = h.get_as(&post_id, Viewer::Profiles(vec![pid(&reader)])).await.unwrap();
    assert!(!seen.downloads_disabled());

    let closed = ReuseDefaults { allow_downloads: false, show_like_counts: false, ..ReuseDefaults::default() };
    h.reuse.set_defaults(&pid(&author), closed).await.unwrap();
    for viewer in [Viewer::Profiles(vec![pid(&reader)]), Viewer::Anonymous, Viewer::Internal] {
        let seen = h.get_as(&post_id, viewer.clone()).await.unwrap();
        assert!(seen.downloads_disabled(), "{viewer:?}");
    }
    let own = h.get_as(&post_id, Viewer::Profiles(vec![pid(&author)])).await.unwrap();
    assert!(!own.downloads_disabled(), "never for the author");

    // The batch engagement's tabs read: each post's author; unknown posts absent.
    let query = GetLikeVisibilityQuery { post_ids: vec![post_id.clone(), open_post.clone(), Uuid::now_v7().to_string()] };
    let mut found: Vec<LikeVisibility> = h.query_bus.dispatch(Envelope::new(Uuid::now_v7(), query)).await.unwrap();
    found.sort_by_key(|v| v.post_id.as_str());
    let mut expected = vec![(post_id, author), (open_post, open)];
    expected.sort();
    assert_eq!(
        found.iter().map(|v| (v.post_id.as_str(), v.author_id.as_str())).collect::<Vec<_>>(),
        expected
    );
}
