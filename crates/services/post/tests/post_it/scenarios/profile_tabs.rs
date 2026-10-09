//! Scenario — a profile's Reposts and Places tabs (#829): each lists the
//! profile's reposts or placed posts; a tab the owner hides is empty for
//! visitors, never for the owner (or the mesh).

use post::application::port::AuthorTabs;
use post::application::query::list_posts_by_profile::ProfileTab;
use post::domain::value_object::LocationSharing;

use crate::post_it::harness::{self, ProfileId, TestHarness, Viewer};

#[tokio::test]
async fn reposts_and_places_tabs_follow_the_owners_flags() {
    let h = TestHarness::start().await;
    let author_id = harness::random_id();
    let author_pid = ProfileId::try_from(author_id.as_str()).unwrap();
    let author = Viewer::Profiles(vec![author_pid.clone()]);
    let visitor = Viewer::Profiles(vec![ProfileId::try_from(harness::random_id().as_str()).unwrap()]);

    let (plain, placed, repost, original) =
        (harness::random_id(), harness::random_id(), harness::random_id(), harness::random_id());
    let other = harness::random_id();
    h.create(&original, &other).await;
    h.publish(&original, &other).await;
    h.create(&plain, &author_id).await;
    h.create_at(&placed, &author_id, 48.8566, 2.3522).await;
    h.create_repost(&repost, &author_id, &original).await;
    for post in [&plain, &placed, &repost] {
        h.publish(post, &author_id).await;
    }
    let ids = |posts: Vec<post::application::port::PostSummary>| posts.iter().map(|p| p.post_id.as_str()).collect::<Vec<_>>();

    assert_eq!(h.list_tab(&author_id, visitor.clone(), true, ProfileTab::All).await.len(), 3);
    assert_eq!(ids(h.list_tab(&author_id, visitor.clone(), true, ProfileTab::Reposts).await), vec![repost.clone()]);
    assert_eq!(ids(h.list_tab(&author_id, visitor.clone(), true, ProfileTab::Places).await), vec![placed.clone()]);
    let all = h.list_tab(&author_id, visitor.clone(), true, ProfileTab::All).await;
    assert!(all.iter().any(|p| p.is_repost) && all.iter().any(|p| p.has_place), "the summary says which is which");

    // The owner hides the Reposts tab: empty for visitors, whole for the owner.
    h.windows.set_tab_settings(&author_pid, None, AuthorTabs { show_reposts: false, show_places: true }).await.unwrap();
    assert!(h.list_tab(&author_id, visitor.clone(), true, ProfileTab::Reposts).await.is_empty());
    assert_eq!(h.list_tab(&author_id, visitor.clone(), true, ProfileTab::Places).await.len(), 1);
    assert_eq!(ids(h.list_tab(&author_id, author, true, ProfileTab::Reposts).await), vec![repost]);
    assert_eq!(h.list_tab(&author_id, Viewer::Internal, true, ProfileTab::Reposts).await.len(), 1, "the mesh reads everything");
    // Nor is the hidden tab rebuilt from the full list: reposts leave it too.
    let listed = h.list_tab(&author_id, visitor.clone(), true, ProfileTab::All).await;
    assert_eq!(listed.len(), 2);
    assert!(listed.iter().all(|p| !p.is_repost));
    assert_eq!(h.list_tab(&author_id, Viewer::Profiles(vec![author_pid.clone()]), true, ProfileTab::All).await.len(), 3);

    // Ghost mode: the Places tab, and whether a post has a place, are the
    // author's to share, like the place itself.
    h.locations.set(&author_pid, LocationSharing { ghost: true, ..LocationSharing::default() }).await.unwrap();
    assert!(h.list_tab(&author_id, visitor.clone(), true, ProfileTab::Places).await.is_empty());
    assert!(h.list_tab(&author_id, visitor, true, ProfileTab::All).await.iter().all(|p| !p.has_place));
    let own = h.list_tab(&author_id, Viewer::Profiles(vec![author_pid]), true, ProfileTab::Places).await;
    assert_eq!(own.len(), 1, "the author still sees its Places");
}
