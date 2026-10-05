//! Scenario — the author's post history window (#664): older posts disappear
//! for everyone but the author (and the mesh), in the list and on `GetPost`,
//! without being deleted; widening the window brings them back.

use crate::post_it::harness::{self, ProfileId, TestHarness, Viewer};

#[tokio::test]
async fn posts_older_than_the_window_are_hidden_from_visitors_only() {
    let h = TestHarness::start().await;
    let author_id = harness::random_id();
    let author_pid = ProfileId::try_from(author_id.as_str()).unwrap();
    let author = Viewer::Profiles(vec![author_pid.clone()]);
    let visitor = Viewer::Profiles(vec![ProfileId::try_from(harness::random_id().as_str()).unwrap()]);

    let (recent, month_old, year_old) = (harness::random_id(), harness::random_id(), harness::random_id());
    h.seed_published(&recent, &author_id, 1).await;
    h.seed_published(&month_old, &author_id, 40).await;
    h.seed_published(&year_old, &author_id, 400).await;
    assert_eq!(h.list_as(&author_id, visitor.clone()).await.len(), 3, "no window: everything");

    // One month: only the recent post for a visitor.
    h.windows.set(&author_pid, Some(30)).await.unwrap();
    let seen: Vec<String> = h.list_as(&author_id, visitor.clone()).await.iter().map(|p| p.post_id.as_str()).collect();
    assert_eq!(seen, vec![recent.clone()]);
    assert!(h.get_as(&month_old, visitor.clone()).await.is_err());
    assert!(h.get_as(&recent, visitor.clone()).await.is_ok());

    // The author and the mesh still see everything: hidden, not deleted.
    assert_eq!(h.list_as(&author_id, author.clone()).await.len(), 3);
    let as_author = h.get_as(&year_old, author).await.expect("the author's own");
    assert!(!as_author.outside_window(), "nothing is withheld from the author");
    assert_eq!(h.list(&author_id).await.len(), 3);
    // The mesh reads it marked, so comment and search withhold it from clients.
    let marked = h.get(&month_old).await.expect("mesh read");
    assert!(marked.outside_window());
    assert_eq!(marked.visible_until(), Some(marked.created_at() + chrono::Duration::days(30)));
    let fresh = h.get(&recent).await.expect("mesh read");
    assert!(!fresh.outside_window());
    assert_eq!(fresh.visible_until(), Some(fresh.created_at() + chrono::Duration::days(30)), "search withholds it later");

    // Six months brings the month-old post back; all posts brings everything.
    h.windows.set(&author_pid, Some(183)).await.unwrap();
    assert_eq!(h.list_as(&author_id, visitor.clone()).await.len(), 2);
    h.windows.set(&author_pid, None).await.unwrap();
    assert_eq!(h.list_as(&author_id, visitor).await.len(), 3);
    assert_eq!(h.get(&year_old).await.unwrap().visible_until(), None, "no window, no end");
}
