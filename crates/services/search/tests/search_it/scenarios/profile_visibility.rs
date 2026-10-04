//! Profile indexing + the dual visibility-authority model against the live engine:
//! moderation and owner masking are independent fields, so neither authority can
//! lift the other's hide. (Drives fully-formed `SourceEvent`s; gRPC hydration of
//! thin `profile.v1.events` is unit-tested.)

use search::domain::EntityKind;

use crate::search_it::harness::Harness;

#[tokio::test]
async fn profile_is_indexed_and_searchable() {
    let h = Harness::start().await;
    h.index_profile("prof-1", "alice", "Alice", "rust engineer", 1)
        .await;
    assert_eq!(h.search_ids("alice").await, vec!["prof-1".to_owned()]);
    assert_eq!(h.search_ids("rust").await, vec!["prof-1".to_owned()]);
}

#[tokio::test]
async fn owner_restore_cannot_lift_a_moderation_hide() {
    let h = Harness::start().await;
    h.index_profile("prof-1", "alice", "Alice", "rust engineer", 1)
        .await;

    h.hide(EntityKind::Profile, "prof-1", 100).await;
    h.refresh().await;
    assert!(h.search("alice").await.hits.is_empty());

    // The owner toggling their own visibility must not reveal a moderated profile.
    h.owner_restore("prof-1", 200).await;
    h.refresh().await;
    assert!(
        h.search("alice").await.hits.is_empty(),
        "owner restore must not lift a moderation hide"
    );

    // Only moderation lifting its own hide makes it searchable again.
    h.restore(EntityKind::Profile, "prof-1", 300).await;
    assert_eq!(h.search_ids("alice").await, vec!["prof-1".to_owned()]);
}

#[tokio::test]
async fn moderation_restore_cannot_lift_an_owner_mask() {
    let h = Harness::start().await;
    h.index_profile("prof-1", "alice", "Alice", "rust engineer", 1)
        .await;

    h.owner_hide("prof-1", 100).await;
    h.refresh().await;
    assert!(h.search("alice").await.hits.is_empty());

    h.restore(EntityKind::Profile, "prof-1", 200).await;
    h.refresh().await;
    assert!(
        h.search("alice").await.hits.is_empty(),
        "a moderation restore must not lift an owner mask"
    );

    h.owner_restore("prof-1", 300).await;
    assert_eq!(h.search_ids("alice").await, vec!["prof-1".to_owned()]);
}

#[tokio::test]
async fn owner_mask_survives_a_profile_content_edit() {
    let h = Harness::start().await;
    h.index_profile("prof-1", "alice", "Alice", "first bio", 1)
        .await;
    h.owner_hide("prof-1", 100).await;
    h.refresh().await;
    assert!(h.search("alice").await.hits.is_empty());

    // A newer profile edit (re-hydrated upsert) must not un-mask it.
    h.index_profile("prof-1", "alice", "Alice", "edited bio", 2)
        .await;
    h.refresh().await;
    assert!(
        h.search("edited").await.hits.is_empty(),
        "a content re-projection must preserve the owner mask"
    );

    h.owner_restore("prof-1", 200).await;
    assert_eq!(h.search_ids("edited").await, vec!["prof-1".to_owned()]);
}

#[tokio::test]
async fn a_profile_not_findable_by_search_leaves_it_and_masking_cannot_undo_that() {
    let h = Harness::start().await;
    h.index_profile("prof-1", "alice", "Alice", "rust engineer", 1)
        .await;

    h.discoverable("prof-1", false, 100).await;
    h.refresh().await;
    assert!(h.search("alice").await.hits.is_empty());

    // Restoring after a mask is the owner authority: it leaves the choice alone.
    h.owner_hide("prof-1", 200).await;
    h.owner_restore("prof-1", 300).await;
    h.refresh().await;
    assert!(h.search("alice").await.hits.is_empty(), "masking must not undo the setting");

    // A stale replay of an older "findable" is rejected by the version guard.
    h.discoverable("prof-1", true, 50).await;
    h.refresh().await;
    assert!(h.search("alice").await.hits.is_empty(), "stale event");

    h.discoverable("prof-1", true, 400).await;
    assert_eq!(h.search_ids("alice").await, vec!["prof-1".to_owned()]);
}
