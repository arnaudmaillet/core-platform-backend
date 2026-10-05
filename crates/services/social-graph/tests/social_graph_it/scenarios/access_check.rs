//! Scenario — the access check over the real tables (follow_status, blocks and
//! the profile_audience projection), and the follower lists that apply it.

use social_graph::application::command::AudienceFact;

use crate::social_graph_it::harness::{self, ContentAccess, TestHarness, Viewer};

#[tokio::test]
async fn private_profiles_open_to_followers_and_blocks_close_everything() {
    let h = TestHarness::start().await;
    let (owner, follower, stranger, fan) =
        (harness::random_profile(), harness::random_profile(), harness::random_profile(), harness::random_profile());
    h.follow(&follower, &owner).await;
    h.follow(&fan, &owner).await;

    // Public by default (no projection row yet).
    assert_eq!(h.access(&[stranger], &owner).await, ContentAccess::Visible);
    assert_eq!(h.access(&[], &owner).await, ContentAccess::Visible, "anonymous");

    // Private: followers only; the header for everyone else.
    h.audience(&owner, AudienceFact::Private(true)).await;
    assert_eq!(h.access(&[follower], &owner).await, ContentAccess::Visible);
    assert_eq!(h.access(&[stranger, follower], &owner).await, ContentAccess::Visible, "any own profile");
    assert_eq!(h.access(&[stranger], &owner).await, ContentAccess::HeaderOnly);
    assert_eq!(h.access(&[], &owner).await, ContentAccess::HeaderOnly);
    assert_eq!(h.access(&[owner], &owner).await, ContentAccess::Visible, "the owner");

    // Its lists follow the same rule.
    assert_eq!(h.followers_as(&owner, Viewer::Profiles(vec![follower])).await.len(), 2);
    assert!(h.followers_as(&owner, Viewer::Profiles(vec![stranger])).await.is_empty());
    assert!(h.followers_as(&owner, Viewer::Profiles(vec![])).await.is_empty());
    assert_eq!(h.followers_as(&owner, Viewer::Internal).await.len(), 2);

    // A block (here by the owner) hides everything, follower or not. Blocking
    // severs the follow too, so check the follower that was blocked.
    h.block(&owner, &fan).await;
    h.audience(&owner, AudienceFact::Private(false)).await;
    assert_eq!(h.access(&[fan], &owner).await, ContentAccess::Hidden);
    assert_eq!(h.access(&[stranger], &owner).await, ContentAccess::Visible, "public again");

    // Hidden (suspension, moderation): nothing for anyone but the owner.
    h.audience(&owner, AudienceFact::Hidden(true)).await;
    assert_eq!(h.access(&[follower], &owner).await, ContentAccess::Hidden);
    assert_eq!(h.access(&[owner], &owner).await, ContentAccess::Visible);
    h.audience(&owner, AudienceFact::Hidden(false)).await;
    assert_eq!(h.access(&[follower], &owner).await, ContentAccess::Visible);
}

/// How the reader relates to the author (#657: an author's location audience),
/// over the real follow table.
#[tokio::test]
async fn the_answer_says_who_follows_and_who_is_mutual() {
    use social_graph::domain::access::Relationship;
    let h = TestHarness::start().await;
    let (author, follower, mutual, stranger) =
        (harness::random_profile(), harness::random_profile(), harness::random_profile(), harness::random_profile());
    h.follow(&follower, &author).await;
    h.follow(&mutual, &author).await;
    h.follow(&author, &mutual).await;

    let rel = |r: Relationship| (r.follows, r.mutual);
    assert_eq!(rel(h.answer(&[stranger], &author).await.relation), (false, false));
    assert_eq!(rel(h.answer(&[follower], &author).await.relation), (true, false));
    assert_eq!(rel(h.answer(&[mutual], &author).await.relation), (true, true));
    assert_eq!(rel(h.answer(&[stranger, mutual], &author).await.relation), (true, true), "any own profile");
    assert_eq!(rel(h.answer(&[], &author).await.relation), (false, false), "anonymous");
    assert_eq!(rel(h.answer(&[author], &author).await.relation), (true, true), "oneself");
    // Followed back by someone who does not follow: not mutual.
    h.follow(&author, &stranger).await;
    assert_eq!(rel(h.answer(&[stranger], &author).await.relation), (false, false));
}
