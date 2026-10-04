//! Scenario — list privacy and RemoveFollower over the real tables (#659).
//!
//! The owner's audience for each list (profile_audience.lists) decides who
//! else reads it, on top of the access rule; the owner and the mesh always
//! read it. A removed follower is gone from both adjacency lists.

use cqrs::{CommandBus, Envelope, QueryBus};
use uuid::Uuid;

use social_graph::application::command::{SetListPrivacyCommand, UnfollowProfileCommand};
use social_graph::application::query::{FollowListPage, ListFollowersQuery, ListFollowingQuery};
use social_graph::domain::interaction::InteractionAudience;

use crate::social_graph_it::harness::{self, ProfileId, TestHarness, Viewer};

async fn set(h: &TestHarness, owner: &ProfileId, followers: Option<InteractionAudience>, following: Option<InteractionAudience>) {
    let cmd = SetListPrivacyCommand { profile_id: owner.as_str(), followers, following };
    h.command_bus.dispatch(Envelope::new(Uuid::now_v7(), cmd)).await.expect("set_list_privacy");
}

async fn followers_page(h: &TestHarness, owner: &ProfileId, viewer: Viewer) -> FollowListPage {
    let query = ListFollowersQuery { followee_id: owner.as_str(), limit: 100, page_token: None, viewer };
    h.query_bus.dispatch(Envelope::new(Uuid::now_v7(), query)).await.expect("list_followers")
}

async fn following_page(h: &TestHarness, owner: &ProfileId, viewer: Viewer) -> FollowListPage {
    let query = ListFollowingQuery { follower_id: owner.as_str(), limit: 100, page_token: None, viewer };
    h.query_bus.dispatch(Envelope::new(Uuid::now_v7(), query)).await.expect("list_following")
}

fn as_(profile: &ProfileId) -> Viewer {
    Viewer::Profiles(vec![*profile])
}

#[tokio::test]
async fn each_list_shows_to_exactly_the_audience_its_owner_picked() {
    let h = TestHarness::start().await;
    let (owner, stranger, follower, mutual) = (
        harness::random_profile(),
        harness::random_profile(),
        harness::random_profile(),
        harness::random_profile(),
    );
    h.follow(&follower, &owner).await;
    h.follow(&mutual, &owner).await;
    h.follow(&owner, &mutual).await;

    // Default: everyone, anonymous included.
    let page = followers_page(&h, &owner, Viewer::Profiles(Vec::new())).await;
    assert!(!page.hidden);
    assert_eq!(page.edges.len(), 2);

    // Followers list for followers, following list for mutuals.
    set(&h, &owner, Some(InteractionAudience::Followers), Some(InteractionAudience::Mutuals)).await;
    for (viewer, sees_followers, sees_following) in [
        (Viewer::Profiles(Vec::new()), false, false),
        (as_(&stranger), false, false),
        (as_(&follower), true, false),
        (as_(&mutual), true, true),
        // A viewer with several profiles is judged by its best-placed one.
        (Viewer::Profiles(vec![stranger, mutual]), true, true),
    ] {
        let f = followers_page(&h, &owner, viewer.clone()).await;
        assert_eq!(!f.hidden, sees_followers, "followers list for {viewer:?}");
        assert_eq!(f.edges.is_empty(), !sees_followers);
        let g = following_page(&h, &owner, viewer.clone()).await;
        assert_eq!(!g.hidden, sees_following, "following list for {viewer:?}");
        assert_eq!(g.edges.is_empty(), !sees_following);
    }

    // Only me: nobody else, while the owner and the mesh still read both.
    // An unset list keeps its audience.
    set(&h, &owner, Some(InteractionAudience::NoOne), None).await;
    assert!(followers_page(&h, &owner, as_(&mutual)).await.hidden);
    assert!(!following_page(&h, &owner, as_(&mutual)).await.hidden, "following stayed mutuals");
    for viewer in [as_(&owner), Viewer::Internal] {
        assert_eq!(followers_page(&h, &owner, viewer.clone()).await.edges.len(), 2);
        assert_eq!(following_page(&h, &owner, viewer).await.edges.len(), 1);
    }
}

#[tokio::test]
async fn a_removed_follower_leaves_both_lists() {
    let h = TestHarness::start().await;
    let (owner, follower) = (harness::random_profile(), harness::random_profile());
    h.follow(&follower, &owner).await;
    assert_eq!(h.followers(&owner).await, vec![follower]);

    // RemoveFollower dispatches the follower's unfollow on the owner's behalf.
    let cmd = UnfollowProfileCommand { actor_id: follower.as_str(), target_id: owner.as_str() };
    h.command_bus.dispatch(Envelope::new(Uuid::now_v7(), cmd)).await.expect("remove_follower");

    assert!(h.followers(&owner).await.is_empty());
    assert!(h.following(&follower).await.is_empty());
}
