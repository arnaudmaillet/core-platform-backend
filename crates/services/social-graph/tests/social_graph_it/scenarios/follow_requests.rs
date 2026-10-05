//! Scenario — follow requests to a private profile over the real tables: a
//! request is not a follow (no access, not in the lists) until the owner
//! approves; declines, cancels and blocks leave nothing behind.

use cqrs::{CommandBus, Envelope, QueryBus};
use uuid::Uuid;

use social_graph::application::command::{
    ApproveFollowRequestCommand, AudienceFact, BlockProfileCommand, WithdrawFollowRequestCommand,
};
use social_graph::application::query::{FollowRequestsPage, GetRelationStatusQuery, ListFollowRequestsQuery};
use social_graph::domain::value_object::{ProfileId, RelationStatus};

use crate::social_graph_it::harness::{self, ContentAccess, TestHarness};

async fn requests(h: &TestHarness, owner: &ProfileId) -> Vec<ProfileId> {
    let query = ListFollowRequestsQuery { owner_id: owner.as_str(), limit: 100, page_token: None };
    let page: FollowRequestsPage = h.query_bus.dispatch(Envelope::new(Uuid::now_v7(), query)).await.unwrap();
    assert_eq!(page.pending, Some(page.requests.len() as u64), "the first page counts them all");
    page.requests.into_iter().map(|e| e.profile_id).collect()
}

async fn status(h: &TestHarness, actor: &ProfileId, target: &ProfileId) -> RelationStatus {
    let query = GetRelationStatusQuery { actor_id: actor.as_str(), target_id: target.as_str() };
    h.query_bus.dispatch(Envelope::new(Uuid::now_v7(), query)).await.unwrap().status
}

async fn withdraw(h: &TestHarness, requester: &ProfileId, target: &ProfileId) -> Result<(), cqrs::error::CqrsError> {
    h.command_bus
        .dispatch(Envelope::new(
            Uuid::now_v7(),
            WithdrawFollowRequestCommand { requester_id: requester.as_str(), target_id: target.as_str() },
        ))
        .await
}

#[tokio::test]
async fn a_private_profile_lets_in_only_the_requests_its_owner_approves() {
    let h = TestHarness::start().await;
    let owner = harness::random_profile();
    let (approved, declined, cancelled, blocked) = (
        harness::random_profile(),
        harness::random_profile(),
        harness::random_profile(),
        harness::random_profile(),
    );
    h.audience(&owner, AudienceFact::Private(true)).await;

    for requester in [&approved, &declined, &cancelled, &blocked] {
        h.follow(requester, &owner).await;
        assert_eq!(status(&h, requester, &owner).await, RelationStatus::Requested);
    }
    // A request is not a follow: no access, not a follower.
    assert_eq!(h.access(&[approved], &owner).await, ContentAccess::HeaderOnly);
    assert!(h.followers(&owner).await.is_empty());
    assert!(harness::dispatch_follow(h.command_bus.clone(), approved.as_str(), owner.as_str())
        .await
        .is_err(), "already requested");
    let inbox = requests(&h, &owner).await;
    assert_eq!(inbox.len(), 4);
    assert_eq!(inbox[0], blocked, "newest first");

    // Approved: a real follow.
    h.command_bus
        .dispatch(Envelope::new(
            Uuid::now_v7(),
            ApproveFollowRequestCommand { owner_id: owner.as_str(), requester_id: approved.as_str() },
        ))
        .await
        .expect("approve");
    assert_eq!(status(&h, &approved, &owner).await, RelationStatus::Following);
    assert_eq!(h.access(&[approved], &owner).await, ContentAccess::Visible);
    assert!(harness::contains(&h.followers(&owner).await, &approved));

    // Declined by the owner, cancelled by the requester: nothing left.
    withdraw(&h, &declined, &owner).await.expect("decline");
    withdraw(&h, &cancelled, &owner).await.expect("cancel");
    assert!(withdraw(&h, &cancelled, &owner).await.is_err(), "nothing pending");
    for gone in [&declined, &cancelled] {
        assert_eq!(status(&h, gone, &owner).await, RelationStatus::None);
    }

    // A block drops the pending request too.
    h.command_bus
        .dispatch(Envelope::new(
            Uuid::now_v7(),
            BlockProfileCommand { actor_id: owner.as_str(), target_id: blocked.as_str() },
        ))
        .await
        .expect("block");
    assert!(requests(&h, &owner).await.is_empty());
    assert_eq!(h.followers(&owner).await, vec![approved]);
}

/// The first page counts every pending request (#755: the privacy screen shows
/// the number); later pages do not count again.
#[tokio::test]
async fn the_first_page_tells_how_many_requests_are_pending() {
    let h = TestHarness::start().await;
    let owner = harness::random_profile();
    h.audience(&owner, AudienceFact::Private(true)).await;
    for _ in 0..3 {
        h.follow(&harness::random_profile(), &owner).await;
    }
    let page = |page_token| ListFollowRequestsQuery { owner_id: owner.as_str(), limit: 2, page_token };
    let first: FollowRequestsPage = h.query_bus.dispatch(Envelope::new(Uuid::now_v7(), page(None))).await.unwrap();
    assert_eq!(first.requests.len(), 2);
    assert_eq!(first.pending, Some(3));
    let second: FollowRequestsPage =
        h.query_bus.dispatch(Envelope::new(Uuid::now_v7(), page(first.next_page_token))).await.unwrap();
    assert_eq!(second.requests.len(), 1);
    assert_eq!(second.pending, None);
}

