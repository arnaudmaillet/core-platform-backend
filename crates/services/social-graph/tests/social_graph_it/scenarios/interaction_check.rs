//! Scenario — CheckInteraction over the real tables: the projected interaction
//! policy (profile_audience.interaction) against follows and blocks.

use cqrs::{Envelope, QueryBus};
use uuid::Uuid;

use social_graph::application::command::AudienceFact;
use social_graph::application::query::CheckInteractionQuery;
use social_graph::domain::interaction::{
    InteractionAudience, InteractionKind, InteractionLimit, InteractionPolicy, InteractionVerdict, LimitAudience,
};
use social_graph::domain::value_object::ProfileId;

use crate::social_graph_it::harness::{self, TestHarness};

async fn verdict(h: &TestHarness, actor: &ProfileId, target: &ProfileId, kind: InteractionKind) -> InteractionVerdict {
    let query = CheckInteractionQuery { actor_id: actor.as_str(), target_id: target.as_str(), kind };
    h.query_bus.dispatch(Envelope::new(Uuid::now_v7(), query)).await.unwrap()
}

async fn may(h: &TestHarness, actor: &ProfileId, target: &ProfileId, kind: InteractionKind) -> bool {
    verdict(h, actor, target, kind).await != InteractionVerdict::Refused
}

#[tokio::test]
async fn the_owners_audience_decides_and_a_block_always_refuses() {
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

    // No policy projected yet: everyone.
    assert!(may(&h, &stranger, &owner, InteractionKind::Comment).await);

    h.audience(
        &owner,
        AudienceFact::Interaction(InteractionPolicy {
            comments: InteractionAudience::Followers,
            mentions: InteractionAudience::Everyone,
            messages: InteractionAudience::Mutuals,
            limit:    None,
        }),
    )
    .await;
    assert!(!may(&h, &stranger, &owner, InteractionKind::Comment).await);
    assert!(may(&h, &follower, &owner, InteractionKind::Comment).await);
    assert!(!may(&h, &follower, &owner, InteractionKind::Message).await);
    assert!(may(&h, &mutual, &owner, InteractionKind::Message).await);
    assert!(may(&h, &stranger, &owner, InteractionKind::Mention).await);
    assert!(may(&h, &owner, &owner, InteractionKind::Message).await, "oneself");

    // The owner blocks the mutual: nothing gets through.
    h.block(&owner, &mutual).await;
    assert!(!may(&h, &mutual, &owner, InteractionKind::Mention).await);
}

#[tokio::test]
async fn a_limit_holds_non_followers_comments_and_lets_followers_through() {
    let h = TestHarness::start().await;
    let (owner, stranger, follower) = (harness::random_profile(), harness::random_profile(), harness::random_profile());
    h.follow(&follower, &owner).await;
    let until_ms = (chrono::Utc::now() + chrono::Duration::days(1)).timestamp_millis();
    h.audience(
        &owner,
        AudienceFact::Interaction(InteractionPolicy {
            limit: Some(InteractionLimit { audience: LimitAudience::NonFollowers, until_ms }),
            ..InteractionPolicy::default()
        }),
    )
    .await;
    assert_eq!(verdict(&h, &stranger, &owner, InteractionKind::Comment).await, InteractionVerdict::Held);
    assert_eq!(verdict(&h, &follower, &owner, InteractionKind::Comment).await, InteractionVerdict::Allowed);
    assert_eq!(verdict(&h, &stranger, &owner, InteractionKind::Mention).await, InteractionVerdict::Allowed);
}
