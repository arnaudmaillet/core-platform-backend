//! Scenario — restrictions over the real table (#659): the owner's list, the
//! relation status, and the mesh lookup comment's read gate uses.

use std::collections::HashSet;

use chrono::{DateTime, Utc};
use cqrs::{CommandBus, Envelope, QueryBus};
use uuid::Uuid;

use social_graph::application::command::{RestrictProfileCommand, UnrestrictProfileCommand};
use social_graph::application::query::get_relation_status::RelationStatusView;
use social_graph::application::query::{GetRelationStatusQuery, ListRestrictedQuery, RestrictedAmongQuery};

use crate::social_graph_it::harness::{self, ProfileId, TestHarness};

async fn restrict(h: &TestHarness, owner: &ProfileId, target: &ProfileId) {
    let cmd = RestrictProfileCommand { actor_id: owner.as_str(), target_id: target.as_str() };
    h.command_bus.dispatch(Envelope::new(Uuid::now_v7(), cmd)).await.expect("restrict");
}

async fn among(h: &TestHarness, owner: &ProfileId, candidates: &[ProfileId]) -> HashSet<ProfileId> {
    let query = RestrictedAmongQuery {
        owner_id:      owner.as_str(),
        candidate_ids: candidates.iter().map(ProfileId::as_str).collect(),
    };
    h.query_bus.dispatch(Envelope::new(Uuid::now_v7(), query)).await.expect("restricted_among")
}

#[tokio::test]
async fn restrictions_are_listed_looked_up_and_lifted() {
    let h = TestHarness::start().await;
    let (owner, troll, other, bystander) = (
        harness::random_profile(),
        harness::random_profile(),
        harness::random_profile(),
        harness::random_profile(),
    );
    restrict(&h, &owner, &troll).await;
    restrict(&h, &owner, &troll).await; // again: a no-op
    restrict(&h, &owner, &other).await;

    // The mesh lookup answers within the candidates only.
    assert_eq!(among(&h, &owner, &[troll, bystander]).await, HashSet::from([troll]));
    assert!(among(&h, &troll, &[owner]).await.is_empty(), "restriction is one-way");

    // The relation status tells the owner.
    let status: RelationStatusView = h
        .query_bus
        .dispatch(Envelope::new(
            Uuid::now_v7(),
            GetRelationStatusQuery { actor_id: owner.as_str(), target_id: troll.as_str() },
        ))
        .await
        .expect("relation_status");
    assert!(status.restricted);

    // The owner's list.
    let query = ListRestrictedQuery { profile_id: owner.as_str(), limit: 10, page_token: None };
    let (listed, next): (Vec<(ProfileId, DateTime<Utc>)>, Option<String>) =
        h.query_bus.dispatch(Envelope::new(Uuid::now_v7(), query)).await.expect("list_restricted");
    assert_eq!(listed.iter().map(|(p, _)| *p).collect::<HashSet<_>>(), HashSet::from([troll, other]));
    assert_eq!(next, None);

    // Unrestrict (twice is fine).
    for _ in 0..2 {
        let cmd = UnrestrictProfileCommand { actor_id: owner.as_str(), target_id: troll.as_str() };
        h.command_bus.dispatch(Envelope::new(Uuid::now_v7(), cmd)).await.expect("unrestrict");
    }
    assert!(among(&h, &owner, &[troll]).await.is_empty());

    // Not oneself.
    let cmd = RestrictProfileCommand { actor_id: owner.as_str(), target_id: owner.as_str() };
    assert!(h.command_bus.dispatch(Envelope::new(Uuid::now_v7(), cmd)).await.is_err());
}
