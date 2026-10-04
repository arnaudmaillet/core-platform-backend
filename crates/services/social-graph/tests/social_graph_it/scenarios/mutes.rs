//! Scenario — mutes over the real table (#659): scopes, re-muting, the owner's
//! list, the relation status, and the mesh lookup the feeds use.

use std::collections::HashSet;

use cqrs::{CommandBus, Envelope, QueryBus};
use uuid::Uuid;

use social_graph::application::command::{MuteProfileCommand, UnmuteProfileCommand};
use social_graph::application::query::get_relation_status::RelationStatusView;
use social_graph::application::query::{GetRelationStatusQuery, ListMutesQuery, MutedProfilesQuery};
use social_graph::domain::mute::{Mute, MuteScope, MuteScopes};

use crate::social_graph_it::harness::{self, ProfileId, TestHarness};

const POSTS: MuteScopes = MuteScopes { posts: true, stories: false, messages: false };
const MESSAGES: MuteScopes = MuteScopes { posts: false, stories: false, messages: true };

async fn mute(h: &TestHarness, actor: &ProfileId, target: &ProfileId, scopes: MuteScopes) {
    let cmd = MuteProfileCommand { actor_id: actor.as_str(), target_id: target.as_str(), scopes };
    h.command_bus.dispatch(Envelope::new(Uuid::now_v7(), cmd)).await.expect("mute");
}

async fn muted(h: &TestHarness, muters: &[ProfileId], scope: MuteScope) -> HashSet<ProfileId> {
    let query = MutedProfilesQuery { profile_ids: muters.iter().map(ProfileId::as_str).collect(), scope };
    h.query_bus.dispatch(Envelope::new(Uuid::now_v7(), query)).await.expect("muted_profiles")
}

#[tokio::test]
async fn mutes_are_scoped_listed_and_lifted() {
    let h = TestHarness::start().await;
    let (me, alt, a, b, c) = (
        harness::random_profile(),
        harness::random_profile(),
        harness::random_profile(),
        harness::random_profile(),
        harness::random_profile(),
    );
    mute(&h, &me, &a, POSTS).await;
    mute(&h, &me, &b, MESSAGES).await;
    mute(&h, &alt, &c, POSTS).await;

    // The mesh lookup: per scope, across all of a reader's profiles.
    assert_eq!(muted(&h, &[me], MuteScope::Posts).await, HashSet::from([a]));
    assert_eq!(muted(&h, &[me], MuteScope::Messages).await, HashSet::from([b]));
    assert_eq!(muted(&h, &[me, alt], MuteScope::Posts).await, HashSet::from([a, c]));

    // Re-muting replaces the scopes.
    mute(&h, &me, &b, POSTS).await;
    assert_eq!(muted(&h, &[me], MuteScope::Posts).await, HashSet::from([a, b]));
    assert!(muted(&h, &[me], MuteScope::Messages).await.is_empty());

    // The relation status tells the actor how it mutes the target.
    let status: RelationStatusView = h
        .query_bus
        .dispatch(Envelope::new(Uuid::now_v7(), GetRelationStatusQuery { actor_id: me.as_str(), target_id: a.as_str() }))
        .await
        .expect("relation_status");
    assert_eq!(status.muted, POSTS);

    // The owner's list pages in profile-id order.
    let list = |token: Option<String>| ListMutesQuery { profile_id: me.as_str(), limit: 1, page_token: token };
    let (first, next): (Vec<Mute>, Option<String>) =
        h.query_bus.dispatch(Envelope::new(Uuid::now_v7(), list(None))).await.expect("list_mutes");
    assert_eq!(first.len(), 1);
    let (second, _): (Vec<Mute>, Option<String>) =
        h.query_bus.dispatch(Envelope::new(Uuid::now_v7(), list(next))).await.expect("list_mutes");
    let mut listed: Vec<ProfileId> = first.iter().chain(&second).map(|m| m.profile_id).collect();
    listed.sort_by_key(ProfileId::as_uuid);
    let mut expected = vec![a, b];
    expected.sort_by_key(ProfileId::as_uuid);
    assert_eq!(listed, expected);

    // Unmute lifts it (twice is fine).
    for _ in 0..2 {
        let cmd = UnmuteProfileCommand { actor_id: me.as_str(), target_id: a.as_str() };
        h.command_bus.dispatch(Envelope::new(Uuid::now_v7(), cmd)).await.expect("unmute");
    }
    assert_eq!(muted(&h, &[me], MuteScope::Posts).await, HashSet::from([b]));
}

#[tokio::test]
async fn a_profile_cannot_mute_itself_and_a_mute_needs_a_scope() {
    let h = TestHarness::start().await;
    let (me, other) = (harness::random_profile(), harness::random_profile());
    let dispatch = |target: ProfileId, scopes| {
        let cmd = MuteProfileCommand { actor_id: me.as_str(), target_id: target.as_str(), scopes };
        h.command_bus.dispatch(Envelope::new(Uuid::now_v7(), cmd))
    };
    assert!(dispatch(me, POSTS).await.is_err(), "self");
    assert!(dispatch(other, MuteScopes::default()).await.is_err(), "no scope");
}
