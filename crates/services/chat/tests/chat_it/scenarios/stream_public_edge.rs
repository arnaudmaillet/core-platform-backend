//! Scenario — `StreamPublic` binds `subscriber_id` to the edge caller.
//!
//! On the edge, the subscriber must be one of the profiles the token's account
//! owns (`pids`), like every other actor field: a caller cannot stream (and pick
//! a shard, or probe a private roster) as someone else. Over the mesh the field
//! is trusted as-is, as before.

use std::sync::Arc;

use auth_context::{CurrentPrincipal, OidcClaims, PrincipalId};
use tonic::Code;
use transport::grpc::edge::EdgePrincipal;

use crate::chat_it::harness::{
    self, proto, ChatService, ConversationId, HarnessOptions, ProfileId, Request, Status,
    TestHarness,
};

/// A member edge token whose account owns exactly `profile`.
fn member_owning(profile: &ProfileId) -> EdgePrincipal {
    principal("acct-edge", serde_json::json!({
        "sub": "acct-edge", "exp": 4_102_444_800_i64, "pids": [profile.as_str()]
    }))
}

fn principal(sub: &str, claims: serde_json::Value) -> EdgePrincipal {
    let raw: OidcClaims = serde_json::from_value(claims).expect("claims");
    EdgePrincipal::new(Arc::new(CurrentPrincipal {
        user_id:     PrincipalId::new(sub),
        tenant_id:   None,
        permissions: vec![],
        raw_claims:  raw,
    }))
}

/// `StreamPublic` for `subscriber`, as `caller` on the edge (`None` = mesh).
async fn stream_public(
    h: &TestHarness,
    conv: &ConversationId,
    subscriber: &ProfileId,
    caller: Option<EdgePrincipal>,
) -> Result<(), Status> {
    let mut request = Request::new(proto::StreamPublicRequest {
        conversation_id: conv.as_str(),
        subscriber_id:   subscriber.as_str(),
    });
    if let Some(p) = caller {
        request.extensions_mut().insert(p);
    }
    ChatService::stream_public(&h.handler, request).await.map(drop)
}

#[tokio::test]
async fn the_edge_streams_only_as_the_callers_own_profile() {
    let h = TestHarness::start(HarnessOptions::default()).await;
    let owner = harness::random_profile();
    let viewer = harness::random_profile();
    let conv = h.create_public_channel(&owner).await;

    stream_public(&h, &conv, &viewer, Some(member_owning(&viewer))).await.expect("its own profile");

    let other = harness::random_profile();
    let refused = stream_public(&h, &conv, &other, Some(member_owning(&viewer))).await.unwrap_err();
    assert_eq!(refused.code(), Code::PermissionDenied, "{refused:?}");

    // A guest owns no profile, so it cannot claim one.
    let guest = principal("guest:g-1", serde_json::json!({
        "sub": "guest:g-1", "exp": 4_102_444_800_i64, "kind": "guest"
    }));
    let refused = stream_public(&h, &conv, &viewer, Some(guest)).await.unwrap_err();
    assert_eq!(refused.code(), Code::PermissionDenied, "{refused:?}");

    // The mesh is trusted as before.
    stream_public(&h, &conv, &other, None).await.expect("mesh caller");
}

#[tokio::test]
async fn a_bound_member_learns_not_public_but_a_claimed_member_id_does_not_help() {
    let h = TestHarness::start(HarnessOptions::default()).await;
    let owner = harness::random_profile();
    let outsider = harness::random_profile();
    let conv = h.create_private_group(&owner).await;

    // The owner, streaming as itself, gets the precise reason.
    let not_public = stream_public(&h, &conv, &owner, Some(member_owning(&owner))).await.unwrap_err();
    assert_eq!(not_public.code(), Code::FailedPrecondition, "{not_public:?}");

    // An outsider claiming the owner's id is refused before any lookup: it
    // learns nothing about the conversation or its roster.
    let refused = stream_public(&h, &conv, &owner, Some(member_owning(&outsider))).await.unwrap_err();
    assert_eq!(refused.code(), Code::PermissionDenied, "{refused:?}");
    let missing_conv = ConversationId::new();
    let on_missing = stream_public(&h, &missing_conv, &owner, Some(member_owning(&outsider))).await.unwrap_err();
    assert_eq!(
        (refused.code(), refused.message()),
        (on_missing.code(), on_missing.message()),
        "same refusal whether or not the conversation exists",
    );

    // Streaming as itself, the outsider is concealed like a missing conversation.
    let concealed = stream_public(&h, &conv, &outsider, Some(member_owning(&outsider))).await.unwrap_err();
    assert_eq!(concealed.code(), Code::NotFound, "{concealed:?}");
}
