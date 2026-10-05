//! Scenario — Private-conversation join access control.
//!
//! `JoinAsMember` on a **private** conversation requires a pending invitation
//! issued by an owner/admin (`InviteMember`); without one the call fails
//! `NOT_FOUND`, indistinguishable from a conversation that does not exist, and
//! leaves the roster untouched. An invitation is consumed by the join. Public
//! conversations stay open-join. Everything is asserted against live Scylla
//! through the gRPC trait (the invitation row, its read on the join path, and the
//! roster).

use tonic::Code;

use crate::chat_it::harness::{
    self, proto, ChatService, ConversationId, HarnessOptions, ProfileId, Request, TestHarness,
};

/// Strips the (random) conversation id so two `NOT_FOUND` messages compare on shape.
fn shape(message: &str, conv: &ConversationId) -> String {
    message.replace(&conv.as_str(), "<id>")
}

#[tokio::test]
async fn non_member_join_of_a_private_group_is_refused_as_not_found() {
    let h = TestHarness::start(HarnessOptions::default()).await;
    let owner = harness::random_profile();
    let outsider = harness::random_profile();
    let conv = h.create_private_group(&owner).await;

    let refused = h.join(&conv, &outsider).await.expect_err("uninvited join must be refused");
    assert_eq!(refused.code(), Code::NotFound, "{refused:?}");

    // Same answer as for a conversation that does not exist at all.
    let missing_conv = ConversationId::new();
    let missing = h.join(&missing_conv, &outsider).await.expect_err("join of a missing conversation");
    assert_eq!(missing.code(), refused.code());
    assert_eq!(shape(refused.message(), &conv), shape(missing.message(), &missing_conv));

    // Nothing was written: the roster is still the owner alone.
    assert_eq!(h.roster(&conv, &owner).await, vec![owner.as_str()]);

    // An outsider cannot mint its own invitation either — same concealment.
    let self_invite = h.invite(&conv, &outsider, &outsider).await.expect_err("outsider invite");
    assert_eq!(self_invite.code(), Code::NotFound, "{self_invite:?}");
    let still_refused = h.join(&conv, &outsider).await.expect_err("still uninvited");
    assert_eq!(still_refused.code(), Code::NotFound);
}

#[tokio::test]
async fn invited_member_can_join_a_private_group_once() {
    let h = TestHarness::start(HarnessOptions::default()).await;
    let owner = harness::random_profile();
    let invitee = harness::random_profile();
    let conv = h.create_private_group(&owner).await;

    h.invite(&conv, &owner, &invitee).await.expect("owner invites");
    h.join(&conv, &invitee).await.expect("invited join");

    let mut roster = h.roster(&conv, &owner).await;
    roster.sort();
    let mut expected = vec![owner.as_str(), invitee.as_str()];
    expected.sort();
    assert_eq!(roster, expected);

    // The new member participates: it can post into the private group.
    h.send_text(&conv, &invitee, "hello from the invitee").await;

    // A re-join is a conflict (the member already sees the conversation).
    let again = h.join(&conv, &invitee).await.expect_err("re-join");
    assert_eq!(again.code(), Code::AlreadyExists, "{again:?}");

    // A plain member may not invite; only owner/admin can.
    let other = harness::random_profile();
    let denied = h.invite(&conv, &invitee, &other).await.expect_err("member invite");
    assert_eq!(denied.code(), Code::PermissionDenied, "{denied:?}");
    assert_eq!(h.join(&conv, &other).await.expect_err("not invited").code(), Code::NotFound);
}

#[tokio::test]
async fn public_group_join_is_unchanged() {
    let h = TestHarness::start(HarnessOptions::default()).await;
    let owner = harness::random_profile();
    let joiner = harness::random_profile();
    let conv = h.create_private_group(&owner).await;

    set_public(&h, &conv, &owner, true).await;
    h.join(&conv, &joiner).await.expect("open join of a public group");
    assert!(h.roster(&conv, &owner).await.contains(&joiner.as_str()));

    // Going private again closes open join for newcomers.
    set_public(&h, &conv, &owner, false).await;
    let late = harness::random_profile();
    assert_eq!(h.join(&conv, &late).await.expect_err("private again").code(), Code::NotFound);
}

/// `ToggleVisibility` by an administering `actor`.
async fn set_public(h: &TestHarness, conv: &ConversationId, actor: &ProfileId, public: bool) {
    ChatService::toggle_visibility(
        &h.handler,
        Request::new(proto::ToggleVisibilityRequest {
            conversation_id: conv.as_str(),
            actor_id:        actor.as_str(),
            make_public:     public,
        }),
    )
    .await
    .expect("toggle_visibility");
}
