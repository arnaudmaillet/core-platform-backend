//! Scenario — presence settings (#661) and Member-Plane signal authorization.
//!
//! A member who turned activity status off announces no presence; one who
//! turned read receipts off broadcasts no receipt and its `last_read` is
//! withheld from the other members (not from itself). A non-member cannot
//! inject presence or typing into a conversation.

use chat::application::port::PresenceSettings;
use tonic::Code;

use crate::chat_it::harness::{
    self, proto, ChatService, HarnessOptions, PlaneEvent, Request, TestHarness, DEADLINE,
};

fn heartbeat(conv: &str, member: &str) -> Request<proto::HeartbeatRequest> {
    Request::new(proto::HeartbeatRequest { conversation_id: conv.to_owned(), member_id: member.to_owned() })
}

#[tokio::test]
async fn presence_and_receipts_follow_the_members_settings() {
    let h = TestHarness::start(HarnessOptions::default()).await;
    let (owner, other) = (harness::random_profile(), harness::random_profile());
    let conv = h.create_public_channel(&owner).await;
    ChatService::join_as_member(
        &h.handler,
        Request::new(proto::JoinAsMemberRequest { conversation_id: conv.as_str(), profile_id: other.as_str() }),
    )
    .await
    .expect("join");

    // The owner shares nothing.
    h.presence_settings
        .set(&owner, PresenceSettings { activity_status: false, read_receipts: false })
        .await
        .expect("settings");

    let mut tap = h.member_registry.subscribe(&conv);
    let _other_stream = h.open_member_stream(&conv, &other).await;
    // `other` shares by default: its own online announcement comes first.
    let first = harness::recv_event(&mut tap, DEADLINE).await.expect("other online");
    assert!(matches!(&*first, PlaneEvent::Presence { member_id, online: true } if *member_id == other.as_str()));

    // The owner's stream, heartbeat and read: no presence, no receipt.
    let _owner_stream = h.open_member_stream(&conv, &owner).await;
    ChatService::heartbeat(&h.handler, heartbeat(&conv.as_str(), &owner.as_str())).await.expect("heartbeat");
    let read = h.send_text(&conv, &other, "hello").await;
    ChatService::mark_read(
        &h.handler,
        Request::new(proto::MarkReadRequest {
            conversation_id: conv.as_str(),
            member_id:       owner.as_str(),
            message_id:      read.as_str(),
        }),
    )
    .await
    .expect("mark_read");
    // A sentinel: the next event on the plane is the message, then nothing
    // from the owner.
    let next = harness::recv_event(&mut tap, DEADLINE).await.expect("message");
    assert!(matches!(&*next, PlaneEvent::Message(_)), "got {next:?}");
    assert!(harness::recv_event(&mut tap, std::time::Duration::from_millis(500)).await.is_none());

    // The roster: the owner's last_read is withheld from `other`, not from the owner.
    let roster = |requester: &str| {
        ChatService::list_members(
            &h.handler,
            Request::new(proto::ListMembersRequest { conversation_id: conv.as_str(), requester_id: requester.to_owned() }),
        )
    };
    let seen_by_other = roster(&other.as_str()).await.expect("roster").into_inner();
    let owner_row = seen_by_other.members.iter().find(|m| m.profile_id == owner.as_str()).unwrap();
    assert!(owner_row.last_read.is_empty(), "withheld from the other member");
    let seen_by_owner = roster(&owner.as_str()).await.expect("roster").into_inner();
    let own_row = seen_by_owner.members.iter().find(|m| m.profile_id == owner.as_str()).unwrap();
    assert_eq!(own_row.last_read, read.as_str(), "a member always sees its own");
}

#[tokio::test]
async fn a_non_member_cannot_signal_presence_or_typing() {
    let h = TestHarness::start(HarnessOptions::default()).await;
    let (owner, stranger) = (harness::random_profile(), harness::random_profile());
    let conv = h.create_public_channel(&owner).await;

    let err = ChatService::heartbeat(&h.handler, heartbeat(&conv.as_str(), &stranger.as_str())).await.unwrap_err();
    assert_eq!(err.code(), Code::PermissionDenied);
    let err = ChatService::send_typing(
        &h.handler,
        Request::new(proto::SendTypingRequest { conversation_id: conv.as_str(), member_id: stranger.as_str() }),
    )
    .await
    .unwrap_err();
    assert_eq!(err.code(), Code::PermissionDenied);
}

#[tokio::test]
async fn settings_are_read_for_a_roster_past_the_in_clause_cap() {
    // A group holds up to 500 members; Scylla refuses an IN over 100 keys.
    let h = TestHarness::start(HarnessOptions::default()).await;
    let members: Vec<_> = (0..250).map(|_| harness::random_profile()).collect();
    let opted_out = [members[3], members[120], members[249]];
    for profile in &opted_out {
        h.presence_settings
            .set(profile, PresenceSettings { activity_status: true, read_receipts: false })
            .await
            .expect("settings");
    }

    let read = h.presence_settings.get_many(&members).await.expect("get_many over 250 members");
    assert_eq!(read.len(), 3, "only stored rows come back");
    for profile in &opted_out {
        assert!(!read[profile].read_receipts);
    }
}
