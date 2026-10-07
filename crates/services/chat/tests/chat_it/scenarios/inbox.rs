//! #656 over live Scylla: each member's inbox, newest activity first, and the
//! requests folder. The graph without a broker projects inline; the last
//! scenario goes through Kafka and the `InboxWorker`.

use chat::application::port::MessageVerdict;

use crate::chat_it::harness::{
    self, proto, ChatService, HarnessOptions, ProfileId, Request, TestHarness, KAFKA_DEADLINE,
};

use proto::InboxFolder::{Inbox, Requests};

fn ids(entries: &[proto::InboxEntryView]) -> Vec<String> {
    entries.iter().map(|e| e.conversation_id.clone()).collect()
}

async fn mark_read(h: &TestHarness, conv: &str, member: &ProfileId, message_id: &str) {
    ChatService::mark_read(
        &h.handler,
        Request::new(proto::MarkReadRequest {
            conversation_id: conv.to_owned(),
            member_id:       member.as_str(),
            message_id:      message_id.to_owned(),
        }),
    )
    .await
    .expect("mark_read");
}

#[tokio::test]
async fn the_inbox_lists_dms_and_groups_newest_first_with_previews_and_unread() {
    let h = TestHarness::start(HarnessOptions::default()).await;
    let (me, friend) = (harness::random_profile(), harness::random_profile());

    let group = h.create_private_group(&me).await;
    let (dm, _) = h.open_direct(&me, &friend).await.unwrap();
    assert_eq!(ids(&h.inbox(&me, Inbox).await), vec![group.as_str()], "a group from its creation; a DM from its first message");

    h.send_text(&dm, &friend, &"long ".repeat(40)).await;
    let inbox = h.inbox(&me, Inbox).await;
    assert_eq!(ids(&inbox), vec![dm.as_str(), group.as_str()], "newest first");
    let entry = &inbox[0];
    assert_eq!(entry.kind, proto::ConversationKind::Direct as i32);
    assert_eq!(entry.peer_id, friend.as_str());
    let last = entry.last_message.as_ref().expect("a preview");
    assert_eq!(last.preview.chars().count(), 100);
    assert!(entry.unread);
    assert!(!entry.request);

    mark_read(&h, &dm.as_str(), &me, &last.message_id).await;
    assert!(!h.inbox(&me, Inbox).await[0].unread, "read");
    assert!(!h.inbox(&friend, Inbox).await[0].unread, "one's own message is never unread");

    h.send_text(&group, &me, "hello group").await;
    assert_eq!(ids(&h.inbox(&me, Inbox).await), vec![group.as_str(), dm.as_str()]);
}

#[tokio::test]
async fn a_request_waits_in_requests_until_accepted_and_a_decline_clears_it() {
    let h = TestHarness::start(HarnessOptions::default()).await;
    let (stranger, me, other) = (harness::random_profile(), harness::random_profile(), harness::random_profile());
    h.gate.set(&stranger, &me, MessageVerdict::Request);
    h.gate.set(&other, &me, MessageVerdict::Request);

    let (accepted, _) = h.open_direct(&stranger, &me).await.unwrap();
    h.send_text(&accepted, &stranger, "hi, can we talk?").await;
    let (declined, _) = h.open_direct(&other, &me).await.unwrap();
    h.send_text(&declined, &other, "hey").await;

    let requests = h.inbox(&me, Requests).await;
    assert_eq!(ids(&requests), vec![declined.as_str(), accepted.as_str()]);
    assert!(requests.iter().all(|e| e.unread && !e.request));
    assert!(h.inbox(&me, Inbox).await.is_empty());
    let theirs = h.inbox(&stranger, Inbox).await;
    assert!(theirs[0].request, "the requester sees their own request awaiting");
    // The recipient reads the request; the requester learns nothing.
    assert_eq!(h.bodies(&accepted, &me).await, vec!["hi, can we talk?"]);

    h.respond(&accepted, &me, true).await.unwrap();
    h.respond(&declined, &me, false).await.unwrap();
    assert_eq!(ids(&h.inbox(&me, Inbox).await), vec![accepted.as_str()], "accepted: in the inbox");
    assert!(h.inbox(&me, Requests).await.is_empty(), "declined: gone");
    assert!(!h.inbox(&stranger, Inbox).await[0].request, "accepted: no longer a request");
    assert!(h.inbox(&other, Inbox).await[0].request, "declined: still pending, to its sender");
}

/// The must-have: a blocked sender never reaches the blocker's requests —
/// opened while blocked, or blocked after the request.
#[tokio::test]
async fn a_blocked_sender_never_shows_in_the_blockers_requests() {
    let h = TestHarness::start(HarnessOptions::default()).await;
    let (blocked, blocker) = (harness::random_profile(), harness::random_profile());
    h.gate.set(&blocked, &blocker, MessageVerdict::Silenced);
    let (silenced, request) = h.open_direct(&blocked, &blocker).await.unwrap();
    assert!(request);
    h.send_text(&silenced, &blocked, "hello").await;
    assert!(h.inbox(&blocker, Requests).await.is_empty(), "nothing in their requests");
    assert!(h.inbox(&blocker, Inbox).await.is_empty(), "nor in their inbox");
    let mine = h.inbox(&blocked, Inbox).await;
    assert_eq!(ids(&mine), vec![silenced.as_str()]);
    assert!(mine[0].request, "to the sender: a request like any other");

    let (late, me) = (harness::random_profile(), harness::random_profile());
    h.gate.set(&late, &me, MessageVerdict::Request);
    let (conv, _) = h.open_direct(&late, &me).await.unwrap();
    h.send_text(&conv, &late, "hi").await;
    assert_eq!(h.inbox(&me, Requests).await.len(), 1);
    h.gate.set(&late, &me, MessageVerdict::Silenced);
    assert!(h.inbox(&me, Requests).await.is_empty(), "blocked after the request: gone");
}

#[tokio::test]
async fn the_inbox_worker_follows_chats_topics() {
    let h = TestHarness::start(HarnessOptions { with_kafka: true, ..Default::default() }).await;
    let (me, friend) = (harness::random_profile(), harness::random_profile());
    let (dm, _) = h.open_direct(&me, &friend).await.unwrap();
    h.send_text(&dm, &friend, "through kafka").await;
    let group = h.create_private_group(&me).await;

    let expected = vec![group.as_str(), dm.as_str()];
    harness::await_until("the worker filed both", KAFKA_DEADLINE, || {
        let (h, me, expected) = (&h, &me, expected.clone());
        async move { ids(&h.inbox(me, Inbox).await) == expected }
    })
    .await;
}

async fn mute(h: &TestHarness, conv: &str, member: &ProfileId, muted: bool, until_ms: i64) -> Result<(), tonic::Status> {
    ChatService::mute_conversation(
        &h.handler,
        Request::new(proto::MuteConversationRequest {
            conversation_id: conv.to_owned(),
            member_id: member.as_str(),
            muted,
            until_ms,
        }),
    )
    .await
    .map(|_| ())
}

/// #654: a member mutes a conversation's pushes — for a while or until they
/// unmute — and their inbox shows it; the roster stays readable (messages
/// still project), and a non-member cannot mute.
#[tokio::test]
async fn a_member_mutes_a_conversation_and_the_inbox_shows_it() {
    let h = TestHarness::start(HarnessOptions::default()).await;
    let (me, friend) = (harness::random_profile(), harness::random_profile());
    let (conversation, _) = h.open_direct(&me, &friend).await.unwrap();
    h.send_text(&conversation, &friend, "hi").await;
    let dm = conversation.as_str();
    assert!(!h.inbox(&me, Inbox).await[0].muted);

    let until = chrono::Utc::now().timestamp_millis() + 3_600_000;
    mute(&h, &dm, &me, true, until).await.expect("mute for an hour");
    let entry = &h.inbox(&me, Inbox).await[0];
    assert!(entry.muted);
    assert_eq!(entry.muted_until_ms, until);
    assert!(!h.inbox(&friend, Inbox).await[0].muted, "one member's own setting");

    mute(&h, &dm, &me, true, 0).await.expect("mute until unmuted");
    let entry = &h.inbox(&me, Inbox).await[0];
    assert!(entry.muted && entry.muted_until_ms == 0);

    // The roster still reads: a new message lands in both inboxes.
    h.send_text(&conversation, &friend, "still there?").await;
    assert_eq!(h.inbox(&me, Inbox).await[0].last_message.as_ref().unwrap().preview, "still there?");

    mute(&h, &dm, &me, false, 0).await.expect("unmute");
    assert!(!h.inbox(&me, Inbox).await[0].muted);

    let outsider = harness::random_profile();
    assert!(mute(&h, &dm, &outsider, true, 0).await.is_err(), "not a member");
    let past = chrono::Utc::now().timestamp_millis() - 1_000;
    assert!(mute(&h, &dm, &me, true, past).await.is_err(), "a mute ends in the future");
}
