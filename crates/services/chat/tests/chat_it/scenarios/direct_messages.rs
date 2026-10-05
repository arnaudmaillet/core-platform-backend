//! #656 over live Scylla: direct conversations and message requests. One per
//! pair (the LWT claim), one message per request (the LWT spend), a silent
//! decline, and a block that looks exactly like an unanswered request while
//! withholding the blocked sender's messages from everyone else.

use chat::application::port::MessageVerdict;
use tonic::Code;

use crate::chat_it::harness::{self, proto, ChatService, HarnessOptions, Request, TestHarness};

#[tokio::test]
async fn one_conversation_per_pair_whoever_opens_it() {
    let h = TestHarness::start(HarnessOptions::default()).await;
    let (a, b) = (harness::random_profile(), harness::random_profile());

    // Both open at once: one LWT claim wins, both get its id.
    let (first, second) = tokio::join!(h.open_direct(&a, &b), h.open_direct(&b, &a));
    let (first, second) = (first.unwrap(), second.unwrap());
    assert_eq!(first.0, second.0, "unique per pair");
    assert!(!first.1 && !second.1, "admitted: no request");

    let mut roster = h.roster(&first.0, &a).await;
    roster.sort();
    let mut expected = vec![a.as_str(), b.as_str()];
    expected.sort();
    assert_eq!(roster, expected, "both on the roster");

    h.send_text(&first.0, &a, "hi").await;
    h.send_text(&first.0, &b, "hey").await;
    assert_eq!(h.bodies(&first.0, &a).await.len(), 2);

    // The export reads its kind.
    let page = ChatService::list_conversations_by_member(
        &h.handler,
        Request::new(proto::ListConversationsByMemberRequest { member_id: a.as_str(), limit: 10, page_token: String::new() }),
    )
    .await
    .unwrap()
    .into_inner();
    assert_eq!(page.memberships[0].kind, proto::ConversationKind::Direct as i32);

    // Never through CreateConversation, never public, never invited into.
    let err = ChatService::create_conversation(
        &h.handler,
        Request::new(proto::CreateConversationRequest { kind: proto::ConversationKind::Direct as i32, owner_id: a.as_str() }),
    )
    .await
    .unwrap_err();
    assert_eq!(err.code(), Code::FailedPrecondition);
    assert!(h.invite(&first.0, &a, &harness::random_profile()).await.is_err());
}

#[tokio::test]
async fn a_request_holds_one_message_and_a_reply_accepts_it() {
    let h = TestHarness::start(HarnessOptions::default()).await;
    let (stranger, recipient) = (harness::random_profile(), harness::random_profile());
    h.gate.set(&stranger, &recipient, MessageVerdict::Request);

    let (id, request) = h.open_direct(&stranger, &recipient).await.unwrap();
    assert!(request);
    // Two taps at once: the LWT spends the one message once.
    let (one, two) = tokio::join!(h.try_send(&id, &stranger, "hello?"), h.try_send(&id, &stranger, "hello??"));
    assert_eq!([one.is_ok(), two.is_ok()].iter().filter(|ok| **ok).count(), 1, "exactly one message");
    let err = h.try_send(&id, &stranger, "anyone?").await.unwrap_err();
    assert_eq!(err.code(), Code::FailedPrecondition, "CHT-1010");
    assert_eq!(h.bodies(&id, &recipient).await.len(), 1, "the recipient sees the request");

    // No read receipt before an answer: nothing kept.
    let history = ChatService::get_history(
        &h.handler,
        Request::new(proto::GetHistoryRequest {
            conversation_id: id.as_str(),
            requester_id:    recipient.as_str(),
            limit:           1,
            page_token:      String::new(),
        }),
    )
    .await
    .unwrap()
    .into_inner();
    ChatService::mark_read(
        &h.handler,
        Request::new(proto::MarkReadRequest {
            conversation_id: id.as_str(),
            member_id:       recipient.as_str(),
            message_id:      history.messages[0].message_id.clone(),
        }),
    )
    .await
    .unwrap();
    let members = ChatService::list_members(
        &h.handler,
        Request::new(proto::ListMembersRequest { conversation_id: id.as_str(), requester_id: stranger.as_str() }),
    )
    .await
    .unwrap()
    .into_inner()
    .members;
    assert!(members.iter().all(|m| m.last_read.is_empty()), "no receipt before acceptance");

    // The recipient replies: accepted, and the stranger writes freely.
    h.send_text(&id, &recipient, "hi!").await;
    h.send_text(&id, &stranger, "thanks").await;
    h.send_text(&id, &stranger, "so,").await;
    assert_eq!(h.bodies(&id, &recipient).await.len(), 4);
}

#[tokio::test]
async fn no_one_refuses_and_a_decline_is_silent() {
    let h = TestHarness::start(HarnessOptions::default()).await;
    let (sender, closed, decliner) = (harness::random_profile(), harness::random_profile(), harness::random_profile());
    h.gate.set(&sender, &closed, MessageVerdict::Refused);
    let err = h.open_direct(&sender, &closed).await.unwrap_err();
    assert_eq!(err.code(), Code::PermissionDenied, "CHT-1011");

    h.gate.set(&sender, &decliner, MessageVerdict::Request);
    let (id, _) = h.open_direct(&sender, &decliner).await.unwrap();
    h.try_send(&id, &sender, "hi").await.unwrap();
    assert!(h.respond(&id, &sender, true).await.is_err(), "a requester answers nothing");
    h.respond(&id, &decliner, false).await.unwrap();

    // The sender sees what any unanswered request shows.
    assert!(h.open_direct(&sender, &decliner).await.unwrap().1, "still a request to them");
    assert_eq!(h.try_send(&id, &sender, "?").await.unwrap_err().code(), Code::FailedPrecondition);

    // The decliner changes their mind by writing: open both ways.
    h.send_text(&id, &decliner, "sorry, hi").await;
    h.send_text(&id, &sender, "hi again").await;
}

/// Blocked, the sender's view is byte-for-byte that of an unanswered request;
/// the recipient never sees a word.
#[tokio::test]
async fn a_block_looks_like_an_unanswered_request_and_withholds() {
    let h = TestHarness::start(HarnessOptions::default()).await;
    let (blocked, blocker) = (harness::random_profile(), harness::random_profile());
    let (pending, recipient) = (harness::random_profile(), harness::random_profile());
    h.gate.set(&blocked, &blocker, MessageVerdict::Silenced);
    h.gate.set(&pending, &recipient, MessageVerdict::Request);

    let (silenced, silenced_request) = h.open_direct(&blocked, &blocker).await.unwrap();
    let (requested, requested_request) = h.open_direct(&pending, &recipient).await.unwrap();
    assert_eq!(silenced_request, requested_request);

    h.try_send(&silenced, &blocked, "hello").await.unwrap();
    h.try_send(&requested, &pending, "hello").await.unwrap();
    let a = h.try_send(&silenced, &blocked, "again").await.unwrap_err();
    let b = h.try_send(&requested, &pending, "again").await.unwrap_err();
    assert_eq!(a.code(), b.code());
    assert_eq!(
        a.message().replace(&silenced.as_str(), "_"),
        b.message().replace(&requested.as_str(), "_"),
        "the same answer, word for word"
    );

    assert_eq!(h.bodies(&silenced, &blocked).await, vec!["hello"], "the sender sees it sent");
    assert!(h.bodies(&silenced, &blocker).await.is_empty(), "the blocker never does");

    // Inside an open conversation: still sent, never delivered — not even
    // after an unblock.
    let (c, d) = (harness::random_profile(), harness::random_profile());
    let (open, _) = h.open_direct(&c, &d).await.unwrap();
    h.send_text(&open, &c, "before").await;
    h.gate.set(&c, &d, MessageVerdict::Silenced);
    h.send_text(&open, &c, "during").await;
    h.gate.set(&c, &d, MessageVerdict::Allowed);
    h.gate.set(&d, &c, MessageVerdict::Allowed);
    h.send_text(&open, &c, "after").await;
    // Sorted: messages sent within one millisecond tie on created_at.
    let sorted = |mut v: Vec<String>| {
        v.sort();
        v
    };
    assert_eq!(sorted(h.bodies(&open, &c).await), vec!["after", "before", "during"]);
    assert_eq!(sorted(h.bodies(&open, &d).await), vec!["after", "before"]);
}

/// Live, too: a withheld message reaches its sender's own stream and never
/// the blocker's, which gets the next delivered message first.
#[tokio::test]
async fn a_withheld_message_never_reaches_the_blockers_stream() {
    use futures::StreamExt;
    use crate::chat_it::harness::DEADLINE;

    let h = TestHarness::start(HarnessOptions::default()).await;
    let (sender, blocker) = (harness::random_profile(), harness::random_profile());
    let (id, _) = h.open_direct(&sender, &blocker).await.unwrap();
    let mut theirs = h.open_member_stream(&id, &blocker).await;
    let mut mine = h.open_member_stream(&id, &sender).await;

    async fn next_body(stream: &mut harness::ResponseStream<proto::StreamConversationResponse>) -> String {
        loop {
            let item = tokio::time::timeout(DEADLINE, stream.next()).await.expect("a frame in time");
            let event = item.expect("stream open").expect("no error").event.and_then(|e| e.event);
            if let Some(proto::chat_event::Event::Message(m)) = event {
                return m.body;
            }
        }
    }

    h.gate.set(&sender, &blocker, MessageVerdict::Silenced);
    h.send_text(&id, &sender, "withheld").await;
    h.gate.set(&sender, &blocker, MessageVerdict::Allowed);
    h.gate.set(&blocker, &sender, MessageVerdict::Allowed);
    h.send_text(&id, &sender, "delivered").await;

    assert_eq!(next_body(&mut mine).await, "withheld", "the sender's own echo");
    assert_eq!(next_body(&mut mine).await, "delivered");
    assert_eq!(next_body(&mut theirs).await, "delivered", "the withheld one never arrived");
}
