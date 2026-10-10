//! Scenario — A private conversation is unprobeable by outsiders.
//!
//! Every conversation-scoped RPC answers a non-member of a **private**
//! conversation exactly as it answers for a conversation that does not exist:
//! same gRPC code (`NOT_FOUND`) and same message once the id is masked. Members
//! keep their precise errors, and outsiders of a **public** conversation (which
//! is discoverable anyway) keep `PERMISSION_DENIED` on the member-only RPCs.
//! Asserted against live Scylla/Redis through the gRPC trait.

use tonic::Code;

use crate::chat_it::harness::{
    self, proto, ChatService, ConversationId, HarnessOptions, ProfileId, Request, Status,
    TestHarness,
};

/// Every conversation-scoped RPC a caller can aim at someone else's conversation.
const RPCS: &[&str] = &[
    "ToggleVisibility",
    "JoinAsMember",
    "InviteMember",
    "Subscribe",
    "SendMessage",
    "MarkRead",
    "SendTyping",
    "Heartbeat",
    "GetHistory",
    "ListMembers",
    "StreamConversation",
    "StreamPublic",
];

/// The RPCs reserved to roster members (no audience access at all).
const MEMBER_ONLY: &[&str] =
    &["SendMessage", "MarkRead", "SendTyping", "Heartbeat", "ListMembers", "StreamConversation"];

/// Calls `rpc` on `conv` as `caller`, returning the refusal (or `None` on success).
async fn probe(h: &TestHarness, rpc: &str, conv: &ConversationId, caller: &ProfileId) -> Option<Status> {
    let (c, p) = (conv.as_str(), caller.as_str());
    match rpc {
        "ToggleVisibility" => ChatService::toggle_visibility(
            &h.handler,
            Request::new(proto::ToggleVisibilityRequest { conversation_id: c, actor_id: p, make_public: true }),
        )
        .await
        .err(),
        "JoinAsMember" => h.join(conv, caller).await.err(),
        "InviteMember" => h.invite(conv, caller, &harness::random_profile()).await.err(),
        "Subscribe" => ChatService::subscribe(
            &h.handler,
            Request::new(proto::SubscribeRequest { conversation_id: c, subscriber_id: p }),
        )
        .await
        .err(),
        "SendMessage" => ChatService::send_message(
            &h.handler,
            Request::new(proto::SendMessageRequest {
                conversation_id: c,
                sender_id:       p,
                content_type:    0, // Text
                body:            "probe".to_owned(),
                media_ref:       String::new(),
                reply_to:        String::new(),
                idempotency_key: String::new(),
            }),
        )
        .await
        .err(),
        "MarkRead" => ChatService::mark_read(
            &h.handler,
            Request::new(proto::MarkReadRequest {
                conversation_id: c,
                member_id:       p,
                message_id:      harness::random_message_id(),
            }),
        )
        .await
        .err(),
        "SendTyping" => ChatService::send_typing(
            &h.handler,
            Request::new(proto::SendTypingRequest { conversation_id: c, member_id: p }),
        )
        .await
        .err(),
        "Heartbeat" => ChatService::heartbeat(
            &h.handler,
            Request::new(proto::HeartbeatRequest { conversation_id: c, member_id: p }),
        )
        .await
        .err(),
        "GetHistory" => ChatService::get_history(
            &h.handler,
            Request::new(proto::GetHistoryRequest {
                conversation_id: c,
                requester_id:    p,
                limit:           10,
                page_token:      String::new(),
            }),
        )
        .await
        .err(),
        "ListMembers" => ChatService::list_members(
            &h.handler,
            Request::new(proto::ListMembersRequest { conversation_id: c, requester_id: p }),
        )
        .await
        .err(),
        "StreamConversation" => ChatService::stream_conversation(
            &h.handler,
            Request::new(proto::StreamConversationRequest { conversation_id: c, member_id: p }),
        )
        .await
        .err(),
        "StreamPublic" => ChatService::stream_public(
            &h.handler,
            Request::new(proto::StreamPublicRequest { conversation_id: c, subscriber_id: p }),
        )
        .await
        .err(),
        other => unreachable!("unknown rpc {other}"),
    }
}

/// Masks the (random) conversation id so two refusals compare on shape.
fn shape(status: &Status, conv: &ConversationId) -> (Code, String) {
    (status.code(), status.message().replace(&conv.as_str(), "<id>"))
}

#[tokio::test]
async fn every_rpc_answers_an_outsider_of_a_private_conversation_like_a_missing_one() {
    let h = TestHarness::start(HarnessOptions::default()).await;
    let owner = harness::random_profile();
    let outsider = harness::random_profile();
    let conv = h.create_private_group(&owner).await;
    // Content an outsider must not reach, and the roster it must not join.
    h.send_text(&conv, &owner, "private").await;

    for rpc in RPCS {
        let missing_conv = ConversationId::new();
        let concealed = probe(&h, rpc, &conv, &outsider).await.unwrap_or_else(|| panic!("{rpc}: outsider admitted"));
        let missing = probe(&h, rpc, &missing_conv, &outsider).await.unwrap_or_else(|| panic!("{rpc}: missing admitted"));

        assert_eq!(concealed.code(), Code::NotFound, "{rpc}: {concealed:?}");
        assert_eq!(shape(&concealed, &conv), shape(&missing, &missing_conv), "{rpc}: distinguishable");
    }

    // Nothing leaked into the conversation: still private, roster untouched.
    assert_eq!(h.roster(&conv, &owner).await, vec![owner.as_str()]);
    assert_eq!(
        probe(&h, "StreamPublic", &conv, &outsider).await.map(|s| s.code()),
        Some(Code::NotFound),
        "the outsider's ToggleVisibility must not have published it",
    );
}

#[tokio::test]
async fn members_keep_their_precise_errors_on_a_private_conversation() {
    let h = TestHarness::start(HarnessOptions::default()).await;
    let owner = harness::random_profile();
    let conv = h.create_private_group(&owner).await;

    // A member knows the conversation exists: the precise reason is safe.
    let not_public = probe(&h, "Subscribe", &conv, &owner).await.expect("subscribe to private");
    assert_eq!(not_public.code(), Code::FailedPrecondition, "{not_public:?}");
    let already = probe(&h, "JoinAsMember", &conv, &owner).await.expect("re-join");
    assert_eq!(already.code(), Code::AlreadyExists, "{already:?}");
    let not_streamable = probe(&h, "StreamPublic", &conv, &owner).await.expect("stream a private one");
    assert_eq!(not_streamable.code(), Code::FailedPrecondition, "{not_streamable:?}");

    // And every member RPC that it is allowed to call still succeeds.
    for rpc in ["SendMessage", "MarkRead", "SendTyping", "Heartbeat", "GetHistory", "ListMembers", "StreamConversation"] {
        assert!(probe(&h, rpc, &conv, &owner).await.is_none(), "{rpc}: member refused");
    }
}

#[tokio::test]
async fn outsiders_of_a_public_conversation_keep_permission_denied() {
    let h = TestHarness::start(HarnessOptions::default()).await;
    let owner = harness::random_profile();
    let outsider = harness::random_profile();
    let conv = h.create_public_channel(&owner).await;

    for rpc in MEMBER_ONLY {
        let denied = probe(&h, rpc, &conv, &outsider).await.unwrap_or_else(|| panic!("{rpc}: outsider admitted"));
        assert_eq!(denied.code(), Code::PermissionDenied, "{rpc}: {denied:?}");
    }
    let toggle = probe(&h, "ToggleVisibility", &conv, &outsider).await.expect("outsider toggle");
    assert_eq!(toggle.code(), Code::PermissionDenied, "{toggle:?}");

    // The audience surface stays open on a public conversation.
    for rpc in ["Subscribe", "GetHistory", "StreamPublic"] {
        assert!(probe(&h, rpc, &conv, &outsider).await.is_none(), "{rpc}: audience refused");
    }
}
