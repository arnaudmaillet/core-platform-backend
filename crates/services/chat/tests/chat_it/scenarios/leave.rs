//! #656 over live Scylla: leaving a group. Off the roster and out of the
//! inbox; the membership kept with left_at, so the GDPR export still reads
//! the leaver's own messages — up to the departure, others' reduced to their
//! time. Rejoining clears left_at.

use tonic::Code;

use crate::chat_it::harness::{self, proto, ChatService, ConversationId, HarnessOptions, ProfileId, Request, TestHarness};

async fn leave(h: &TestHarness, conv: &ConversationId, who: &ProfileId) -> Result<(), tonic::Status> {
    ChatService::leave_conversation(
        &h.handler,
        Request::new(proto::LeaveConversationRequest { conversation_id: conv.as_str(), profile_id: who.as_str() }),
    )
    .await
    .map(drop)
}

async fn membership(h: &TestHarness, who: &ProfileId, conv: &ConversationId) -> Option<proto::MembershipView> {
    ChatService::list_conversations_by_member(
        &h.handler,
        Request::new(proto::ListConversationsByMemberRequest { member_id: who.as_str(), limit: 50, page_token: String::new() }),
    )
    .await
    .unwrap()
    .into_inner()
    .memberships
    .into_iter()
    .find(|m| m.conversation_id == conv.as_str())
}

async fn former_history(h: &TestHarness, conv: &ConversationId, who: &ProfileId) -> Vec<(String, String)> {
    ChatService::get_former_member_history(
        &h.handler,
        Request::new(proto::GetFormerMemberHistoryRequest {
            conversation_id: conv.as_str(),
            member_id:       who.as_str(),
            limit:           50,
            page_token:      String::new(),
        }),
    )
    .await
    .unwrap()
    .into_inner()
    .messages
    .into_iter()
    .map(|m| (m.sender_id, m.body))
    .collect()
}

#[tokio::test]
async fn a_leaver_keeps_its_own_messages_up_to_its_departure() {
    let h = TestHarness::start(HarnessOptions::default()).await;
    let (owner, me) = (harness::random_profile(), harness::random_profile());
    let group = h.create_private_group(&owner).await;
    h.invite(&group, &owner, &me).await.unwrap();
    h.join(&group, &me).await.unwrap();
    h.send_text(&group, &me, "mine").await;
    h.send_text(&group, &owner, "theirs").await;
    assert_eq!(h.inbox(&me, proto::InboxFolder::Inbox).await.len(), 1);

    leave(&h, &group, &me).await.unwrap();
    // Leave the departure its own millisecond.
    tokio::time::sleep(std::time::Duration::from_millis(5)).await;
    h.send_text(&group, &owner, "after you left").await;

    assert!(!h.roster(&group, &owner).await.contains(&me.as_str()), "off the roster");
    assert!(h.inbox(&me, proto::InboxFolder::Inbox).await.is_empty(), "out of the inbox");
    let ended = membership(&h, &me, &group).await.expect("the membership is kept");
    assert!(ended.left_at_ms > 0);
    let err = ChatService::get_history(
        &h.handler,
        Request::new(proto::GetHistoryRequest {
            conversation_id: group.as_str(),
            requester_id:    me.as_str(),
            limit:           10,
            page_token:      String::new(),
        }),
    )
    .await
    .unwrap_err();
    assert_eq!(err.code(), Code::NotFound, "no longer a member: concealed");

    let mut seen = former_history(&h, &group, &me).await;
    seen.sort();
    assert_eq!(seen, vec![(String::new(), String::new()), (me.as_str(), "mine".to_owned())]);

    // Back in: left_at cleared.
    h.invite(&group, &owner, &me).await.unwrap();
    h.join(&group, &me).await.unwrap();
    assert_eq!(membership(&h, &me, &group).await.unwrap().left_at_ms, 0);
}

#[tokio::test]
async fn the_owner_stays_and_an_outsider_learns_nothing() {
    let h = TestHarness::start(HarnessOptions::default()).await;
    let owner = harness::random_profile();
    let group = h.create_private_group(&owner).await;
    assert_eq!(leave(&h, &group, &owner).await.unwrap_err().code(), Code::FailedPrecondition);
    assert_eq!(leave(&h, &group, &harness::random_profile()).await.unwrap_err().code(), Code::NotFound);

    let peer = harness::random_profile();
    let (dm, _) = h.open_direct(&owner, &peer).await.unwrap();
    assert_eq!(leave(&h, &dm, &peer).await.unwrap_err().code(), Code::FailedPrecondition, "a DM is never left");
}
