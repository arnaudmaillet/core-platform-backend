//! #653 over live Scylla: the conversations a profile is a member of, paged
//! by id — what the GDPR data export walks before reading each one's history.
//! The reverse index follows the roster (one logged batch); memberships from
//! before it existed are found once the backfill ran.

use uuid::Uuid;

use chat::application::port::MemberRepository;

use crate::chat_it::harness::{self, proto, ChatService, HarnessOptions, Request, TestHarness};

async fn conversations_of(h: &TestHarness, member: &str) -> Vec<String> {
    let (mut ids, mut token) = (Vec::new(), String::new());
    loop {
        let page = ChatService::list_conversations_by_member(
            &h.handler,
            Request::new(proto::ListConversationsByMemberRequest {
                member_id: member.to_owned(),
                limit: 2,
                page_token: token.clone(),
            }),
        )
        .await
        .expect("list by member")
        .into_inner();
        ids.extend(page.memberships.iter().map(|m| m.conversation_id.clone()));
        if page.next_page_token.is_empty() {
            return ids;
        }
        token = page.next_page_token;
    }
}

#[tokio::test]
async fn a_profiles_conversations_page_by_id_and_follow_the_roster() {
    let h = TestHarness::start(HarnessOptions::default()).await;
    let (owner, other) = (harness::random_profile(), harness::random_profile());
    let mut mine = Vec::new();
    for _ in 0..3 {
        mine.push(h.create_private_group(&owner).await.as_str());
    }
    let theirs = h.create_private_group(&other).await;
    mine.sort();

    assert_eq!(conversations_of(&h, &owner.as_str()).await, mine, "all of mine, by id, none of theirs");
    // The owner's role rides along.
    let page = ChatService::list_conversations_by_member(
        &h.handler,
        Request::new(proto::ListConversationsByMemberRequest { member_id: other.as_str(), limit: 10, page_token: String::new() }),
    )
    .await
    .unwrap()
    .into_inner();
    assert_eq!(page.memberships.len(), 1);
    assert_eq!(page.memberships[0].conversation_id, theirs.as_str());
    assert_eq!(page.memberships[0].role, proto::Role::Owner as i32);

    // Leaving the roster leaves the index too.
    let first = chat::domain::value_object::ConversationId::try_from(mine[0].as_str()).unwrap();
    h.member_repo.delete(&first, &owner).await.unwrap();
    assert_eq!(conversations_of(&h, &owner.as_str()).await, mine[1..].to_vec());
}

#[tokio::test]
async fn memberships_from_before_the_index_are_found_after_the_backfill() {
    let h = TestHarness::start(HarnessOptions::default()).await;
    let owner = harness::random_profile();
    let conv = h.create_private_group(&owner).await;
    h.scylla
        .session
        .execute_unpaged(
            "DELETE FROM chat.conversations_by_member WHERE member_id = ?",
            (Uuid::parse_str(&owner.as_str()).unwrap(),),
        )
        .await
        .unwrap();
    assert!(conversations_of(&h, &owner.as_str()).await.is_empty());

    assert!(h.member_repo.backfill_member_index().await.unwrap() >= 1);
    h.member_repo.backfill_member_index().await.unwrap();
    assert_eq!(conversations_of(&h, &owner.as_str()).await, vec![conv.as_str()]);
}
