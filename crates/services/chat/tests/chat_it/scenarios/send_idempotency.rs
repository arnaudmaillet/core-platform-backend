//! #875 over live Scylla + Redis: a retried `SendMessage` with the same
//! idempotency key stores one message and both calls answer its id; another
//! key, or none, sends as before.

use tonic::Code;

use crate::chat_it::harness::{self, proto, ChatService, ConversationId, HarnessOptions, ProfileId, Request, TestHarness};

async fn send(h: &TestHarness, conv: &ConversationId, sender: &ProfileId, body: &str, key: &str) -> Result<String, tonic::Status> {
    ChatService::send_message(
        &h.handler,
        Request::new(proto::SendMessageRequest {
            conversation_id: conv.as_str(),
            sender_id:       sender.as_str(),
            content_type:    0, // Text
            body:            body.to_owned(),
            media_ref:       String::new(),
            reply_to:        String::new(),
            idempotency_key: key.to_owned(),
        }),
    )
    .await
    .map(|r| r.into_inner().message_id)
}

#[tokio::test]
async fn a_retried_send_with_the_same_key_stores_one_message() {
    let h = TestHarness::start(HarnessOptions::default()).await;
    let owner = harness::random_profile();
    let conv = h.create_private_group(&owner).await;
    let key = uuid::Uuid::now_v7().to_string();

    let first = send(&h, &conv, &owner, "hello", &key).await.expect("first send");
    let retry = send(&h, &conv, &owner, "hello", &key).await.expect("retry");
    assert_eq!(retry, first, "the retry answers the first message");
    assert_eq!(h.bodies(&conv, &owner).await, vec!["hello"]);

    // Another key and no key are new messages.
    let other = send(&h, &conv, &owner, "again", &uuid::Uuid::now_v7().to_string()).await.expect("other key");
    let unkeyed = send(&h, &conv, &owner, "again", "").await.expect("no key");
    assert_ne!(other, first);
    assert_ne!(unkeyed, other);
    assert_eq!(h.bodies(&conv, &owner).await.len(), 3);
}

#[tokio::test]
async fn a_malformed_key_is_refused() {
    let h = TestHarness::start(HarnessOptions::default()).await;
    let owner = harness::random_profile();
    let conv = h.create_private_group(&owner).await;

    let err = send(&h, &conv, &owner, "hello", "bad key!").await.unwrap_err();
    assert_eq!(err.code(), Code::FailedPrecondition, "{err:?}");
    assert!(h.bodies(&conv, &owner).await.is_empty());
}
