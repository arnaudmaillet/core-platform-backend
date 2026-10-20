//! #930 over live Scylla: incremental sync. A client lists its inbox (or a
//! thread) once, then asks only for what changed since the watermark it was
//! handed, past the server's grace window.

use std::time::Duration;

use crate::chat_it::harness::{self, proto, ChatService, HarnessOptions, Request, TestHarness};

/// Past `chat::application::sync::GRACE` (5 s).
const PAST_GRACE: Duration = Duration::from_secs(6);

#[tokio::test]
async fn the_inbox_and_a_thread_sync_from_their_watermarks() {
    let h = TestHarness::start(HarnessOptions::default()).await;
    let (me, friend) = (harness::random_profile(), harness::random_profile());
    let (dm, _) = h.open_direct(&me, &friend).await.unwrap();
    h.send_text(&dm, &friend, "before").await;
    // The watermarks handed out lag the grace window: let "before" age past it.
    tokio::time::sleep(PAST_GRACE).await;

    let listed = ChatService::list_inbox(
        &h.handler,
        Request::new(proto::ListInboxRequest {
            profile_id: me.as_str(),
            folder:     proto::InboxFolder::Inbox as i32,
            limit:      50,
            page_token: String::new(),
        }),
    )
    .await
    .unwrap()
    .into_inner();
    let history = ChatService::get_history(
        &h.handler,
        Request::new(proto::GetHistoryRequest {
            conversation_id: dm.as_str(),
            requester_id:    me.as_str(),
            limit:           50,
            page_token:      String::new(),
        }),
    )
    .await
    .unwrap()
    .into_inner();
    assert!(listed.sync_watermark.starts_with("i1_"), "{}", listed.sync_watermark);
    assert!(history.sync_watermark.starts_with("t1_"), "{}", history.sync_watermark);

    let group = h.create_private_group(&me).await;
    h.send_text(&dm, &friend, "after").await;
    tokio::time::sleep(PAST_GRACE).await;

    let inbox = ChatService::sync_inbox(
        &h.handler,
        Request::new(proto::SyncInboxRequest {
            profile_id: me.as_str(),
            watermark:  listed.sync_watermark,
            limit:      50,
        }),
    )
    .await
    .unwrap()
    .into_inner();
    let mut changed: Vec<_> = inbox.changed.iter().map(|c| c.entry.as_ref().unwrap().conversation_id.clone()).collect();
    changed.sort();
    let mut expected = vec![dm.as_str(), group.as_str()];
    expected.sort();
    assert_eq!(changed, expected);
    assert!(inbox.changed.iter().all(|c| c.folder == proto::InboxFolder::Inbox as i32));
    assert!(inbox.removed.is_empty() && !inbox.has_more);

    let thread = ChatService::sync_conversation(
        &h.handler,
        Request::new(proto::SyncConversationRequest {
            conversation_id: dm.as_str(),
            requester_id:    me.as_str(),
            watermark:       history.sync_watermark,
            limit:           50,
        }),
    )
    .await
    .unwrap()
    .into_inner();
    let bodies: Vec<_> = thread.messages.iter().map(|m| m.body.as_str()).collect();
    assert_eq!(bodies, vec!["after"], "only what is new");
    assert_eq!(thread.read_receipts.len(), 2, "both members' read positions");

    // Nothing since the last sync.
    let again = ChatService::sync_inbox(
        &h.handler,
        Request::new(proto::SyncInboxRequest { profile_id: me.as_str(), watermark: inbox.watermark, limit: 50 }),
    )
    .await
    .unwrap()
    .into_inner();
    assert!(again.changed.is_empty() && again.removed.is_empty());

    // A watermark past its 35 days: resync in full.
    let stale = format!("i1_{}_{}", harness::now_ms() - 36 * 86_400_000, uuid::Uuid::nil());
    let err = ChatService::sync_inbox(
        &h.handler,
        Request::new(proto::SyncInboxRequest { profile_id: me.as_str(), watermark: stale, limit: 50 }),
    )
    .await
    .unwrap_err();
    assert_eq!(err.code(), tonic::Code::OutOfRange, "{err:?}");
}
