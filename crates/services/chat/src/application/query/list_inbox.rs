use std::sync::Arc;

use cqrs::{Envelope, Query, QueryHandler};
use futures::{StreamExt, TryStreamExt};

use crate::application::command::direct::verdict;
use text_filter::{ContentFilter, TermList};

use crate::application::port::{
    ConversationRepository, Folder, InboxEntry, InboxStore, InteractionGate, MemberRepository, MessageFilterStore,
    MessageRepository, MessageVerdict,
};
use crate::domain::value_object::{ConversationId, ProfileId};
use crate::error::ChatError;

/// A member's inbox, one folder, newest activity first (#656).
pub struct ListInboxQuery {
    pub profile_id: String,
    pub folder:     Folder,
    pub limit:      i32,
    /// The `next_page_token` of the previous page: `"{activity_ms}_{id}"`.
    pub page_token: Option<String>,
}

/// An inbox entry as its member sees it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct InboxItem {
    pub entry:   InboxEntry,
    /// The last message is someone else's and newer than the member's read
    /// position. A request's recipient keeps none: always unread until they
    /// answer.
    pub unread:  bool,
    /// The member's own request, unanswered — a declined one, or one to
    /// someone who blocks them, looks the same.
    pub request: bool,
}

pub struct InboxPage {
    pub items:           Vec<InboxItem>,
    pub next_page_token: Option<String>,
}

impl Query for ListInboxQuery {
    type Response = InboxPage;
}

/// Entries resolved in flight per page.
const RESOLVES: usize = 16;

pub struct ListInboxHandler {
    pub conversation_repo: Arc<dyn ConversationRepository>,
    pub member_repo:       Arc<dyn MemberRepository>,
    pub inbox:             Arc<dyn InboxStore>,
    pub gate:              Option<Arc<dyn InteractionGate>>,
    pub max_page_size:     i32,
    /// Hidden words / offensive filter on requests (#810).
    pub filters:           Arc<dyn MessageFilterStore>,
    pub messages:          Arc<dyn MessageRepository>,
    pub offensive:         Arc<TermList>,
}

/// Messages of a request read to sort it (a request holds one until answered).
const REQUEST_MESSAGES: i32 = 5;

impl ListInboxHandler {
    /// The entry as `me` sees it, or `None` when it must not show: a request
    /// whose sender `me` blocks, or is blocked by (a block that came after the
    /// request), or one no longer awaiting `me`.
    /// Does `filter` catch the request in `entry` (any of its sender's
    /// messages, read whole — the entry keeps a preview only)?
    async fn caught(&self, filter: &ContentFilter, entry: &InboxEntry, requester: ProfileId) -> Result<bool, ChatError> {
        let (messages, _) = self.messages.list_history(&entry.conversation_id, REQUEST_MESSAGES, None, None).await?;
        Ok(messages
            .iter()
            .filter(|m| m.sender_id == requester.as_uuid())
            .any(|m| filter.hides(&m.body, &self.offensive)))
    }

    async fn resolve(
        &self,
        me: ProfileId,
        folder: Folder,
        filter: Option<&ContentFilter>,
        entry: InboxEntry,
    ) -> Result<Option<InboxItem>, ChatError> {
        let id = entry.conversation_id;
        let (member, conversation, current) = tokio::join!(
            self.member_repo.find(&id, &me),
            self.conversation_repo.find(&id),
            self.inbox.find(&me, &id),
        );
        let (Some(member), Some(conversation)) = (member?, conversation?) else {
            return Ok(None);
        };
        // A listing row a concurrent move left behind: the entry has moved on.
        if current?.is_none_or(|c| c.folder != entry.folder || c.activity != entry.activity) {
            return Ok(None);
        }
        let request = conversation.request();
        if entry.folder == Folder::Requests {
            let Some(requester) = request.requester().filter(|r| *r != me && request.is_unanswered()) else {
                return Ok(None);
            };
            if verdict(self.gate.as_ref(), &requester, &me).await? == MessageVerdict::Silenced {
                return Ok(None);
            }
            // Hidden requests (#810): those the member's filter catches go to
            // their own folder, the rest stay in Requests.
            let caught = match filter {
                Some(filter) => self.caught(filter, &entry, requester).await?,
                None => false,
            };
            if caught != (folder == Folder::HiddenRequests) {
                return Ok(None);
            }
        }
        let unread = entry.last.as_ref().is_some_and(|last| {
            last.sender_id != me && member.last_read().is_none_or(|read| last.message_id > read)
        });
        let own_request = request.is_unanswered() && request.requester() == Some(me);
        Ok(Some(InboxItem { entry, unread, request: own_request }))
    }
}

impl QueryHandler<ListInboxQuery> for ListInboxHandler {
    type Error = ChatError;

    async fn handle(&self, envelope: Envelope<ListInboxQuery>) -> Result<InboxPage, ChatError> {
        let q = &envelope.payload;
        let me = ProfileId::try_from(q.profile_id.as_str())?;
        let limit = q.limit.clamp(1, self.max_page_size.max(1));
        let after = q.page_token.as_deref().map(decode_cursor).transpose()?;

        let entries = self.inbox.list(&me, q.folder.stored(), limit, after).await?;
        // The member's filter, once per page (fail closed: unreadable, the
        // read fails). One that hides nothing skips reading the messages.
        let filter = if q.folder.stored() == Folder::Requests {
            Some(self.filters.get(&me).await?)
                .filter(|f| !f.hidden_words.is_empty() || (f.filter_offensive && !self.offensive.is_empty()))
        } else {
            None
        };
        // The cursor is the store's: a page whose entries are filtered out
        // comes back short, never skips.
        let next_page_token = match entries.last() {
            Some(last) if entries.len() == limit as usize => {
                Some(format!("{}_{}", last.activity.timestamp_millis(), last.conversation_id.as_str()))
            }
            _ => None,
        };
        let items: Vec<Option<InboxItem>> = futures::stream::iter(entries)
            .map(|entry| self.resolve(me, q.folder, filter.as_ref(), entry))
            .buffered(RESOLVES)
            .try_collect()
            .await?;
        Ok(InboxPage { items: items.into_iter().flatten().collect(), next_page_token })
    }
}

fn decode_cursor(token: &str) -> Result<(i64, ConversationId), ChatError> {
    let invalid = || ChatError::InvalidPageToken { token: token.to_owned() };
    let (ms, id) = token.split_once('_').ok_or_else(invalid)?;
    Ok((ms.parse().map_err(|_| invalid())?, ConversationId::try_from(id).map_err(|_| invalid())?))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::application::command::fakes::{FakeConversations, FakeInbox, FakeMembers, ScriptedGate};
    use crate::application::command::inbox::InboxProjector;
    use crate::domain::aggregate::{Conversation, Participant};
    use crate::domain::event::MessageSentEvent;
    use std::collections::HashMap;
    use std::sync::Mutex;

    use async_trait::async_trait;
    use chrono::{TimeZone, Utc};

    use crate::application::port::MessageSummary;
    use crate::domain::aggregate::Message;
    use crate::domain::value_object::{ContentType, MessageId, Role};

    /// Each conversation's messages (newest first).
    #[derive(Default)]
    struct Logs(Mutex<HashMap<ConversationId, Vec<MessageSummary>>>);

    #[async_trait]
    impl MessageRepository for Logs {
        async fn insert(&self, _: &Message) -> Result<(), ChatError> {
            Ok(())
        }
        async fn list_history(
            &self,
            id: &ConversationId,
            _: i32,
            _: Option<(i64, uuid::Uuid)>,
            _: Option<i64>,
        ) -> Result<(Vec<MessageSummary>, Option<(i64, uuid::Uuid)>), ChatError> {
            Ok((self.0.lock().unwrap().get(id).cloned().unwrap_or_default(), None))
        }
    }

    /// One member's filter; `down` fails every read.
    #[derive(Default)]
    struct Filters {
        filter: Mutex<ContentFilter>,
        down:   Mutex<bool>,
    }

    #[async_trait]
    impl MessageFilterStore for Filters {
        async fn set(&self, _: &ProfileId, filter: &ContentFilter) -> Result<(), ChatError> {
            *self.filter.lock().unwrap() = filter.clone();
            Ok(())
        }
        async fn get(&self, _: &ProfileId) -> Result<ContentFilter, ChatError> {
            if *self.down.lock().unwrap() {
                return Err(ChatError::InvalidPageToken { token: "filters down".into() });
            }
            Ok(self.filter.lock().unwrap().clone())
        }
    }

    fn pid() -> ProfileId {
        ProfileId::from_uuid(uuid::Uuid::now_v7())
    }

    struct World {
        conversations: Arc<FakeConversations>,
        members:       Arc<FakeMembers>,
        gate:          Arc<ScriptedGate>,
        logs:          Arc<Logs>,
        filters:       Arc<Filters>,
        projector:     InboxProjector,
        handler:       ListInboxHandler,
    }

    fn world() -> World {
        let conversations = Arc::<FakeConversations>::default();
        let members = Arc::<FakeMembers>::default();
        let inbox = Arc::<FakeInbox>::default();
        let gate = Arc::<ScriptedGate>::default();
        let logs = Arc::<Logs>::default();
        let filters = Arc::<Filters>::default();
        let projector = InboxProjector {
            conversation_repo: Arc::clone(&conversations) as Arc<dyn ConversationRepository>,
            member_repo:       Arc::clone(&members) as Arc<dyn MemberRepository>,
            inbox:             Arc::clone(&inbox) as Arc<dyn InboxStore>,
        };
        let handler = ListInboxHandler {
            conversation_repo: Arc::clone(&conversations) as Arc<dyn ConversationRepository>,
            member_repo:       Arc::clone(&members) as Arc<dyn MemberRepository>,
            inbox:             Arc::clone(&inbox) as Arc<dyn InboxStore>,
            gate:              Some(Arc::clone(&gate) as Arc<dyn InteractionGate>),
            max_page_size:     50,
            filters:           Arc::clone(&filters) as Arc<dyn MessageFilterStore>,
            messages:          Arc::clone(&logs) as Arc<dyn MessageRepository>,
            offensive:         Arc::new(TermList::new(["badword".to_owned()])),
        };
        World { conversations, members, gate, logs, filters, projector, handler }
    }

    impl World {
        async fn request(&self, from: ProfileId, to: ProfileId, at_ms: i64) -> ConversationId {
            self.request_saying(from, to, at_ms, "hi").await
        }

        async fn request_saying(&self, from: ProfileId, to: ProfileId, at_ms: i64, body: &str) -> ConversationId {
            let c = Conversation::open_direct(ConversationId::new(), from, to, true);
            self.conversations.insert_direct(&c).await.unwrap();
            for p in [from, to] {
                self.members.insert(&c.id(), &Participant::new(p, Role::Member).unwrap()).await.unwrap();
            }
            self.projector
                .on_message(&MessageSentEvent {
                    conversation_id: c.id().as_str(),
                    message_id:      MessageId::new().as_str(),
                    sender_id:       from.as_str(),
                    content_type:    "text".into(),
                    body:            body.into(),
                    media_ref:       None,
                    reply_to:        None,
                    created_at_ms:   at_ms,
                    withheld:        false,
                    request:         true,
                })
                .await
                .unwrap();
            self.logs.0.lock().unwrap().insert(c.id(), vec![MessageSummary {
                message_id:   uuid::Uuid::now_v7(),
                sender_id:    from.as_uuid(),
                content_type: ContentType::Text,
                body:         body.into(),
                media_ref:    None,
                reply_to:     None,
                created_at:   Utc.timestamp_millis_opt(at_ms).unwrap(),
                withheld:     false,
            }]);
            c.id()
        }

        async fn list(&self, me: ProfileId, folder: Folder) -> InboxPage {
            let query = ListInboxQuery { profile_id: me.as_str(), folder, limit: 10, page_token: None };
            self.handler.handle(Envelope::new(uuid::Uuid::now_v7(), query)).await.unwrap()
        }
    }

    #[tokio::test]
    async fn requests_show_newest_first_unread_and_the_requester_sees_their_own_as_a_request() {
        let w = world();
        let (me, a, b) = (pid(), pid(), pid());
        let older = w.request(a, me, 1_000).await;
        let newer = w.request(b, me, 2_000).await;

        let requests = w.list(me, Folder::Requests).await;
        let ids: Vec<_> = requests.items.iter().map(|i| i.entry.conversation_id).collect();
        assert_eq!(ids, vec![newer, older]);
        assert!(requests.items.iter().all(|i| i.unread && !i.request));
        assert!(w.list(me, Folder::Inbox).await.items.is_empty());

        let theirs = w.list(a, Folder::Inbox).await;
        assert_eq!(theirs.items.len(), 1);
        assert!(theirs.items[0].request, "their request, unanswered");
        assert!(!theirs.items[0].unread, "their own message");
    }

    /// A block after the request: the blocker's requests no longer show it.
    #[tokio::test]
    async fn a_request_from_someone_blocked_never_shows() {
        let w = world();
        let (me, blocked) = (pid(), pid());
        w.request(blocked, me, 1_000).await;
        assert_eq!(w.list(me, Folder::Requests).await.items.len(), 1);
        w.gate.set(blocked, me, MessageVerdict::Silenced);
        assert!(w.list(me, Folder::Requests).await.items.is_empty());
    }

    #[tokio::test]
    async fn pages_follow_the_cursor() {
        let w = world();
        let me = pid();
        for at in 1..=3 {
            w.request(pid(), me, at * 1_000).await;
        }
        let query = |token| ListInboxQuery { profile_id: me.as_str(), folder: Folder::Requests, limit: 2, page_token: token };
        let first = w.handler.handle(Envelope::new(uuid::Uuid::now_v7(), query(None))).await.unwrap();
        assert_eq!(first.items.len(), 2);
        let second = w.handler.handle(Envelope::new(uuid::Uuid::now_v7(), query(first.next_page_token))).await.unwrap();
        assert_eq!(second.items.len(), 1);
        assert!(second.next_page_token.is_none());
        assert_eq!(second.items[0].entry.activity.timestamp_millis(), 1_000);
    }

    /// Hidden words and the offensive filter (#810): caught requests leave
    /// Requests for Hidden requests, sorted with the member's current filter.
    #[tokio::test]
    async fn requests_caught_by_the_members_filter_are_hidden_requests() {
        let w = world();
        let (me, a, b, c) = (pid(), pid(), pid(), pid());
        let plain = w.request_saying(a, me, 1_000, "hello there").await;
        // Past the 100-char preview: the whole message is read.
        let long = format!("{} SPOILER ahead", "x".repeat(150));
        let spoiler = w.request_saying(b, me, 2_000, &long).await;
        let rude = w.request_saying(c, me, 3_000, "you badword").await;
        let ids = |page: InboxPage| page.items.into_iter().map(|i| i.entry.conversation_id).collect::<Vec<_>>();

        // The default: offensive filter on, no hidden words.
        assert_eq!(ids(w.list(me, Folder::Requests).await), vec![spoiler, plain]);
        assert_eq!(ids(w.list(me, Folder::HiddenRequests).await), vec![rude]);

        w.filters.set(&me, &ContentFilter { hidden_words: vec!["spoiler".into()], filter_offensive: true }).await.unwrap();
        assert_eq!(ids(w.list(me, Folder::Requests).await), vec![plain]);
        assert_eq!(ids(w.list(me, Folder::HiddenRequests).await), vec![rude, spoiler]);

        // A change re-sorts at once: offensive filter off.
        w.filters.set(&me, &ContentFilter { hidden_words: vec!["spoiler".into()], filter_offensive: false }).await.unwrap();
        assert_eq!(ids(w.list(me, Folder::Requests).await), vec![rude, plain]);
        assert_eq!(ids(w.list(me, Folder::HiddenRequests).await), vec![spoiler]);

        // The inbox is never filtered; an unreadable filter fails the read.
        assert!(w.list(me, Folder::Inbox).await.items.is_empty());
        *w.filters.down.lock().unwrap() = true;
        let query = ListInboxQuery { profile_id: me.as_str(), folder: Folder::Requests, limit: 10, page_token: None };
        assert!(w.handler.handle(Envelope::new(uuid::Uuid::now_v7(), query)).await.is_err());
    }
}
