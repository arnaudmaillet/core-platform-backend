use std::sync::Arc;

use cqrs::{Envelope, Query, QueryHandler};
use futures::{StreamExt, TryStreamExt};

use crate::application::command::direct::verdict;
use crate::application::port::{
    ConversationRepository, Folder, InboxEntry, InboxStore, InteractionGate, MemberRepository, MessageVerdict,
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
}

impl ListInboxHandler {
    /// The entry as `me` sees it, or `None` when it must not show: a request
    /// whose sender `me` blocks, or is blocked by (a block that came after the
    /// request), or one no longer awaiting `me`.
    async fn resolve(&self, me: ProfileId, entry: InboxEntry) -> Result<Option<InboxItem>, ChatError> {
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

        let entries = self.inbox.list(&me, q.folder, limit, after).await?;
        // The cursor is the store's: a page whose entries are filtered out
        // comes back short, never skips.
        let next_page_token = match entries.last() {
            Some(last) if entries.len() == limit as usize => {
                Some(format!("{}_{}", last.activity.timestamp_millis(), last.conversation_id.as_str()))
            }
            _ => None,
        };
        let items: Vec<Option<InboxItem>> = futures::stream::iter(entries)
            .map(|entry| self.resolve(me, entry))
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
    use crate::domain::value_object::{MessageId, Role};

    fn pid() -> ProfileId {
        ProfileId::from_uuid(uuid::Uuid::now_v7())
    }

    struct World {
        conversations: Arc<FakeConversations>,
        members:       Arc<FakeMembers>,
        gate:          Arc<ScriptedGate>,
        projector:     InboxProjector,
        handler:       ListInboxHandler,
    }

    fn world() -> World {
        let conversations = Arc::<FakeConversations>::default();
        let members = Arc::<FakeMembers>::default();
        let inbox = Arc::<FakeInbox>::default();
        let gate = Arc::<ScriptedGate>::default();
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
        };
        World { conversations, members, gate, projector, handler }
    }

    impl World {
        async fn request(&self, from: ProfileId, to: ProfileId, at_ms: i64) -> ConversationId {
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
                    body:            "hi".into(),
                    media_ref:       None,
                    reply_to:        None,
                    created_at_ms:   at_ms,
                    withheld:        false,
                    request:         true,
                })
                .await
                .unwrap();
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
}
