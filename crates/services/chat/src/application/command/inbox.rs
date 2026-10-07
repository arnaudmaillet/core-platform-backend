//! Each member's inbox (#656), projected from chat's own facts: a delivered
//! message brings its conversation to the top for every member who may see
//! it; a join (or a group's creation) puts the conversation in; leaving takes
//! it out.
//!
//! Folders follow the conversation's request: the recipient of a pending
//! request finds it in [`Folder::Requests`], its requester in their inbox; a
//! declined request leaves the decliner's inbox. A **withheld** message — its
//! sender is blocked — moves only its sender's own entry: the blocker never
//! gets an entry from them, so a blocked request never shows in their
//! requests.
//!
//! Applied at least once (the Kafka worker) or inline (no broker): an event
//! older than the entry it would overwrite is ignored, so redelivery and
//! replay never move an entry back.

use std::sync::Arc;

use chrono::{DateTime, Utc};
use futures::{StreamExt, TryStreamExt};

use crate::application::port::{
    ConversationRepository, Folder, InboxEntry, InboxStore, LastMessage, MemberRepository, MessagePush, MessagePushes,
    PREVIEW_CHARS,
};
use crate::domain::aggregate::Conversation;
use crate::domain::event::{DomainEvent, MessageSentEvent};
use crate::domain::value_object::{ContentType, ConversationId, MessageId, MessageRequest, ProfileId};
use crate::error::ChatError;

/// Where `member` keeps `conversation`, or `None` when it is not theirs to
/// see (a request they declined).
pub fn folder_for(conversation: &Conversation, member: ProfileId) -> Option<Folder> {
    match conversation.request() {
        MessageRequest::Open => Some(Folder::Inbox),
        MessageRequest::Pending { requester } if requester == member => Some(Folder::Inbox),
        MessageRequest::Pending { .. } => Some(Folder::Requests),
        MessageRequest::Declined { requester, .. } if requester == member => Some(Folder::Inbox),
        MessageRequest::Declined { .. } => None,
    }
}

/// Roster entries written at once per message.
const PUTS_IN_FLIGHT: usize = 16;

fn at(ms: i64) -> DateTime<Utc> {
    DateTime::from_timestamp_millis(ms).unwrap_or_default()
}

/// Projects chat's facts into [`InboxStore`].
pub struct InboxProjector {
    pub conversation_repo: Arc<dyn ConversationRepository>,
    pub member_repo:       Arc<dyn MemberRepository>,
    pub inbox:             Arc<dyn InboxStore>,
    /// Asks for the message's push (#654) for the members it reaches.
    pub pushes:            Arc<dyn MessagePushes>,
}

impl InboxProjector {
    /// A message was sent: up front for every member who may see it.
    pub async fn on_message(&self, event: &MessageSentEvent) -> Result<(), ChatError> {
        let conversation_id = ConversationId::try_from(event.conversation_id.as_str())?;
        let sender = ProfileId::try_from(event.sender_id.as_str())?;
        let last = LastMessage {
            message_id:   MessageId::try_from(event.message_id.as_str())?,
            sender_id:    sender,
            content_type: ContentType::try_from(event.content_type.as_str())?,
            preview:      event.body.chars().take(PREVIEW_CHARS).collect(),
        };
        let Some(conversation) = self.conversation_repo.find(&conversation_id).await? else {
            return Ok(());
        };
        let conversation = &conversation;
        let last = &last;
        let now = Utc::now();
        let mut entries = Vec::new();
        let mut recipients = Vec::new();
        for participant in self.member_repo.list(&conversation_id).await? {
            let member = participant.profile_id();
            // A withheld message is its sender's alone.
            if event.withheld && member != sender {
                continue;
            }
            let Some(folder) = folder_for(conversation, member) else { continue };
            // Pushed to the members it reaches in their inbox (a request
            // waits in its recipient's requests, unpushed) who did not mute it.
            if member != sender && folder == Folder::Inbox && !participant.is_muted(now) {
                recipients.push(member.as_str());
            }
            entries.push((member, folder));
        }
        // A group's roster (up to 500) is written a few entries at a time: each
        // `put` is idempotent and per member.
        futures::stream::iter(entries)
            .map(Ok::<_, ChatError>)
            .try_for_each_concurrent(PUTS_IN_FLIGHT, |(member, folder)| async move {
                let entry = InboxEntry {
                    conversation_id,
                    kind: conversation.kind(),
                    peer: conversation.peer_of(member),
                    folder,
                    activity: at(event.created_at_ms),
                    last: Some(last.clone()),
                };
                self.inbox.put(&member, &entry).await
            })
            .await?;

        // Once the inbox is written: a failure retries the whole projection
        // (idempotent), and notification sends a message's push once.
        if recipients.is_empty() || last.content_type == ContentType::System {
            return Ok(());
        }
        let push = MessagePush {
            message_id:        event.message_id.clone(),
            conversation_id:   event.conversation_id.clone(),
            conversation_kind: conversation.kind().as_str().to_owned(),
            sender_id:         event.sender_id.clone(),
            content_type:      last.content_type.as_str().to_owned(),
            preview:           if last.content_type == ContentType::Text { last.preview.clone() } else { String::new() },
            recipients,
            created_at_ms:     event.created_at_ms,
        };
        self.pushes.request(&push).await
    }

    /// A conversation's lifecycle: its owner in at creation, members in as
    /// they join, out as they leave. Direct conversations enter an inbox with
    /// their first delivered message only.
    pub async fn on_conversation(&self, event: &DomainEvent) -> Result<(), ChatError> {
        match event {
            DomainEvent::ConversationCreated(e) => self.joined(&e.conversation_id, &e.owner_id, e.created_at_ms).await,
            DomainEvent::MemberJoined(e) => self.joined(&e.conversation_id, &e.profile_id, e.joined_at_ms).await,
            DomainEvent::MemberLeft(e) => {
                let conversation_id = ConversationId::try_from(e.conversation_id.as_str())?;
                self.inbox.remove(&ProfileId::try_from(e.profile_id.as_str())?, &conversation_id).await
            }
            DomainEvent::ConversationPublished(_) | DomainEvent::ConversationUnpublished(_) => Ok(()),
        }
    }

    async fn joined(&self, conversation_id: &str, member: &str, at_ms: i64) -> Result<(), ChatError> {
        let conversation_id = ConversationId::try_from(conversation_id)?;
        let member = ProfileId::try_from(member)?;
        let Some(conversation) = self.conversation_repo.find(&conversation_id).await? else {
            return Ok(());
        };
        if conversation.is_direct() {
            return Ok(());
        }
        let entry = InboxEntry {
            conversation_id,
            kind: conversation.kind(),
            peer: None,
            folder: Folder::Inbox,
            activity: at(at_ms),
            last: None,
        };
        self.inbox.put(&member, &entry).await
    }
}

/// The recipient answered a request (#656): accepted, their entry moves to
/// the inbox; declined, it leaves their inbox. Nothing changes for the
/// requester.
pub(crate) async fn file_answer(
    inbox:           &dyn InboxStore,
    recipient:       &ProfileId,
    conversation_id: &ConversationId,
    accepted:        bool,
) -> Result<(), ChatError> {
    if !accepted {
        return inbox.remove(recipient, conversation_id).await;
    }
    if let Some(mut entry) = inbox.find(recipient, conversation_id).await?
        && entry.folder != Folder::Inbox
    {
        entry.folder = Folder::Inbox;
        inbox.put(recipient, &entry).await?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::application::command::fakes::{FakeConversations, FakeInbox, FakeMembers, FakePushes};
    use crate::domain::aggregate::Participant;
    use crate::domain::event::{ConversationCreatedEvent, MemberJoinedEvent, MemberLeftEvent};
    use crate::domain::value_object::{ConversationKind, Role};

    fn pid() -> ProfileId {
        ProfileId::from_uuid(uuid::Uuid::now_v7())
    }

    struct World {
        conversations: Arc<FakeConversations>,
        members:       Arc<FakeMembers>,
        inbox:         Arc<FakeInbox>,
        pushes:        Arc<FakePushes>,
        projector:     InboxProjector,
    }

    fn world() -> World {
        let (conversations, members, inbox) =
            (Arc::<FakeConversations>::default(), Arc::<FakeMembers>::default(), Arc::<FakeInbox>::default());
        let pushes = Arc::<FakePushes>::default();
        let projector = InboxProjector {
            conversation_repo: Arc::clone(&conversations) as Arc<dyn ConversationRepository>,
            member_repo:       Arc::clone(&members) as Arc<dyn MemberRepository>,
            inbox:             Arc::clone(&inbox) as Arc<dyn InboxStore>,
            pushes:            Arc::clone(&pushes) as Arc<dyn MessagePushes>,
        };
        World { conversations, members, inbox, pushes, projector }
    }

    impl World {
        async fn direct(&self, opener: ProfileId, peer: ProfileId, request: bool) -> ConversationId {
            let c = Conversation::open_direct(ConversationId::new(), opener, peer, request);
            self.conversations.insert_direct(&c).await.unwrap();
            for p in [opener, peer] {
                self.members.insert(&c.id(), &Participant::new(p, Role::Member).unwrap()).await.unwrap();
            }
            c.id()
        }

        async fn sent(&self, id: ConversationId, sender: ProfileId, at_ms: i64, withheld: bool) {
            self.sent_as(id, sender, at_ms, withheld, "text").await;
        }

        async fn sent_as(&self, id: ConversationId, sender: ProfileId, at_ms: i64, withheld: bool, content_type: &str) {
            let event = MessageSentEvent {
                conversation_id: id.as_str(),
                message_id:      MessageId::new().as_str(),
                sender_id:       sender.as_str(),
                content_type:    content_type.to_owned(),
                body:            "x".repeat(150),
                media_ref:       None,
                reply_to:        None,
                created_at_ms:   at_ms,
                withheld,
                request:         false,
            };
            self.projector.on_message(&event).await.unwrap();
        }
    }

    #[tokio::test]
    async fn a_request_files_under_the_recipients_requests_and_the_requesters_inbox() {
        let w = world();
        let (a, b) = (pid(), pid());
        let id = w.direct(a, b, true).await;
        assert!(w.inbox.entry(&b, &id).is_none(), "nothing before a message");
        w.sent(id, a, 1_000, false).await;

        let mine = w.inbox.entry(&a, &id).unwrap();
        assert_eq!((mine.folder, mine.peer), (Folder::Inbox, Some(b)));
        let theirs = w.inbox.entry(&b, &id).unwrap();
        assert_eq!((theirs.folder, theirs.peer), (Folder::Requests, Some(a)));
        assert_eq!(theirs.last.unwrap().preview.chars().count(), PREVIEW_CHARS);

        file_answer(&*w.inbox, &b, &id, true).await.unwrap();
        assert_eq!(w.inbox.entry(&b, &id).unwrap().folder, Folder::Inbox, "accepted: moved");
        let id2 = w.direct(pid(), b, true).await;
        let requester = w.conversations.find(&id2).await.unwrap().unwrap().owner_id();
        w.sent(id2, requester, 2_000, false).await;
        file_answer(&*w.inbox, &b, &id2, false).await.unwrap();
        assert!(w.inbox.entry(&b, &id2).is_none(), "declined: gone for the decliner");
        assert!(w.inbox.entry(&requester, &id2).is_some(), "still there for its requester");
    }

    /// The must-have: a blocked opener never reaches the blocker's inbox.
    #[tokio::test]
    async fn a_withheld_message_moves_only_its_senders_entry() {
        let w = world();
        let (blocked, blocker) = (pid(), pid());
        let id = w.direct(blocked, blocker, true).await;
        w.sent(id, blocked, 1_000, true).await;
        assert!(w.inbox.entry(&blocker, &id).is_none(), "the blocker's requests stay empty");
        assert_eq!(w.inbox.entry(&blocked, &id).unwrap().folder, Folder::Inbox, "the sender sees it sent");
        assert!(w.inbox.folder(&blocker, Folder::Requests).is_empty());
    }

    #[tokio::test]
    async fn groups_enter_on_join_leave_on_leaving_and_replays_never_move_back() {
        let w = world();
        let owner = pid();
        let group = Conversation::create(ConversationId::new(), ConversationKind::Group, owner);
        w.conversations.insert(&group).await.unwrap();
        let id = group.id();
        w.projector
            .on_conversation(&DomainEvent::ConversationCreated(ConversationCreatedEvent {
                conversation_id: id.as_str(),
                kind:            "group".into(),
                visibility:      "private".into(),
                owner_id:        owner.as_str(),
                created_at_ms:   500,
            }))
            .await
            .unwrap();
        let entry = w.inbox.entry(&owner, &id).unwrap();
        assert_eq!((entry.folder, entry.activity.timestamp_millis(), entry.last), (Folder::Inbox, 500, None));

        let joiner = pid();
        w.members.insert(&id, &Participant::new(owner, Role::Owner).unwrap()).await.unwrap();
        w.members.insert(&id, &Participant::new(joiner, Role::Member).unwrap()).await.unwrap();
        w.projector
            .on_conversation(&DomainEvent::MemberJoined(MemberJoinedEvent {
                conversation_id: id.as_str(),
                profile_id:      joiner.as_str(),
                role:            "member".into(),
                joined_at_ms:    600,
            }))
            .await
            .unwrap();
        w.sent(id, owner, 2_000, false).await;
        w.sent(id, owner, 1_000, false).await; // an older event, replayed late
        assert_eq!(w.inbox.entry(&joiner, &id).unwrap().activity.timestamp_millis(), 2_000);

        w.projector
            .on_conversation(&DomainEvent::MemberLeft(MemberLeftEvent {
                conversation_id: id.as_str(),
                profile_id:      joiner.as_str(),
                left_at_ms:      3_000,
            }))
            .await
            .unwrap();
        assert!(w.inbox.entry(&joiner, &id).is_none());
    }

    /// #654: a message is pushed to the members it reaches in their inbox,
    /// except its sender and those who muted the conversation; a request, a
    /// withheld message and a system message are not pushed.
    #[tokio::test]
    async fn a_message_is_pushed_to_whom_it_reaches_unless_muted() {
        let w = world();
        let (a, b) = (pid(), pid());
        let open = w.direct(a, b, false).await;
        w.sent(open, a, 1_000, false).await;
        let asked = w.pushes.asked();
        assert_eq!(asked.len(), 1);
        assert_eq!(asked[0].recipients, vec![b.as_str()]);
        assert_eq!((asked[0].conversation_kind.as_str(), asked[0].preview.chars().count()), ("direct", PREVIEW_CHARS));

        // A request waits unpushed; so does a message from someone blocked.
        let request = w.direct(pid(), b, true).await;
        let requester = w.conversations.find(&request).await.unwrap().unwrap().owner_id();
        w.sent(request, requester, 2_000, false).await;
        w.sent(open, a, 3_000, true).await;
        // A media message shows no text; a system message is never pushed.
        w.sent_as(open, a, 4_000, false, "media").await;
        w.sent_as(open, a, 5_000, false, "system").await;
        let asked = w.pushes.asked();
        assert_eq!(asked.len(), 2, "{asked:?}");
        assert_eq!((asked[1].content_type.as_str(), asked[1].preview.as_str()), ("media", ""));

        // b mutes the conversation: a's next message is pushed to no one.
        let until = Utc::now() + chrono::Duration::hours(1);
        assert!(w.members.set_muted_until(&open, &b, Some(until)).await.unwrap());
        w.sent(open, a, 6_000, false).await;
        assert_eq!(w.pushes.asked().len(), 2, "muted");
        assert!(w.inbox.entry(&b, &open).unwrap().activity.timestamp_millis() == 6_000, "still in the inbox");
    }
}
