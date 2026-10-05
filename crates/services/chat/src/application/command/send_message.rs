use std::sync::Arc;

use async_trait::async_trait;
use cqrs::{Command, CommandHandler, Envelope};
use validate_core::{FieldViolation, Validate};

use crate::application::command::direct::{admit, Admission};
use crate::application::port::{
    ConversationRepository, EventPublisher, InteractionGate, MemberRepository, MessageRepository,
};
use crate::domain::aggregate::Message;
use crate::domain::event::{MessageEvent, MessageSentEvent};
use crate::domain::value_object::{
    ContentType, ConversationId, MessageContent, MessageId, ProfileId,
};
use crate::error::ChatError;

/// Posts a message to a conversation.
///
/// The member write path is intentionally lean: a single authorization read
/// (roster membership) followed by the durable write and the fan-out event. The
/// conversation aggregate is not loaded here — membership implies existence — to
/// keep the hot write path to one round-trip + one write: the roster read and
/// the conversation read run side by side. A non-member is answered like a
/// missing conversation when it is private (see
/// [`deny_outsider`](crate::domain::aggregate::Conversation::deny_outsider)).
///
/// A direct conversation (#656) also asks who may message whom
/// ([`admit`]): a request holds one message, a block withholds it — sent, as
/// far as its sender can tell, but shown to nobody else.
pub struct SendMessageCommand {
    pub message_id:      String,
    pub conversation_id: String,
    pub sender_id:       String,
    pub content_type:    i32,
    pub body:            String,
    pub media_ref:       Option<String>,
    pub reply_to:        Option<String>,
}

impl Command for SendMessageCommand {}

impl Validate for SendMessageCommand {
    fn validate(&self) -> Result<(), Vec<FieldViolation>> {
        let mut v = Vec::new();
        if self.message_id.trim().is_empty() {
            v.push(FieldViolation::new("message_id", "CHT-VAL-010", "message_id must not be empty"));
        }
        if self.conversation_id.trim().is_empty() {
            v.push(FieldViolation::new(
                "conversation_id",
                "CHT-VAL-011",
                "conversation_id must not be empty",
            ));
        }
        if self.sender_id.trim().is_empty() {
            v.push(FieldViolation::new("sender_id", "CHT-VAL-012", "sender_id must not be empty"));
        }
        if v.is_empty() { Ok(()) } else { Err(v) }
    }
}

/// How a sent message goes out.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct SentMessage {
    /// Shown to its sender only: deliver it live to nobody else.
    pub withheld: bool,
    /// A message request's one message.
    pub request:  bool,
}

/// Sends a message and says how it goes out (the gRPC layer's live fan-out
/// needs to know).
#[async_trait]
pub trait SendMessages: Send + Sync + 'static {
    async fn send(&self, cmd: &SendMessageCommand) -> Result<SentMessage, ChatError>;
}

pub struct SendMessageHandler<CR, MR, MSG, EP> {
    pub conversation_repo: Arc<CR>,
    pub member_repo:       Arc<MR>,
    pub message_repo:      Arc<MSG>,
    pub publisher:         Arc<EP>,
    /// Who may message whom in a direct conversation (#656).
    pub gate:              Option<Arc<dyn InteractionGate>>,
}

impl<CR, MR, MSG, EP> CommandHandler<SendMessageCommand> for SendMessageHandler<CR, MR, MSG, EP>
where
    CR:  ConversationRepository,
    MR:  MemberRepository,
    MSG: MessageRepository,
    EP:  EventPublisher,
{
    type Error = ChatError;

    async fn handle(&self, envelope: Envelope<SendMessageCommand>) -> Result<(), ChatError> {
        self.send(&envelope.payload).await.map(|_| ())
    }
}

#[async_trait]
impl<CR, MR, MSG, EP> SendMessages for SendMessageHandler<CR, MR, MSG, EP>
where
    CR:  ConversationRepository,
    MR:  MemberRepository,
    MSG: MessageRepository,
    EP:  EventPublisher,
{
    async fn send(&self, cmd: &SendMessageCommand) -> Result<SentMessage, ChatError> {

        let conversation_id = ConversationId::try_from(cmd.conversation_id.as_str())?;
        let sender_id       = ProfileId::try_from(cmd.sender_id.as_str())?;
        let message_id      = MessageId::try_from(cmd.message_id.as_str())?;
        let content_type    = ContentType::try_from(cmd.content_type as i8)?;

        // Authorization: only roster members may write. Audience roles are never
        // in the roster, so this read enforces read-only for guests.
        let (member, conversation) = tokio::join!(
            self.member_repo.find(&conversation_id, &sender_id),
            self.conversation_repo.find(&conversation_id),
        );
        let (member, conversation) = (member?, conversation?);
        let Some(member) = member else {
            return Err(match conversation {
                Some(conversation) => conversation.deny_outsider(sender_id),
                None => ChatError::ConversationNotFound { conversation_id: conversation_id.as_str() },
            });
        };

        if !member.can_write() {
            return Err(ChatError::NotAuthorized {
                profile_id:      sender_id.as_str(),
                conversation_id: conversation_id.as_str(),
            });
        }

        let reply_to = cmd
            .reply_to
            .as_deref()
            .filter(|s| !s.is_empty())
            .map(MessageId::try_from)
            .transpose()?;

        let admission = match &conversation {
            Some(conversation) if conversation.is_direct() => {
                admit(&*self.conversation_repo, self.gate.as_ref(), conversation, sender_id).await?
            }
            _ => Admission::default(),
        };

        let mut message = Message::create(
            message_id,
            conversation_id,
            sender_id,
            content_type,
            MessageContent::new(cmd.body.clone())?,
            cmd.media_ref.clone().filter(|s| !s.is_empty()),
            reply_to,
        )?;
        if admission.withheld {
            message.withhold();
        }

        // Durable write first; the event is the seam the routing layer forks into
        // the Member-Plane broadcast and the Audience-Plane shadow.
        self.message_repo.insert(&message).await?;

        let event = MessageEvent::Sent(MessageSentEvent {
            conversation_id: conversation_id.as_str(),
            message_id:      message.id().as_str(),
            sender_id:       sender_id.as_str(),
            content_type:    content_type.as_str().to_owned(),
            body:            message.content().as_str().to_owned(),
            media_ref:       message.media_ref().map(str::to_owned),
            reply_to:        message.reply_to().map(|m| m.as_str()),
            created_at_ms:   message.created_at().timestamp_millis(),
            withheld:        admission.withheld,
            request:         admission.request,
        });
        self.publisher.publish_message(&event).await?;

        Ok(SentMessage { withheld: admission.withheld, request: admission.request })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::application::command::fakes::{
        FakeConversations, FakeMembers, FakeMessages, FakePublisher, Fixture,
    };

    async fn send(
        f: &Fixture,
        conversation_id: ConversationId,
        sender: ProfileId,
        messages: &Arc<FakeMessages>,
    ) -> Result<(), ChatError> {
        let handler: SendMessageHandler<FakeConversations, FakeMembers, FakeMessages, FakePublisher> =
            SendMessageHandler {
                conversation_repo: Arc::clone(&f.conversations),
                member_repo:       Arc::clone(&f.members),
                message_repo:      Arc::clone(messages),
                publisher:         Arc::clone(&f.publisher),
                gate:              None,
            };
        handler
            .handle(Envelope::new(uuid::Uuid::now_v7(), SendMessageCommand {
                message_id:      MessageId::new().as_str(),
                conversation_id: conversation_id.as_str(),
                sender_id:       sender.as_str(),
                content_type:    ContentType::Text.as_tinyint() as i32,
                body:            "hello".to_owned(),
                media_ref:       None,
                reply_to:        None,
            }))
            .await
    }

    #[tokio::test]
    async fn outsider_of_a_private_conversation_answers_like_a_missing_one() {
        let f = Fixture::private_group();
        let messages = Arc::default();
        let outsider = Fixture::profile();

        let err = send(&f, f.conversation_id, outsider, &messages).await.unwrap_err();
        assert!(matches!(err, ChatError::ConversationConcealed { .. }), "{err:?}");

        let err = send(&f, ConversationId::new(), outsider, &messages).await.unwrap_err();
        assert!(matches!(err, ChatError::ConversationNotFound { .. }), "{err:?}");
        assert_eq!(messages.inserts(), 0);
    }

    #[tokio::test]
    async fn outsider_of_a_public_conversation_is_not_a_member() {
        let f = Fixture::public_group();
        let messages = Arc::default();
        let err = send(&f, f.conversation_id, Fixture::profile(), &messages).await.unwrap_err();
        assert!(matches!(err, ChatError::NotAMember { .. }), "{err:?}");
        assert_eq!(messages.inserts(), 0);
    }

    /// A direct conversation's request and a block (#656), end to end through
    /// the handler: the blocked sender's message is stored withheld.
    #[tokio::test]
    async fn a_direct_message_is_admitted_by_the_recipients_settings() {
        use crate::application::command::direct::DirectConversations;
        use crate::application::command::fakes::ScriptedGate;
        use crate::application::port::{InteractionGate, MessageVerdict};

        let f = Fixture::private_group();
        let gate = Arc::new(ScriptedGate::default());
        let gate_dyn = Arc::clone(&gate) as Arc<dyn InteractionGate>;
        let direct = DirectConversations {
            conversation_repo: Arc::clone(&f.conversations),
            member_repo:       Arc::clone(&f.members),
            publisher:         Arc::clone(&f.publisher),
            gate:              Some(Arc::clone(&gate_dyn)),
            inbox:             Arc::new(crate::application::command::fakes::FakeInbox::default()),
        };
        let messages = Arc::<FakeMessages>::default();
        let handler = SendMessageHandler {
            conversation_repo: Arc::clone(&f.conversations),
            member_repo:       Arc::clone(&f.members),
            message_repo:      Arc::clone(&messages),
            publisher:         Arc::clone(&f.publisher),
            gate:              Some(gate_dyn),
        };
        let cmd = |conversation_id: ConversationId, sender: ProfileId| SendMessageCommand {
            message_id:      MessageId::new().as_str(),
            conversation_id: conversation_id.as_str(),
            sender_id:       sender.as_str(),
            content_type:    ContentType::Text.as_tinyint() as i32,
            body:            "hi".to_owned(),
            media_ref:       None,
            reply_to:        None,
        };

        let (stranger, recipient, blocked) = (Fixture::profile(), Fixture::profile(), Fixture::profile());
        gate.set(stranger, recipient, MessageVerdict::Request);
        gate.set(blocked, recipient, MessageVerdict::Silenced);
        let request = direct.open(stranger, recipient).await.unwrap().conversation_id;
        let silenced = direct.open(blocked, recipient).await.unwrap().conversation_id;

        let sent = handler.send(&cmd(request, stranger)).await.unwrap();
        assert_eq!(sent, SentMessage { withheld: false, request: true });
        let sent = handler.send(&cmd(silenced, blocked)).await.unwrap();
        assert_eq!(sent, SentMessage { withheld: true, request: true });
        assert_eq!(messages.withheld(), vec![false, true]);
        assert!(handler.send(&cmd(request, stranger)).await.is_err(), "one message per request");
        assert!(handler.send(&cmd(request, Fixture::profile())).await.is_err(), "outsiders never write");

        // Groups are untouched by the gate.
        assert_eq!(handler.send(&cmd(f.conversation_id, f.owner)).await.unwrap(), SentMessage::default());
    }

    #[tokio::test]
    async fn member_sends() {
        let f = Fixture::private_group();
        let messages = Arc::default();
        send(&f, f.conversation_id, f.owner, &messages).await.unwrap();
        assert_eq!(messages.inserts(), 1);
    }
}
