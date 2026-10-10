use std::sync::Arc;

use async_trait::async_trait;
use cqrs::{Command, CommandHandler, Envelope};
use validate_core::{FieldViolation, Validate};

use crate::application::command::direct::{admit, Admission};
use crate::application::port::{
    ConversationRepository, EventPublisher, InteractionGate, MemberRepository, MessageRepository,
    SendClaim, SendKeys,
};
use crate::domain::aggregate::{Conversation, Message};
use crate::domain::event::{MessageEvent, MessageSentEvent};
use crate::domain::value_object::{
    ContentType, ConversationId, IdempotencyKey, MessageContent, MessageId, ProfileId,
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
///
/// An idempotency key (#875) makes a retried send store nothing: the key is
/// claimed alongside the membership read and before the request's
/// one-message rule, so a stored send answers its retry even after the sender
/// has left, and the retry of a request's message answers it rather than
/// `CHT-1010`.
pub struct SendMessageCommand {
    pub message_id:      String,
    pub conversation_id: String,
    pub sender_id:       String,
    pub content_type:    i32,
    pub body:            String,
    pub media_ref:       Option<String>,
    pub reply_to:        Option<String>,
    /// The client's key for this message, reused on its retries (#875).
    pub idempotency_key: Option<String>,
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
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SentMessage {
    /// The stored message: the command's own id, or on a replay the first send's.
    pub message_id: MessageId,
    /// Shown to its sender only: deliver it live to nobody else.
    pub withheld:   bool,
    /// A message request's one message.
    pub request:    bool,
    /// The idempotency key was already sent (#875): nothing was written or
    /// published, so nothing goes out live either.
    pub replayed:   bool,
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
    /// Idempotency keys (#875); `None` sends every message, key or not.
    pub send_keys:         Option<Arc<dyn SendKeys>>,
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
        let key = cmd
            .idempotency_key
            .as_deref()
            .filter(|k| !k.is_empty())
            .map(IdempotencyKey::try_from)
            .transpose()?;

        // Authorization: only roster members may write. Audience roles are never
        // in the roster, so this read enforces read-only for guests. The key is
        // claimed alongside: a send already stored answers its message even if
        // the sender has left since.
        let (member, conversation, claim) = tokio::join!(
            self.member_repo.find(&conversation_id, &sender_id),
            self.conversation_repo.find(&conversation_id),
            self.claim(&conversation_id, &sender_id, key.as_ref(), message_id),
        );
        let claimed = match claim {
            Some(SendClaim::Sent(first)) => {
                return Ok(SentMessage { message_id: first, withheld: false, request: false, replayed: true });
            }
            Some(SendClaim::InFlight) => {
                return Err(ChatError::SendInFlight { conversation_id: conversation_id.as_str() });
            }
            Some(SendClaim::Fresh) => key.as_ref(),
            None => None,
        };

        let sent = async {
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
            self.write(cmd, conversation_id, sender_id, message_id, conversation.as_ref(), claimed).await
        }
        .await;

        // Frees the key for the retry unless the message was stored (then the
        // key is already sent and this is a no-op).
        if let (Err(_), Some(key), Some(keys)) = (&sent, claimed, &self.send_keys)
            && let Err(e) = keys.release(&conversation_id, &sender_id, key, message_id).await
        {
            tracing::warn!(error = %e, "send key release failed; it expires on its own");
        }
        sent
    }
}

impl<CR, MR, MSG, EP> SendMessageHandler<CR, MR, MSG, EP>
where
    CR:  ConversationRepository,
    MR:  MemberRepository,
    MSG: MessageRepository,
    EP:  EventPublisher,
{
    /// Claims the send's key, if it has one and the store answers: a store that
    /// does not answer sends the message unkeyed (a lost dedupe beats a lost
    /// message).
    async fn claim(
        &self,
        conversation_id: &ConversationId,
        sender_id:       &ProfileId,
        key:             Option<&IdempotencyKey>,
        message_id:      MessageId,
    ) -> Option<SendClaim> {
        let (keys, key) = (self.send_keys.as_ref()?, key?);
        keys.claim(conversation_id, sender_id, key, message_id)
            .await
            .inspect_err(|e| tracing::warn!(error = %e, "send key claim failed; sending without deduplication"))
            .ok()
    }

    /// Admits, stores and announces a member's message.
    async fn write(
        &self,
        cmd:             &SendMessageCommand,
        conversation_id: ConversationId,
        sender_id:       ProfileId,
        message_id:      MessageId,
        conversation:    Option<&Conversation>,
        claimed:         Option<&IdempotencyKey>,
    ) -> Result<SentMessage, ChatError> {
        let content_type = ContentType::try_from(cmd.content_type as i8)?;
        let reply_to = cmd
            .reply_to
            .as_deref()
            .filter(|s| !s.is_empty())
            .map(MessageId::try_from)
            .transpose()?;

        let admission = match conversation {
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

        // Stored: from here a retry answers this message, even if announcing it
        // fails below — a missed announcement beats a duplicate in the log.
        if let (Some(key), Some(keys)) = (claimed, &self.send_keys)
            && let Err(e) = keys.complete(&conversation_id, &sender_id, key, message_id).await
        {
            tracing::warn!(error = %e, "send key completion failed; retries are refused until it expires");
        }

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

        Ok(SentMessage {
            message_id,
            withheld: admission.withheld,
            request:  admission.request,
            replayed: false,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashSet;

    use crate::application::command::fakes::{
        FakeConversations, FakeMembers, FakeMessages, FakePublisher, FakeSendKeys, Fixture,
    };
    use crate::domain::value_object::Role;

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
                send_keys:         None,
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
                idempotency_key: None,
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
            send_keys:         None,
        };
        let cmd = |conversation_id: ConversationId, sender: ProfileId| SendMessageCommand {
            message_id:      MessageId::new().as_str(),
            conversation_id: conversation_id.as_str(),
            sender_id:       sender.as_str(),
            content_type:    ContentType::Text.as_tinyint() as i32,
            body:            "hi".to_owned(),
            media_ref:       None,
            reply_to:        None,
            idempotency_key: None,
        };

        let (stranger, recipient, blocked) = (Fixture::profile(), Fixture::profile(), Fixture::profile());
        gate.set(stranger, recipient, MessageVerdict::Request);
        gate.set(blocked, recipient, MessageVerdict::Silenced);
        let request = direct.open(stranger, recipient).await.unwrap().conversation_id;
        let silenced = direct.open(blocked, recipient).await.unwrap().conversation_id;

        let sent = handler.send(&cmd(request, stranger)).await.unwrap();
        assert_eq!((sent.withheld, sent.request), (false, true));
        let sent = handler.send(&cmd(silenced, blocked)).await.unwrap();
        assert_eq!((sent.withheld, sent.request), (true, true));
        assert_eq!(messages.withheld(), vec![false, true]);
        assert!(handler.send(&cmd(request, stranger)).await.is_err(), "one message per request");
        assert!(handler.send(&cmd(request, Fixture::profile())).await.is_err(), "outsiders never write");

        // Groups are untouched by the gate.
        let sent = handler.send(&cmd(f.conversation_id, f.owner)).await.unwrap();
        assert_eq!((sent.withheld, sent.request), (false, false));
    }

    #[tokio::test]
    async fn member_sends() {
        let f = Fixture::private_group();
        let messages = Arc::default();
        send(&f, f.conversation_id, f.owner, &messages).await.unwrap();
        assert_eq!(messages.inserts(), 1);
    }

    // ── Idempotency keys (#875) ─────────────────────────────────────────────

    fn keyed_handler(
        f: &Fixture,
        messages: &Arc<FakeMessages>,
        keys: &Arc<FakeSendKeys>,
    ) -> SendMessageHandler<FakeConversations, FakeMembers, FakeMessages, FakePublisher> {
        SendMessageHandler {
            conversation_repo: Arc::clone(&f.conversations),
            member_repo:       Arc::clone(&f.members),
            message_repo:      Arc::clone(messages),
            publisher:         Arc::clone(&f.publisher),
            gate:              None,
            send_keys:         Some(Arc::clone(keys) as Arc<dyn SendKeys>),
        }
    }

    /// Each attempt mints its own message id, as the gRPC layer does.
    fn keyed(conversation_id: ConversationId, sender: ProfileId, key: Option<&str>) -> SendMessageCommand {
        SendMessageCommand {
            message_id:      MessageId::new().as_str(),
            conversation_id: conversation_id.as_str(),
            sender_id:       sender.as_str(),
            content_type:    ContentType::Text.as_tinyint() as i32,
            body:            "hello".to_owned(),
            media_ref:       None,
            reply_to:        None,
            idempotency_key: key.map(str::to_owned),
        }
    }

    #[tokio::test]
    async fn a_repeated_key_stores_one_message_and_answers_its_id() {
        let f = Fixture::private_group();
        let (messages, keys) = (Arc::default(), Arc::<FakeSendKeys>::default());
        let handler = keyed_handler(&f, &messages, &keys);
        let key = Some("bubble-0001");

        let first = handler.send(&keyed(f.conversation_id, f.owner, key)).await.unwrap();
        let retry = handler.send(&keyed(f.conversation_id, f.owner, key)).await.unwrap();

        assert_eq!(retry.message_id, first.message_id, "the retry answers the first message");
        assert!(!first.replayed);
        assert!(retry.replayed, "nothing goes out live for the retry");
        assert_eq!(messages.inserts(), 1);
    }

    #[tokio::test]
    async fn another_key_or_no_key_sends_as_before() {
        let f = Fixture::private_group();
        let (messages, keys) = (Arc::default(), Arc::<FakeSendKeys>::default());
        let handler = keyed_handler(&f, &messages, &keys);
        let member = f.add_member(Role::Member);

        let a = handler.send(&keyed(f.conversation_id, f.owner, Some("bubble-0001"))).await.unwrap();
        let b = handler.send(&keyed(f.conversation_id, f.owner, Some("bubble-0002"))).await.unwrap();
        // The same key from another sender is that sender's own.
        let c = handler.send(&keyed(f.conversation_id, member, Some("bubble-0001"))).await.unwrap();
        let d = handler.send(&keyed(f.conversation_id, f.owner, None)).await.unwrap();
        let e = handler.send(&keyed(f.conversation_id, f.owner, Some(""))).await.unwrap();

        let ids: HashSet<_> = [a, b, c, d, e].iter().map(|s| s.message_id).collect();
        assert_eq!(ids.len(), 5);
        assert_eq!(messages.inserts(), 5);
        assert_eq!(keys.held(), 3, "no key, no claim");
    }

    #[tokio::test]
    async fn a_key_still_in_flight_is_refused_retryably() {
        use error::AppError as _;
        let f = Fixture::private_group();
        let (messages, keys) = (Arc::default(), Arc::<FakeSendKeys>::default());
        let handler = keyed_handler(&f, &messages, &keys);
        keys.hold(&f.conversation_id, &f.owner, &IdempotencyKey::try_from("bubble-0001").unwrap());

        let err = handler.send(&keyed(f.conversation_id, f.owner, Some("bubble-0001"))).await.unwrap_err();
        assert!(matches!(err, ChatError::SendInFlight { .. }), "{err:?}");
        assert!(err.is_retryable());
        assert_eq!(messages.inserts(), 0);
    }

    #[tokio::test]
    async fn a_failed_send_frees_its_key_for_the_retry() {
        let f = Fixture::private_group();
        let (messages, keys) = (Arc::default(), Arc::<FakeSendKeys>::default());
        let handler = keyed_handler(&f, &messages, &keys);
        let mut empty = keyed(f.conversation_id, f.owner, Some("bubble-0001"));
        empty.body = String::new();

        assert!(matches!(handler.send(&empty).await, Err(ChatError::EmptyMessage)));
        assert_eq!(keys.held(), 0, "released");
        handler.send(&keyed(f.conversation_id, f.owner, Some("bubble-0001"))).await.unwrap();
        assert_eq!(messages.inserts(), 1);
    }

    #[tokio::test]
    async fn a_malformed_key_is_refused_and_nothing_is_sent() {
        let f = Fixture::private_group();
        let (messages, keys) = (Arc::default(), Arc::<FakeSendKeys>::default());
        let handler = keyed_handler(&f, &messages, &keys);

        let err = handler.send(&keyed(f.conversation_id, f.owner, Some("short"))).await.unwrap_err();
        assert!(matches!(err, ChatError::InvalidIdempotencyKey), "{err:?}");
        assert_eq!(messages.inserts(), 0);
    }

    /// The keys are best-effort: without them the message still goes out.
    #[tokio::test]
    async fn an_unreachable_key_store_sends_unkeyed() {
        let f = Fixture::private_group();
        let (messages, keys) = (Arc::default(), Arc::<FakeSendKeys>::default());
        let handler = keyed_handler(&f, &messages, &keys);
        keys.down();

        handler.send(&keyed(f.conversation_id, f.owner, Some("bubble-0001"))).await.unwrap();
        assert_eq!(messages.inserts(), 1);
    }

    /// A request's one message (#656), retried with its key, answers the first
    /// send instead of `CHT-1010`.
    #[tokio::test]
    async fn the_retry_of_a_requests_message_is_not_a_second_message() {
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
        let (messages, keys) = (Arc::default(), Arc::<FakeSendKeys>::default());
        let mut handler = keyed_handler(&f, &messages, &keys);
        handler.gate = Some(gate_dyn);

        let (stranger, recipient) = (Fixture::profile(), Fixture::profile());
        gate.set(stranger, recipient, MessageVerdict::Request);
        let request = direct.open(stranger, recipient).await.unwrap().conversation_id;

        let first = handler.send(&keyed(request, stranger, Some("bubble-0001"))).await.unwrap();
        let retry = handler.send(&keyed(request, stranger, Some("bubble-0001"))).await.unwrap();
        assert_eq!(retry.message_id, first.message_id);
        assert!(matches!(
            handler.send(&keyed(request, stranger, Some("bubble-0002"))).await,
            Err(ChatError::MessageRequestPending { .. }),
        ), "a new message is still a second one");
        assert_eq!(messages.inserts(), 1);
    }

    /// Announces nothing: Kafka is down.
    struct DownPublisher;

    #[async_trait]
    impl EventPublisher for DownPublisher {
        async fn publish_conversation(&self, _: &crate::domain::event::DomainEvent) -> Result<(), ChatError> {
            Ok(())
        }

        async fn publish_message(&self, _: &MessageEvent) -> Result<(), ChatError> {
            Err(ChatError::EventPublishFailed { message: "down".to_owned() })
        }
    }

    /// Stored but not announced: the retry answers the stored message rather
    /// than storing a second one.
    #[tokio::test]
    async fn a_send_stored_but_not_announced_is_not_stored_again() {
        let f = Fixture::private_group();
        let (messages, keys) = (Arc::<FakeMessages>::default(), Arc::<FakeSendKeys>::default());
        let handler = SendMessageHandler {
            conversation_repo: Arc::clone(&f.conversations),
            member_repo:       Arc::clone(&f.members),
            message_repo:      Arc::clone(&messages),
            publisher:         Arc::new(DownPublisher),
            gate:              None,
            send_keys:         Some(Arc::clone(&keys) as Arc<dyn SendKeys>),
        };
        let first = keyed(f.conversation_id, f.owner, Some("bubble-0001"));

        assert!(matches!(handler.send(&first).await, Err(ChatError::EventPublishFailed { .. })));
        let retry = handler.send(&keyed(f.conversation_id, f.owner, Some("bubble-0001"))).await.unwrap();
        assert!(retry.replayed);
        assert_eq!(retry.message_id.as_str(), first.message_id);
        assert_eq!(messages.inserts(), 1);
    }

    #[tokio::test]
    async fn a_retry_after_leaving_still_answers_the_stored_message() {
        let f = Fixture::private_group();
        let (messages, keys) = (Arc::default(), Arc::<FakeSendKeys>::default());
        let handler = keyed_handler(&f, &messages, &keys);
        let member = f.add_member(Role::Member);

        let first = handler.send(&keyed(f.conversation_id, member, Some("bubble-0001"))).await.unwrap();
        f.members.remove(&f.conversation_id, &member);
        let retry = handler.send(&keyed(f.conversation_id, member, Some("bubble-0001"))).await.unwrap();
        assert_eq!(retry.message_id, first.message_id);

        // A new message is refused, and its claim freed.
        assert!(handler.send(&keyed(f.conversation_id, member, Some("bubble-0002"))).await.is_err());
        assert_eq!(keys.held(), 1);
    }
}
