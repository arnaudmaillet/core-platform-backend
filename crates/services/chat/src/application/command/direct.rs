//! Direct conversations and message requests (#656).
//!
//! A direct conversation is between two profiles, unique per pair. Whether
//! the opener may write freely is the recipient's call (social-graph's
//! `CheckInteraction(MESSAGE)`, via [`InteractionGate`]):
//!
//! | verdict | opening | the opener's messages |
//! |---|---|---|
//! | allowed | open | delivered |
//! | audience excludes them / a limit holds them | a request | one, waiting for an answer |
//! | no one | refused (`CHT-1011`) | refused |
//! | a block, either way | a request, never answered | withheld — shown to the sender only |
//!
//! The blocked sender's experience is **identical** to that of a sender whose
//! request is pending or declined: same answers, same calls, nothing delivered.
//! The recipient accepts by replying, by opening the conversation themselves,
//! or explicitly; a decline is silent and keeps the requester from asking
//! again for 30 days. Once open, a conversation stays open whatever the
//! recipient's settings become — except a block, which withholds again.
//!
//! The gate fails closed: unreachable, nothing is opened or sent
//! (`CHT-5001`, retryable).

use std::sync::Arc;

use async_trait::async_trait;
use chrono::Utc;

use crate::application::command::inbox::file_answer;
use crate::application::port::{
    ConversationRepository, EventPublisher, InboxStore, InteractionGate, MemberRepository, MessageVerdict,
};
use crate::domain::aggregate::{Conversation, Participant};
use crate::domain::value_object::{ConversationId, MessageRequest, ProfileId, Role};
use crate::error::ChatError;

/// The gate's verdict; with no gate configured, direct messages are off.
pub(crate) async fn verdict(
    gate:      Option<&Arc<dyn InteractionGate>>,
    actor:     &ProfileId,
    recipient: &ProfileId,
) -> Result<MessageVerdict, ChatError> {
    match gate {
        Some(gate) => gate.may_message(actor, recipient).await,
        None => Err(ChatError::InteractionCheckUnavailable { reason: "no interaction gate configured".to_owned() }),
    }
}

/// The direct conversation an opener gets.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct OpenedDirect {
    pub conversation_id: ConversationId,
    /// The opener's messages wait for the peer's answer (one until then).
    pub request:         bool,
}

/// How a message to a direct conversation goes out.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct Admission {
    /// Shown to its sender only.
    pub withheld: bool,
    /// The request's one message (no push until accepted).
    pub request:  bool,
}

/// Opens direct conversations and answers their requests.
pub struct DirectConversations<CR, MR, EP> {
    pub conversation_repo: Arc<CR>,
    pub member_repo:       Arc<MR>,
    pub publisher:         Arc<EP>,
    pub gate:              Option<Arc<dyn InteractionGate>>,
    /// Files an answered request in the recipient's inbox.
    pub inbox:             Arc<dyn InboxStore>,
}

impl<CR, MR, EP> DirectConversations<CR, MR, EP>
where
    CR: ConversationRepository,
    MR: MemberRepository,
    EP: EventPublisher,
{
    /// The direct conversation between `opener` and `peer`, opened if need be.
    pub async fn open(&self, opener: ProfileId, peer: ProfileId) -> Result<OpenedDirect, ChatError> {
        if opener == peer {
            return Err(ChatError::DomainViolation {
                field:   "peer_id".to_owned(),
                message: "a direct conversation is with someone else".to_owned(),
            });
        }
        let verdict = verdict(self.gate.as_ref(), &opener, &peer).await?;
        let id = self.conversation_repo.claim_direct(&opener, &peer, ConversationId::new()).await?;
        // Twice at most: a lost insert race re-reads the winner's row.
        for _ in 0..2 {
            if let Some(existing) = self.conversation_repo.find(&id).await? {
                return self.reopen(existing, opener, verdict).await;
            }
            if verdict == MessageVerdict::Refused {
                return Err(ChatError::MessagingNotAllowed { profile_id: peer.as_str() });
            }
            let as_request = verdict != MessageVerdict::Allowed;
            let mut conversation = Conversation::open_direct(id, opener, peer, as_request);
            if !self.conversation_repo.insert_direct(&conversation).await? {
                continue;
            }
            // The roster rows are idempotent upserts: a retry after a crash
            // between them writes the same rows.
            self.member_repo.insert(&id, &Participant::new(opener, Role::Member)?).await?;
            self.member_repo.insert(&id, &Participant::new(peer, Role::Member)?).await?;
            for event in conversation.take_events() {
                self.publisher.publish_conversation(&event).await?;
            }
            return Ok(OpenedDirect { conversation_id: id, request: as_request });
        }
        Err(ChatError::ConversationNotFound { conversation_id: id.as_str() })
    }

    /// `opener` opens a conversation that exists. The recipient of a request
    /// opening it accepts it; its requester gets it back as it stands.
    async fn reopen(
        &self,
        mut conversation: Conversation,
        opener:           ProfileId,
        verdict:          MessageVerdict,
    ) -> Result<OpenedDirect, ChatError> {
        let id = conversation.id();
        // Restore the roster should a crash have cut the first open short
        // (the rows are upserts; an open conversation is the common case).
        if self.member_repo.find(&id, &opener).await?.is_none() {
            self.member_repo.insert(&id, &Participant::new(opener, Role::Member)?).await?;
        }
        let request = conversation.request();
        match request.requester() {
            None => Ok(OpenedDirect { conversation_id: id, request: false }),
            Some(requester) if requester != opener => {
                self.conversation_repo.transition_request(&id, request, MessageRequest::Open).await?;
                conversation.set_request(MessageRequest::Open);
                file_answer(&*self.inbox, &opener, &id, true).await?;
                Ok(OpenedDirect { conversation_id: id, request: false })
            }
            Some(_) => {
                if verdict == MessageVerdict::Refused {
                    let peer = conversation.peer_of(opener).unwrap_or(opener);
                    return Err(ChatError::MessagingNotAllowed { profile_id: peer.as_str() });
                }
                if request.cooled_down(Utc::now()) {
                    self.conversation_repo
                        .transition_request(&id, request, MessageRequest::Pending { requester: opener })
                        .await?;
                }
                Ok(OpenedDirect { conversation_id: id, request: true })
            }
        }
    }

    /// The recipient of a request accepts or declines it. A decline is
    /// silent: its requester keeps seeing it pending.
    pub async fn respond(&self, conversation_id: ConversationId, profile: ProfileId, accept: bool) -> Result<(), ChatError> {
        let conversation = self
            .conversation_repo
            .find(&conversation_id)
            .await?
            .ok_or_else(|| ChatError::ConversationNotFound { conversation_id: conversation_id.as_str() })?;
        if self.member_repo.find(&conversation_id, &profile).await?.is_none() {
            return Err(conversation.deny_outsider(profile));
        }
        let request = conversation.request();
        let no_request = || ChatError::NoMessageRequest { conversation_id: conversation_id.as_str() };
        let requester = request.requester().ok_or_else(no_request)?;
        if requester == profile {
            return Err(no_request());
        }
        let to = match (accept, request) {
            (true, _) => MessageRequest::Open,
            (false, MessageRequest::Pending { requester }) => MessageRequest::Declined { requester, at: Utc::now() },
            // Declining a declined request again: nothing changes.
            (false, _) => return Ok(()),
        };
        self.conversation_repo.transition_request(&conversation_id, request, to).await?;
        file_answer(&*self.inbox, &profile, &conversation_id, accept).await
    }
}

/// Opening and answering, as the gRPC layer drives them.
#[async_trait]
pub trait DirectMessaging: Send + Sync + 'static {
    async fn open(&self, opener: ProfileId, peer: ProfileId) -> Result<OpenedDirect, ChatError>;
    async fn respond(&self, conversation_id: ConversationId, profile: ProfileId, accept: bool) -> Result<(), ChatError>;
}

#[async_trait]
impl<CR, MR, EP> DirectMessaging for DirectConversations<CR, MR, EP>
where
    CR: ConversationRepository,
    MR: MemberRepository,
    EP: EventPublisher,
{
    async fn open(&self, opener: ProfileId, peer: ProfileId) -> Result<OpenedDirect, ChatError> {
        DirectConversations::open(self, opener, peer).await
    }

    async fn respond(&self, conversation_id: ConversationId, profile: ProfileId, accept: bool) -> Result<(), ChatError> {
        DirectConversations::respond(self, conversation_id, profile, accept).await
    }
}

/// Whether, and how, `sender` (a member) may send to the direct
/// `conversation` (#656). Spends a pending request's one message.
pub(crate) async fn admit<CR: ConversationRepository + ?Sized>(
    conversation_repo: &CR,
    gate:              Option<&Arc<dyn InteractionGate>>,
    conversation:      &Conversation,
    sender:            ProfileId,
) -> Result<Admission, ChatError> {
    let id = conversation.id();
    let Some(peer) = conversation.peer_of(sender) else {
        return Err(conversation.deny_outsider(sender));
    };
    let verdict = verdict(gate, &sender, &peer).await?;
    let withheld = verdict == MessageVerdict::Silenced;
    let mut request = conversation.request();
    match request.requester() {
        // Open: it stays open whatever the settings — a block withholds.
        None => Ok(Admission { withheld, request: false }),
        // The recipient writes back: that accepts.
        Some(requester) if requester != sender => {
            conversation_repo.transition_request(&id, request, MessageRequest::Open).await?;
            Ok(Admission { withheld, request: false })
        }
        // The requester: one message, unless they are refused outright.
        Some(_) => {
            if verdict == MessageVerdict::Refused {
                return Err(ChatError::MessagingNotAllowed { profile_id: peer.as_str() });
            }
            if request.cooled_down(Utc::now()) {
                let fresh = MessageRequest::Pending { requester: sender };
                if conversation_repo.transition_request(&id, request, fresh).await? {
                    request = fresh;
                }
            }
            let pending = matches!(request, MessageRequest::Pending { .. });
            if !pending || !conversation_repo.claim_request_message(&id).await? {
                return Err(ChatError::MessageRequestPending { conversation_id: id.as_str() });
            }
            Ok(Admission { withheld, request: true })
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::application::command::fakes::{FakeConversations, FakeInbox, FakeMembers, FakePublisher, ScriptedGate};

    struct World {
        conversations: Arc<FakeConversations>,
        members:       Arc<FakeMembers>,
        gate:          Arc<ScriptedGate>,
        direct:        DirectConversations<FakeConversations, FakeMembers, FakePublisher>,
    }

    fn world() -> World {
        let (conversations, members, gate) = (Arc::<FakeConversations>::default(), Arc::<FakeMembers>::default(), Arc::<ScriptedGate>::default());
        let direct = DirectConversations {
            conversation_repo: Arc::clone(&conversations),
            member_repo:       Arc::clone(&members),
            publisher:         Arc::default(),
            gate:              Some(Arc::clone(&gate) as Arc<dyn InteractionGate>),
            inbox:             Arc::new(FakeInbox::default()),
        };
        World { conversations, members, gate, direct }
    }

    fn pid() -> ProfileId {
        ProfileId::from_uuid(uuid::Uuid::now_v7())
    }

    impl World {
        async fn send(&self, id: ConversationId, sender: ProfileId) -> Result<Admission, ChatError> {
            let conversation = self.conversations.find(&id).await?.unwrap();
            let gate = Arc::clone(&self.gate) as Arc<dyn InteractionGate>;
            admit(&*self.conversations, Some(&gate), &conversation, sender).await
        }

        async fn request(&self, id: ConversationId) -> MessageRequest {
            self.conversations.find(&id).await.unwrap().unwrap().request()
        }
    }

    #[tokio::test]
    async fn an_admitted_opener_gets_one_open_conversation_per_pair() {
        let w = world();
        let (a, b) = (pid(), pid());
        let first = w.direct.open(a, b).await.unwrap();
        assert!(!first.request);
        assert_eq!(w.direct.open(b, a).await.unwrap().conversation_id, first.conversation_id, "unique per pair");
        assert!(w.members.has(&first.conversation_id, &a) && w.members.has(&first.conversation_id, &b));
        assert_eq!(w.send(first.conversation_id, a).await.unwrap(), Admission::default());
        assert_eq!(w.send(first.conversation_id, a).await.unwrap(), Admission::default(), "no limit once open");
    }

    #[tokio::test]
    async fn a_request_holds_one_message_until_the_recipient_answers() {
        let w = world();
        let (a, b) = (pid(), pid());
        w.gate.set(a, b, MessageVerdict::Request);
        let opened = w.direct.open(a, b).await.unwrap();
        assert!(opened.request);
        let id = opened.conversation_id;

        assert_eq!(w.send(id, a).await.unwrap(), Admission { withheld: false, request: true });
        let err = w.send(id, a).await.unwrap_err();
        assert!(matches!(err, ChatError::MessageRequestPending { .. }), "{err:?}");

        // The recipient replies: that accepts, and the requester writes freely.
        w.send(id, b).await.unwrap();
        assert_eq!(w.request(id).await, MessageRequest::Open);
        assert_eq!(w.send(id, a).await.unwrap(), Admission::default());
    }

    #[tokio::test]
    async fn the_recipient_opening_or_accepting_opens_it() {
        let w = world();
        let (a, b) = (pid(), pid());
        w.gate.set(a, b, MessageVerdict::Request);
        let id = w.direct.open(a, b).await.unwrap().conversation_id;
        assert!(!w.direct.open(b, a).await.unwrap().request);
        assert_eq!(w.request(id).await, MessageRequest::Open);

        let (c, d) = (pid(), pid());
        w.gate.set(c, d, MessageVerdict::Request);
        let id = w.direct.open(c, d).await.unwrap().conversation_id;
        let err = w.direct.respond(id, c, true).await.unwrap_err();
        assert!(matches!(err, ChatError::NoMessageRequest { .. }), "a requester answers nothing: {err:?}");
        w.direct.respond(id, d, true).await.unwrap();
        assert_eq!(w.request(id).await, MessageRequest::Open);
        let err = w.direct.respond(id, d, false).await.unwrap_err();
        assert!(matches!(err, ChatError::NoMessageRequest { .. }), "{err:?}");
    }

    #[tokio::test]
    async fn no_one_refuses_outright_but_an_open_conversation_carries_on() {
        let w = world();
        let (a, b) = (pid(), pid());
        w.gate.set(a, b, MessageVerdict::Refused);
        let err = w.direct.open(a, b).await.unwrap_err();
        assert!(matches!(err, ChatError::MessagingNotAllowed { .. }), "{err:?}");

        let (c, d) = (pid(), pid());
        let id = w.direct.open(c, d).await.unwrap().conversation_id;
        w.gate.set(c, d, MessageVerdict::Refused);
        assert_eq!(w.send(id, c).await.unwrap(), Admission::default(), "accepted before: it continues");
    }

    /// The blocked sender sees exactly what a sender whose request is pending
    /// or declined sees: a request, one message accepted, then "pending".
    #[tokio::test]
    async fn a_block_looks_like_an_unanswered_request_and_withholds() {
        let w = world();
        let (blocked, blocker) = (pid(), pid());
        w.gate.set(blocked, blocker, MessageVerdict::Silenced);
        let silenced = w.direct.open(blocked, blocker).await.unwrap();

        let (declined, decliner) = (pid(), pid());
        w.gate.set(declined, decliner, MessageVerdict::Request);
        let refused = w.direct.open(declined, decliner).await.unwrap();

        assert_eq!(silenced.request, refused.request, "both are requests");
        let first = w.send(silenced.conversation_id, blocked).await.unwrap();
        assert_eq!(first, Admission { withheld: true, request: true }, "accepted, never delivered");
        w.send(refused.conversation_id, declined).await.unwrap();
        w.direct.respond(refused.conversation_id, decliner, false).await.unwrap();

        let a = w.send(silenced.conversation_id, blocked).await.unwrap_err();
        let b = w.send(refused.conversation_id, declined).await.unwrap_err();
        assert_eq!(a.to_string().replace(&silenced.conversation_id.as_str(), "_"), b.to_string().replace(&refused.conversation_id.as_str(), "_"));
        assert!(matches!(a, ChatError::MessageRequestPending { .. }));
        assert!(w.direct.open(blocked, blocker).await.unwrap().request);
        assert!(w.direct.open(declined, decliner).await.unwrap().request, "a decline is silent");

        // Blocked inside an open conversation: still sent, withheld.
        let (c, d) = (pid(), pid());
        let id = w.direct.open(c, d).await.unwrap().conversation_id;
        w.gate.set(c, d, MessageVerdict::Silenced);
        assert_eq!(w.send(id, c).await.unwrap(), Admission { withheld: true, request: false });
    }

    #[tokio::test]
    async fn a_declined_requester_may_ask_again_after_thirty_days() {
        let w = world();
        let (a, b) = (pid(), pid());
        w.gate.set(a, b, MessageVerdict::Request);
        let id = w.direct.open(a, b).await.unwrap().conversation_id;
        w.send(id, a).await.unwrap();
        w.direct.respond(id, b, false).await.unwrap();
        let declined_at = Utc::now() - chrono::Duration::days(31);
        let declined = MessageRequest::Declined { requester: a, at: declined_at };
        w.conversations.force_request(&id, declined);

        assert_eq!(w.send(id, a).await.unwrap(), Admission { withheld: false, request: true }, "a fresh request");
        assert_eq!(w.request(id).await, MessageRequest::Pending { requester: a });
        assert!(w.send(id, a).await.is_err(), "and again just one message");
    }

    #[tokio::test]
    async fn without_the_gate_nothing_opens_and_nobody_messages_themselves() {
        let w = world();
        let off = DirectConversations { gate: None, ..w.direct };
        let err = off.open(pid(), pid()).await.unwrap_err();
        assert!(matches!(err, ChatError::InteractionCheckUnavailable { .. }), "{err:?}");
        let me = pid();
        assert!(matches!(off.open(me, me).await.unwrap_err(), ChatError::DomainViolation { .. }));
    }
}
