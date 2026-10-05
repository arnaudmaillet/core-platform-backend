//! In-memory port fakes for the command-handler unit tests (no Scylla/Kafka).

use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use chrono::{DateTime, Utc};

use uuid::Uuid;

use crate::application::port::{
    ConversationRepository, EventPublisher, InteractionGate, InvitationRepository, MemberRepository,
    MessageRepository, MessageSummary, MessageVerdict, SubscriptionRepository,
};
use crate::domain::aggregate::{Conversation, Direct, Invitation, Message, Participant};
use crate::domain::event::{DomainEvent, MessageEvent};
use crate::domain::value_object::{
    ConversationId, ConversationKind, MessageId, MessageRequest, ProfileId, Role, Visibility,
};
use crate::error::ChatError;

#[derive(Clone, Copy)]
struct ConversationState {
    kind:         ConversationKind,
    visibility:   Visibility,
    owner:        ProfileId,
    member_count: u16,
    public_since: Option<MessageId>,
    direct:       Option<Direct>,
    request_sent: bool,
    created_at:   DateTime<Utc>,
    updated_at:   DateTime<Utc>,
}

#[derive(Default)]
pub struct FakeConversations {
    rows:  Mutex<HashMap<ConversationId, ConversationState>>,
    pairs: Mutex<HashMap<(Uuid, Uuid), ConversationId>>,
}

impl FakeConversations {
    fn put(&self, c: &Conversation) {
        let mut rows = self.rows.lock().unwrap();
        let request_sent = rows.get(&c.id()).is_some_and(|s| s.request_sent);
        rows.insert(
            c.id(),
            ConversationState {
                kind:         c.kind(),
                visibility:   c.visibility(),
                owner:        c.owner_id(),
                member_count: c.member_count(),
                public_since: c.public_since(),
                direct:       c.direct(),
                request_sent,
                created_at:   c.created_at(),
                updated_at:   c.updated_at(),
            },
        );
    }

    fn load(&self, id: &ConversationId) -> Option<Conversation> {
        self.rows.lock().unwrap().get(id).map(|s| {
            Conversation::reconstitute(
                *id, s.kind, s.visibility, s.owner, s.member_count, s.public_since, s.direct, s.created_at, s.updated_at,
            )
        })
    }

    pub fn member_count(&self, id: &ConversationId) -> u16 {
        self.rows.lock().unwrap()[id].member_count
    }

    /// Sets a direct conversation's request as is (e.g. a decline back-dated).
    pub fn force_request(&self, id: &ConversationId, request: MessageRequest) {
        if let Some(direct) = self.rows.lock().unwrap().get_mut(id).and_then(|s| s.direct.as_mut()) {
            direct.request = request;
        }
    }
}

#[async_trait]
impl ConversationRepository for FakeConversations {
    async fn insert(&self, c: &Conversation) -> Result<(), ChatError> {
        self.put(c);
        Ok(())
    }

    async fn update(&self, c: &Conversation) -> Result<(), ChatError> {
        self.put(c);
        Ok(())
    }

    async fn find(&self, id: &ConversationId) -> Result<Option<Conversation>, ChatError> {
        Ok(self.load(id))
    }

    async fn claim_direct(&self, a: &ProfileId, b: &ProfileId, proposed: ConversationId) -> Result<ConversationId, ChatError> {
        let key = if a.as_uuid() < b.as_uuid() { (a.as_uuid(), b.as_uuid()) } else { (b.as_uuid(), a.as_uuid()) };
        Ok(*self.pairs.lock().unwrap().entry(key).or_insert(proposed))
    }

    async fn insert_direct(&self, c: &Conversation) -> Result<bool, ChatError> {
        if self.rows.lock().unwrap().contains_key(&c.id()) {
            return Ok(false);
        }
        self.put(c);
        Ok(true)
    }

    async fn transition_request(&self, id: &ConversationId, from: MessageRequest, to: MessageRequest) -> Result<bool, ChatError> {
        let mut rows = self.rows.lock().unwrap();
        let Some(state) = rows.get_mut(id) else { return Ok(false) };
        let Some(direct) = state.direct.as_mut() else { return Ok(false) };
        if direct.request.as_tinyint() != from.as_tinyint() {
            return Ok(false);
        }
        if matches!(from, MessageRequest::Declined { .. }) {
            state.request_sent = false;
        }
        direct.request = to;
        Ok(true)
    }

    async fn claim_request_message(&self, id: &ConversationId) -> Result<bool, ChatError> {
        let mut rows = self.rows.lock().unwrap();
        let Some(state) = rows.get_mut(id) else { return Ok(false) };
        Ok(!std::mem::replace(&mut state.request_sent, true))
    }
}

/// The interaction gate, scripted per (actor, recipient); everyone else is
/// allowed.
#[derive(Default)]
pub struct ScriptedGate(Mutex<HashMap<(ProfileId, ProfileId), MessageVerdict>>);

impl ScriptedGate {
    /// `actor` → `recipient` gets `verdict`; a block (`Silenced`) holds both ways.
    pub fn set(&self, actor: ProfileId, recipient: ProfileId, verdict: MessageVerdict) {
        let mut verdicts = self.0.lock().unwrap();
        verdicts.insert((actor, recipient), verdict);
        if verdict == MessageVerdict::Silenced {
            verdicts.insert((recipient, actor), verdict);
        }
    }
}

#[async_trait]
impl InteractionGate for ScriptedGate {
    async fn may_message(&self, actor: &ProfileId, recipient: &ProfileId) -> Result<MessageVerdict, ChatError> {
        Ok(self.0.lock().unwrap().get(&(*actor, *recipient)).copied().unwrap_or(MessageVerdict::Allowed))
    }
}

#[derive(Default)]
pub struct FakeMembers(Mutex<HashMap<(ConversationId, ProfileId), Role>>);

impl FakeMembers {
    pub fn has(&self, c: &ConversationId, p: &ProfileId) -> bool {
        self.0.lock().unwrap().contains_key(&(*c, *p))
    }

    pub fn remove(&self, c: &ConversationId, p: &ProfileId) {
        self.0.lock().unwrap().remove(&(*c, *p));
    }
}

#[async_trait]
impl MemberRepository for FakeMembers {
    async fn insert(&self, c: &ConversationId, p: &Participant) -> Result<(), ChatError> {
        self.0.lock().unwrap().insert((*c, p.profile_id()), p.role());
        Ok(())
    }

    async fn find(&self, c: &ConversationId, m: &ProfileId) -> Result<Option<Participant>, ChatError> {
        Ok(self.0.lock().unwrap().get(&(*c, *m)).map(|&role| Participant::reconstitute(*m, role, Utc::now(), None)))
    }

    async fn update_last_read(&self, _: &ConversationId, _: &ProfileId, _: MessageId) -> Result<(), ChatError> {
        Ok(())
    }

    async fn list(&self, c: &ConversationId) -> Result<Vec<Participant>, ChatError> {
        Ok(self
            .0
            .lock()
            .unwrap()
            .iter()
            .filter(|((conv, _), _)| conv == c)
            .map(|(&(_, p), &role)| Participant::reconstitute(p, role, Utc::now(), None))
            .collect())
    }

    async fn delete(&self, c: &ConversationId, m: &ProfileId) -> Result<(), ChatError> {
        self.remove(c, m);
        Ok(())
    }

    async fn list_by_member(
        &self,
        m: &ProfileId,
        limit: i32,
        after: Option<&ConversationId>,
    ) -> Result<Vec<crate::application::port::Membership>, ChatError> {
        let mut mine: Vec<_> = self
            .0
            .lock()
            .unwrap()
            .iter()
            .filter(|((c, p), _)| p == m && after.is_none_or(|a| c.as_uuid() > a.as_uuid()))
            .map(|(&(c, _), &role)| crate::application::port::Membership { conversation_id: c, role, joined_at: Utc::now() })
            .collect();
        mine.sort_by_key(|ms| ms.conversation_id.as_uuid());
        mine.truncate(limit.max(1) as usize);
        Ok(mine)
    }

    async fn backfill_member_index(&self) -> Result<u64, ChatError> {
        Ok(0)
    }
}

#[derive(Default)]
pub struct FakeInvitations(Mutex<HashMap<(ConversationId, ProfileId), Invitation>>, AtomicBool);

impl FakeInvitations {
    pub fn has(&self, c: &ConversationId, p: &ProfileId) -> bool {
        self.0.lock().unwrap().contains_key(&(*c, *p))
    }

    /// Makes every subsequent `delete` fail (storage fault on consume).
    pub fn fail_deletes(&self) {
        self.1.store(true, Ordering::SeqCst);
    }
}

#[async_trait]
impl InvitationRepository for FakeInvitations {
    async fn upsert(&self, c: &ConversationId, i: &Invitation) -> Result<(), ChatError> {
        self.0.lock().unwrap().insert((*c, i.invitee_id()), i.clone());
        Ok(())
    }

    async fn find(&self, c: &ConversationId, p: &ProfileId) -> Result<Option<Invitation>, ChatError> {
        Ok(self.0.lock().unwrap().get(&(*c, *p)).cloned())
    }

    async fn delete(&self, c: &ConversationId, p: &ProfileId) -> Result<(), ChatError> {
        if self.1.load(Ordering::SeqCst) {
            return Err(ChatError::DomainViolation {
                field:   "invitation.delete".to_owned(),
                message: "injected fault".to_owned(),
            });
        }
        self.0.lock().unwrap().remove(&(*c, *p));
        Ok(())
    }
}

#[derive(Default)]
pub struct FakePublisher(Mutex<usize>);

impl FakePublisher {
    pub fn count(&self) -> usize {
        *self.0.lock().unwrap()
    }
}

#[async_trait]
impl EventPublisher for FakePublisher {
    async fn publish_conversation(&self, _: &DomainEvent) -> Result<(), ChatError> {
        *self.0.lock().unwrap() += 1;
        Ok(())
    }

    async fn publish_message(&self, _: &MessageEvent) -> Result<(), ChatError> {
        Ok(())
    }
}

/// Message log fake: counts writes and records the visibility floor each
/// history read was served with (`None` = full member history).
#[derive(Default)]
pub struct FakeMessages {
    inserts:  Mutex<usize>,
    floors:   Mutex<Vec<Option<i64>>>,
    withheld: Mutex<Vec<bool>>,
}

impl FakeMessages {
    pub fn inserts(&self) -> usize {
        *self.inserts.lock().unwrap()
    }

    pub fn floors(&self) -> Vec<Option<i64>> {
        self.floors.lock().unwrap().clone()
    }

    /// Whether each inserted message was withheld, in order.
    pub fn withheld(&self) -> Vec<bool> {
        self.withheld.lock().unwrap().clone()
    }
}

#[async_trait]
impl MessageRepository for FakeMessages {
    async fn insert(&self, m: &Message) -> Result<(), ChatError> {
        *self.inserts.lock().unwrap() += 1;
        self.withheld.lock().unwrap().push(m.withheld());
        Ok(())
    }

    async fn list_history(
        &self,
        _: &ConversationId,
        _: i32,
        _: Option<(i64, Uuid)>,
        floor: Option<i64>,
    ) -> Result<(Vec<MessageSummary>, Option<(i64, Uuid)>), ChatError> {
        self.floors.lock().unwrap().push(floor);
        Ok((Vec::new(), None))
    }
}

#[derive(Default)]
pub struct FakeSubscriptions(Mutex<HashSet<(ConversationId, ProfileId)>>);

impl FakeSubscriptions {
    pub fn has(&self, c: &ConversationId, p: &ProfileId) -> bool {
        self.0.lock().unwrap().contains(&(*c, *p))
    }
}

#[async_trait]
impl SubscriptionRepository for FakeSubscriptions {
    async fn subscribe(&self, c: &ConversationId, p: &ProfileId) -> Result<(), ChatError> {
        self.0.lock().unwrap().insert((*c, *p));
        Ok(())
    }

    async fn unsubscribe(&self, c: &ConversationId, p: &ProfileId) -> Result<(), ChatError> {
        self.0.lock().unwrap().remove(&(*c, *p));
        Ok(())
    }

    async fn is_subscribed(&self, p: &ProfileId, c: &ConversationId) -> Result<bool, ChatError> {
        Ok(self.has(c, p))
    }

    async fn list_by_user(
        &self,
        _: &ProfileId,
        _: i32,
        _: Option<Uuid>,
    ) -> Result<(Vec<ConversationId>, Option<Uuid>), ChatError> {
        Ok((Vec::new(), None))
    }
}

/// One group conversation (owner on the roster) wired over fresh fakes.
pub struct Fixture {
    pub conversation_id: ConversationId,
    pub owner:           ProfileId,
    pub conversations:   Arc<FakeConversations>,
    pub members:         Arc<FakeMembers>,
    pub invitations:     Arc<FakeInvitations>,
    pub publisher:       Arc<FakePublisher>,
}

impl Fixture {
    pub fn profile() -> ProfileId {
        ProfileId::from_uuid(uuid::Uuid::now_v7())
    }

    /// A `Group` as created: `Private`, owner-only roster.
    pub fn private_group() -> Self {
        Self::group(false)
    }

    /// A `Group` toggled `Public`.
    pub fn public_group() -> Self {
        Self::group(true)
    }

    fn group(public: bool) -> Self {
        let owner = Self::profile();
        let mut conversation = Conversation::create(ConversationId::new(), ConversationKind::Group, owner);
        if public {
            conversation.publish().unwrap();
        }
        let f = Self {
            conversation_id: conversation.id(),
            owner,
            conversations:   Arc::default(),
            members:         Arc::default(),
            invitations:     Arc::default(),
            publisher:       Arc::default(),
        };
        f.conversations.put(&conversation);
        f.members.0.lock().unwrap().insert((f.conversation_id, owner), Role::Owner);
        f
    }

    /// Puts a new profile on the roster with `role` (bypassing the join path).
    pub fn add_member(&self, role: Role) -> ProfileId {
        let p = Self::profile();
        self.members.0.lock().unwrap().insert((self.conversation_id, p), role);
        p
    }

    /// Records a pending invitation for `invitee`, issued by the owner.
    pub fn invite(&self, invitee: ProfileId) {
        let owner = Participant::reconstitute(self.owner, Role::Owner, Utc::now(), None);
        let conversation = self.conversations.load(&self.conversation_id).unwrap();
        let invitation = conversation.invite(&owner, invitee).unwrap();
        self.invitations.0.lock().unwrap().insert((self.conversation_id, invitee), invitation);
    }
}
