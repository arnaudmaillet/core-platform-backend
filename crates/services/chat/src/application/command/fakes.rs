//! In-memory port fakes for the command-handler unit tests (no Scylla/Kafka).

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use chrono::{DateTime, Utc};

use crate::application::port::{
    ConversationRepository, EventPublisher, InvitationRepository, MemberRepository,
};
use crate::domain::aggregate::{Conversation, Invitation, Participant};
use crate::domain::event::{DomainEvent, MessageEvent};
use crate::domain::value_object::{
    ConversationId, ConversationKind, MessageId, ProfileId, Role, Visibility,
};
use crate::error::ChatError;

type ConversationState = (
    ConversationKind,
    Visibility,
    ProfileId,
    u16,
    Option<MessageId>,
    DateTime<Utc>,
    DateTime<Utc>,
);

#[derive(Default)]
pub struct FakeConversations(Mutex<HashMap<ConversationId, ConversationState>>);

impl FakeConversations {
    fn put(&self, c: &Conversation) {
        self.0.lock().unwrap().insert(
            c.id(),
            (c.kind(), c.visibility(), c.owner_id(), c.member_count(), c.public_since(), c.created_at(), c.updated_at()),
        );
    }

    fn load(&self, id: &ConversationId) -> Option<Conversation> {
        self.0.lock().unwrap().get(id).map(|&(kind, vis, owner, count, since, created, updated)| {
            Conversation::reconstitute(*id, kind, vis, owner, count, since, created, updated)
        })
    }

    pub fn member_count(&self, id: &ConversationId) -> u16 {
        self.0.lock().unwrap()[id].3
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
}

#[derive(Default)]
pub struct FakeInvitations(Mutex<HashMap<(ConversationId, ProfileId), Invitation>>);

impl FakeInvitations {
    pub fn has(&self, c: &ConversationId, p: &ProfileId) -> bool {
        self.0.lock().unwrap().contains_key(&(*c, *p))
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
