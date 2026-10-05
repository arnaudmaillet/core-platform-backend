use chrono::{DateTime, Utc};

use crate::domain::event::{
    ConversationCreatedEvent, ConversationPublishedEvent, ConversationUnpublishedEvent, DomainEvent,
    MemberJoinedEvent, MemberLeftEvent,
};
use crate::domain::aggregate::{Invitation, Participant};
use crate::domain::value_object::{
    ConversationId, ConversationKind, ConversationPolicy, MessageId, ProfileId, Role, Visibility,
};
use crate::error::ChatError;

/// The Conversation aggregate root — the unified layer over Groups and Channels.
///
/// # Consistency boundary
///
/// The aggregate holds only what it must to enforce its invariants: the
/// immutable topology, the mutable visibility, the owner, the bounded
/// **member count**, and the public-since watermark. It deliberately does **not**
/// hold the audience: subscribers and guests are a read-side projection and are
/// never hydrated here — that separation is what keeps a viral public
/// conversation from dragging millions of passive readers through the member
/// write/presence loops (the Shadowing Pattern).
///
/// # Invariants
///
/// - Topology ([`ConversationKind`]) is immutable after creation.
/// - The member count never exceeds [`ConversationPolicy::max_members`].
/// - Visibility transitions are monotone guards: you cannot publish a public
///   conversation, nor unpublish a private one.
/// - A `Channel` is born `Public`; a `Group` is born `Private`.
/// - Joining a `Private` conversation requires a pending [`Invitation`] for the
///   joiner, issued by an administering member; a `Public` one is open-join.
///
/// Lifecycle transitions buffer a [`DomainEvent`]; drain them with
/// [`Conversation::take_events`] after persisting, mirroring the `post` service.
pub struct Conversation {
    id:             ConversationId,
    kind:           ConversationKind,
    visibility:     Visibility,
    owner_id:       ProfileId,
    member_count:   u16,
    /// Watermark set when the conversation became public: the audience may read
    /// only messages with `id >= public_since`. `None` while private.
    public_since:   Option<MessageId>,
    created_at:     DateTime<Utc>,
    updated_at:     DateTime<Utc>,
    pending_events: Vec<DomainEvent>,
}

impl Conversation {
    /// Creates a new conversation owned by `owner_id`.
    ///
    /// A `Channel` is born `Public` (it is inherently a `1 -> N` broadcast), with
    /// its public-since watermark stamped at creation. A `Group` is born
    /// `Private`. The owner is the first and only initial member.
    pub fn create(id: ConversationId, kind: ConversationKind, owner_id: ProfileId) -> Self {
        let now = Utc::now();

        let (visibility, public_since) = match kind {
            ConversationKind::Channel => (Visibility::Public, Some(MessageId::new())),
            ConversationKind::Group   => (Visibility::Private, None),
        };

        let mut conversation = Self {
            id,
            kind,
            visibility,
            owner_id,
            member_count: 1,
            public_since,
            created_at: now,
            updated_at: now,
            pending_events: Vec::new(),
        };

        conversation.pending_events.push(DomainEvent::ConversationCreated(
            ConversationCreatedEvent {
                conversation_id: id.as_str(),
                kind:            kind.as_str().to_owned(),
                visibility:      visibility.as_str().to_owned(),
                owner_id:        owner_id.as_str(),
                created_at_ms:   now.timestamp_millis(),
            },
        ));

        conversation
    }

    /// Reconstitutes an aggregate from persisted state (no events buffered).
    #[allow(clippy::too_many_arguments)]
    pub fn reconstitute(
        id:           ConversationId,
        kind:         ConversationKind,
        visibility:   Visibility,
        owner_id:     ProfileId,
        member_count: u16,
        public_since: Option<MessageId>,
        created_at:   DateTime<Utc>,
        updated_at:   DateTime<Utc>,
    ) -> Self {
        Self {
            id,
            kind,
            visibility,
            owner_id,
            member_count,
            public_since,
            created_at,
            updated_at,
            pending_events: Vec::new(),
        }
    }

    /// Toggles the conversation to `Public`, attaching the Audience Plane.
    ///
    /// Stamps the public-since watermark at the current instant so the audience
    /// sees history only from this boundary forward (the approved
    /// watermark-default privacy rule). Authorization (admin check) is enforced
    /// by the application layer, consistent with the other services.
    ///
    /// Returns the watermark on success, or
    /// [`ChatError::ConversationAlreadyPublic`] if already public.
    pub fn publish(&mut self) -> Result<MessageId, ChatError> {
        if self.visibility.is_public() {
            return Err(ChatError::ConversationAlreadyPublic { conversation_id: self.id.as_str() });
        }

        let now       = Utc::now();
        let watermark = MessageId::new();

        self.visibility   = Visibility::Public;
        self.public_since = Some(watermark);
        self.updated_at   = now;

        self.pending_events.push(DomainEvent::ConversationPublished(ConversationPublishedEvent {
            conversation_id: self.id.as_str(),
            public_since:    watermark.as_str(),
            published_at_ms: now.timestamp_millis(),
        }));

        Ok(watermark)
    }

    /// Toggles the conversation back to `Private`, detaching the Audience Plane
    /// and clearing the watermark. The infrastructure layer reacts to the emitted
    /// event by tearing down audience channels and cancelling live guest streams.
    ///
    /// Returns [`ChatError::ConversationAlreadyPrivate`] if already private.
    pub fn unpublish(&mut self) -> Result<(), ChatError> {
        if !self.visibility.is_public() {
            return Err(ChatError::ConversationAlreadyPrivate { conversation_id: self.id.as_str() });
        }

        let now = Utc::now();

        self.visibility   = Visibility::Private;
        self.public_since = None;
        self.updated_at   = now;

        self.pending_events.push(DomainEvent::ConversationUnpublished(
            ConversationUnpublishedEvent {
                conversation_id:   self.id.as_str(),
                unpublished_at_ms: now.timestamp_millis(),
            },
        ));

        Ok(())
    }

    /// Admits a profile to the bounded Member Plane, enforcing the roster cap.
    ///
    /// `role` must be a member-plane role; audience roles are rejected with
    /// [`ChatError::InvalidParticipantRole`]. Returns
    /// [`ChatError::MemberLimitExceeded`] when the cap is reached. The caller is
    /// responsible for persisting the corresponding `Participant` row.
    pub fn admit_member(&mut self, profile_id: ProfileId, role: Role) -> Result<(), ChatError> {
        if !role.is_member_plane() {
            return Err(ChatError::InvalidParticipantRole { role: role.as_str().to_owned() });
        }

        let limit = self.policy().max_members;
        if self.member_count >= limit {
            return Err(ChatError::MemberLimitExceeded { conversation_id: self.id.as_str(), limit });
        }

        let now = Utc::now();
        self.member_count += 1;
        self.updated_at = now;

        self.pending_events.push(DomainEvent::MemberJoined(MemberJoinedEvent {
            conversation_id: self.id.as_str(),
            profile_id:      profile_id.as_str(),
            role:            role.as_str().to_owned(),
            joined_at_ms:    now.timestamp_millis(),
        }));

        Ok(())
    }

    /// Issues an invitation for `invitee_id` to join, on behalf of `inviter`.
    ///
    /// Only an administering member (owner/admin) may invite; anyone else gets
    /// [`ChatError::NotAuthorized`]. The caller resolves `inviter` from the roster
    /// (a non-member never reaches this method) and checks that the invitee is
    /// not already a member.
    pub fn invite(&self, inviter: &Participant, invitee_id: ProfileId) -> Result<Invitation, ChatError> {
        if !inviter.can_administer() {
            return Err(ChatError::NotAuthorized {
                profile_id:      inviter.profile_id().as_str(),
                conversation_id: self.id.as_str(),
            });
        }
        Ok(Invitation::issue(invitee_id, inviter.profile_id()))
    }

    /// Admits `profile_id` as a regular `Member` through a self-service join.
    ///
    /// A `Public` conversation is open-join. A `Private` one requires
    /// `invitation` to be a pending invitation **for this profile**; without it
    /// the join fails with [`ChatError::ConversationConcealed`], which the edge
    /// renders exactly like a missing conversation so a non-invited caller
    /// cannot learn that the private conversation exists. The roster cap is then
    /// enforced by [`Conversation::admit_member`].
    pub fn admit_joiner(
        &mut self,
        profile_id: ProfileId,
        invitation: Option<&Invitation>,
    ) -> Result<(), ChatError> {
        let invited = invitation.is_some_and(|i| i.invitee_id() == profile_id);
        if !self.visibility.is_public() && !invited {
            return Err(self.concealed());
        }
        self.admit_member(profile_id, Role::Member)
    }

    /// The error a caller with no standing in this conversation gets when it is
    /// `Private`: indistinguishable at the edge from "not found".
    pub fn concealed(&self) -> ChatError {
        ChatError::ConversationConcealed { conversation_id: self.id.as_str() }
    }

    /// Releases a profile from the Member Plane. The owner cannot leave (a
    /// conversation always has an owner); ownership transfer is a separate
    /// operation handled at the application layer.
    pub fn release_member(&mut self, profile_id: ProfileId) -> Result<(), ChatError> {
        if profile_id == self.owner_id {
            return Err(ChatError::DomainViolation {
                field:   "owner_id".to_owned(),
                message: "the owner cannot leave the conversation; transfer ownership first".to_owned(),
            });
        }

        let now = Utc::now();
        self.member_count = self.member_count.saturating_sub(1);
        self.updated_at = now;

        self.pending_events.push(DomainEvent::MemberLeft(MemberLeftEvent {
            conversation_id: self.id.as_str(),
            profile_id:      profile_id.as_str(),
            left_at_ms:      now.timestamp_millis(),
        }));

        Ok(())
    }

    /// Whether `message_id` is visible to the Audience Plane, per the watermark.
    /// Members always see the full history; this guard applies to subscribers and
    /// guests only.
    pub fn is_visible_to_audience(&self, message_id: MessageId) -> bool {
        match self.public_since {
            Some(watermark) => message_id >= watermark,
            None            => false,
        }
    }

    /// The derived runtime policy gating presence, receipts, the roster cap, and
    /// the Audience Plane.
    pub fn policy(&self) -> ConversationPolicy {
        ConversationPolicy::derive(self.kind, self.visibility)
    }

    /// Drains buffered lifecycle events for publication after a successful
    /// persist.
    pub fn take_events(&mut self) -> Vec<DomainEvent> {
        std::mem::take(&mut self.pending_events)
    }

    pub fn id(&self)           -> ConversationId      { self.id }
    pub fn kind(&self)         -> ConversationKind    { self.kind }
    pub fn visibility(&self)   -> Visibility          { self.visibility }
    pub fn owner_id(&self)     -> ProfileId           { self.owner_id }
    pub fn member_count(&self) -> u16                 { self.member_count }
    pub fn public_since(&self) -> Option<MessageId>   { self.public_since }
    pub fn created_at(&self)   -> DateTime<Utc>       { self.created_at }
    pub fn updated_at(&self)   -> DateTime<Utc>       { self.updated_at }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn pid() -> ProfileId {
        ProfileId::from_uuid(uuid::Uuid::now_v7())
    }

    fn group() -> Conversation {
        let mut c = Conversation::create(ConversationId::new(), ConversationKind::Group, pid());
        c.take_events();
        c
    }

    fn joined_events(c: &mut Conversation) -> usize {
        c.take_events().iter().filter(|e| matches!(e, DomainEvent::MemberJoined(_))).count()
    }

    #[test]
    fn private_join_without_invitation_is_concealed() {
        let mut c = group();
        let err = c.admit_joiner(pid(), None).unwrap_err();
        assert!(matches!(err, ChatError::ConversationConcealed { .. }), "{err:?}");
        assert_eq!(c.member_count(), 1, "a refused join must not touch the roster");
        assert_eq!(joined_events(&mut c), 0);
    }

    #[test]
    fn private_join_with_someone_elses_invitation_is_concealed() {
        let mut c = group();
        let owner = Participant::new(c.owner_id(), Role::Owner).unwrap();
        let invitation = c.invite(&owner, pid()).unwrap();
        let err = c.admit_joiner(pid(), Some(&invitation)).unwrap_err();
        assert!(matches!(err, ChatError::ConversationConcealed { .. }), "{err:?}");
        assert_eq!(c.member_count(), 1);
    }

    #[test]
    fn private_join_with_own_invitation_is_admitted() {
        let mut c = group();
        let owner = Participant::new(c.owner_id(), Role::Owner).unwrap();
        let invitee = pid();
        let invitation = c.invite(&owner, invitee).unwrap();
        assert_eq!(invitation.invitee_id(), invitee);
        assert_eq!(invitation.inviter_id(), c.owner_id());

        c.admit_joiner(invitee, Some(&invitation)).unwrap();
        assert_eq!(c.member_count(), 2);
        assert_eq!(joined_events(&mut c), 1);
    }

    #[test]
    fn public_join_is_open() {
        let mut c = group();
        c.publish().unwrap();
        c.take_events();
        c.admit_joiner(pid(), None).unwrap();
        assert_eq!(c.member_count(), 2);
        assert_eq!(joined_events(&mut c), 1);
    }

    #[test]
    fn unpublishing_closes_open_join() {
        let mut c = group();
        c.publish().unwrap();
        c.unpublish().unwrap();
        let err = c.admit_joiner(pid(), None).unwrap_err();
        assert!(matches!(err, ChatError::ConversationConcealed { .. }), "{err:?}");
    }

    #[test]
    fn invited_join_still_respects_the_roster_cap() {
        let mut c = Conversation::reconstitute(
            ConversationId::new(),
            ConversationKind::Group,
            Visibility::Private,
            pid(),
            ConversationKind::Group.max_members(),
            None,
            Utc::now(),
            Utc::now(),
        );
        let owner = Participant::new(c.owner_id(), Role::Owner).unwrap();
        let invitee = pid();
        let invitation = c.invite(&owner, invitee).unwrap();
        let err = c.admit_joiner(invitee, Some(&invitation)).unwrap_err();
        assert!(matches!(err, ChatError::MemberLimitExceeded { .. }), "{err:?}");
    }

    #[test]
    fn only_owner_or_admin_may_invite() {
        let c = group();
        let admin = Participant::new(pid(), Role::Admin).unwrap();
        assert!(c.invite(&admin, pid()).is_ok());

        let member = Participant::new(pid(), Role::Member).unwrap();
        let err = c.invite(&member, pid()).unwrap_err();
        assert!(matches!(err, ChatError::NotAuthorized { .. }), "{err:?}");
    }
}
