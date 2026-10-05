use std::sync::Arc;

use cqrs::{Command, CommandHandler, Envelope};
use validate_core::{FieldViolation, Validate};

use crate::application::port::{ConversationRepository, InvitationRepository, MemberRepository};
use crate::domain::value_object::{ConversationId, ProfileId};
use crate::error::ChatError;

/// Invites a profile to join a conversation's Member Plane.
///
/// The inviter must be an owner/admin. The pending invitation is what lets the
/// invitee [`JoinAsMember`](super::JoinAsMemberCommand) a **private**
/// conversation; it expires after 7 days (table TTL) and re-inviting refreshes
/// it. A non-member inviter of a private conversation gets the same concealed
/// "not found" as a non-invited joiner, so neither RPC is an existence oracle.
pub struct InviteMemberCommand {
    pub conversation_id: String,
    pub inviter_id:      String,
    pub invitee_id:      String,
}

impl Command for InviteMemberCommand {}

impl Validate for InviteMemberCommand {
    fn validate(&self) -> Result<(), Vec<FieldViolation>> {
        let mut v = Vec::new();
        if self.conversation_id.trim().is_empty() {
            v.push(FieldViolation::new(
                "conversation_id",
                "CHT-VAL-060",
                "conversation_id must not be empty",
            ));
        }
        if self.inviter_id.trim().is_empty() {
            v.push(FieldViolation::new("inviter_id", "CHT-VAL-061", "inviter_id must not be empty"));
        }
        if self.invitee_id.trim().is_empty() {
            v.push(FieldViolation::new("invitee_id", "CHT-VAL-062", "invitee_id must not be empty"));
        }
        if v.is_empty() { Ok(()) } else { Err(v) }
    }
}

pub struct InviteMemberHandler<CR, MR, IR> {
    pub conversation_repo: Arc<CR>,
    pub member_repo:       Arc<MR>,
    pub invitation_repo:   Arc<IR>,
}

impl<CR, MR, IR> CommandHandler<InviteMemberCommand> for InviteMemberHandler<CR, MR, IR>
where
    CR: ConversationRepository,
    MR: MemberRepository,
    IR: InvitationRepository,
{
    type Error = ChatError;

    async fn handle(&self, envelope: Envelope<InviteMemberCommand>) -> Result<(), ChatError> {
        let cmd = &envelope.payload;

        let conversation_id = ConversationId::try_from(cmd.conversation_id.as_str())?;
        let inviter_id      = ProfileId::try_from(cmd.inviter_id.as_str())?;
        let invitee_id      = ProfileId::try_from(cmd.invitee_id.as_str())?;

        let conversation = self
            .conversation_repo
            .find(&conversation_id)
            .await?
            .ok_or_else(|| ChatError::ConversationNotFound {
                conversation_id: conversation_id.as_str(),
            })?;

        let Some(inviter) = self.member_repo.find(&conversation_id, &inviter_id).await? else {
            // An outsider must not learn that a private conversation exists.
            return Err(conversation.deny_outsider(inviter_id));
        };

        // Authority first (owner/admin), so a plain member cannot use the
        // AlreadyMember answer to probe the roster through this RPC.
        let invitation = conversation.invite(&inviter, invitee_id)?;

        if self.member_repo.find(&conversation_id, &invitee_id).await?.is_some() {
            return Err(ChatError::AlreadyMember {
                profile_id:      invitee_id.as_str(),
                conversation_id: conversation_id.as_str(),
            });
        }

        self.invitation_repo.upsert(&conversation_id, &invitation).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::application::command::fakes::Fixture;
    use crate::domain::value_object::Role;

    fn cmd(f: &Fixture, inviter: ProfileId, invitee: ProfileId) -> Envelope<InviteMemberCommand> {
        Envelope::new(uuid::Uuid::now_v7(), InviteMemberCommand {
            conversation_id: f.conversation_id.as_str(),
            inviter_id:      inviter.as_str(),
            invitee_id:      invitee.as_str(),
        })
    }

    fn handler(f: &Fixture) -> InviteMemberHandler<
        crate::application::command::fakes::FakeConversations,
        crate::application::command::fakes::FakeMembers,
        crate::application::command::fakes::FakeInvitations,
    > {
        InviteMemberHandler {
            conversation_repo: Arc::clone(&f.conversations),
            member_repo:       Arc::clone(&f.members),
            invitation_repo:   Arc::clone(&f.invitations),
        }
    }

    #[tokio::test]
    async fn owner_and_admin_can_invite() {
        let f = Fixture::private_group();
        let admin = f.add_member(Role::Admin);
        let (a, b) = (Fixture::profile(), Fixture::profile());

        handler(&f).handle(cmd(&f, f.owner, a)).await.unwrap();
        handler(&f).handle(cmd(&f, admin, b)).await.unwrap();

        assert!(f.invitations.has(&f.conversation_id, &a));
        assert!(f.invitations.has(&f.conversation_id, &b));
    }

    #[tokio::test]
    async fn plain_member_cannot_invite() {
        let f = Fixture::private_group();
        let member = f.add_member(Role::Member);
        let invitee = Fixture::profile();

        let err = handler(&f).handle(cmd(&f, member, invitee)).await.unwrap_err();
        assert!(matches!(err, ChatError::NotAuthorized { .. }), "{err:?}");
        assert!(!f.invitations.has(&f.conversation_id, &invitee));
    }

    #[tokio::test]
    async fn outsider_inviting_into_a_private_conversation_is_concealed() {
        let f = Fixture::private_group();
        let err = handler(&f).handle(cmd(&f, Fixture::profile(), Fixture::profile())).await.unwrap_err();
        assert!(matches!(err, ChatError::ConversationConcealed { .. }), "{err:?}");
    }

    #[tokio::test]
    async fn outsider_inviting_into_a_public_conversation_is_not_a_member() {
        let f = Fixture::public_group();
        let err = handler(&f).handle(cmd(&f, Fixture::profile(), Fixture::profile())).await.unwrap_err();
        assert!(matches!(err, ChatError::NotAMember { .. }), "{err:?}");
    }

    #[tokio::test]
    async fn inviting_an_existing_member_conflicts() {
        let f = Fixture::private_group();
        let member = f.add_member(Role::Member);
        let err = handler(&f).handle(cmd(&f, f.owner, member)).await.unwrap_err();
        assert!(matches!(err, ChatError::AlreadyMember { .. }), "{err:?}");
    }
}
