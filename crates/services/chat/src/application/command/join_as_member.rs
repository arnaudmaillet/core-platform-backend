use std::sync::Arc;

use cqrs::{Command, CommandHandler, Envelope};
use validate_core::{FieldViolation, Validate};

use crate::application::port::{
    ConversationRepository, EventPublisher, InvitationRepository, MemberRepository,
};
use crate::domain::aggregate::Participant;
use crate::domain::value_object::{ConversationId, ProfileId, Role};
use crate::error::ChatError;

/// Admits a profile to the bounded Member Plane as a regular `Member`.
///
/// A public conversation is open-join; a private one requires a pending
/// invitation for the joiner (see [`InviteMemberCommand`](super::InviteMemberCommand)),
/// which the join consumes. A non-invited joiner of a private conversation gets
/// [`ChatError::ConversationConcealed`] — rendered at the edge exactly like a
/// missing conversation. The roster cap is enforced through the aggregate and a
/// profile that is already a member is rejected. Promotion to admin/owner is a
/// separate operation.
pub struct JoinAsMemberCommand {
    pub conversation_id: String,
    pub profile_id:      String,
}

impl Command for JoinAsMemberCommand {}

impl Validate for JoinAsMemberCommand {
    fn validate(&self) -> Result<(), Vec<FieldViolation>> {
        let mut v = Vec::new();
        if self.conversation_id.trim().is_empty() {
            v.push(FieldViolation::new(
                "conversation_id",
                "CHT-VAL-030",
                "conversation_id must not be empty",
            ));
        }
        if self.profile_id.trim().is_empty() {
            v.push(FieldViolation::new("profile_id", "CHT-VAL-031", "profile_id must not be empty"));
        }
        if v.is_empty() { Ok(()) } else { Err(v) }
    }
}

pub struct JoinAsMemberHandler<CR, MR, IR, EP> {
    pub conversation_repo: Arc<CR>,
    pub member_repo:       Arc<MR>,
    pub invitation_repo:   Arc<IR>,
    pub publisher:         Arc<EP>,
}

impl<CR, MR, IR, EP> CommandHandler<JoinAsMemberCommand> for JoinAsMemberHandler<CR, MR, IR, EP>
where
    CR: ConversationRepository,
    MR: MemberRepository,
    IR: InvitationRepository,
    EP: EventPublisher,
{
    type Error = ChatError;

    async fn handle(&self, envelope: Envelope<JoinAsMemberCommand>) -> Result<(), ChatError> {
        let cmd = &envelope.payload;

        let conversation_id = ConversationId::try_from(cmd.conversation_id.as_str())?;
        let profile_id      = ProfileId::try_from(cmd.profile_id.as_str())?;

        let mut conversation = self
            .conversation_repo
            .find(&conversation_id)
            .await?
            .ok_or_else(|| ChatError::ConversationNotFound {
                conversation_id: conversation_id.as_str(),
            })?;

        // Only a member can get here for a private conversation it already sees,
        // so this answer leaks nothing to an outsider.
        if self.member_repo.find(&conversation_id, &profile_id).await?.is_some() {
            return Err(ChatError::AlreadyMember {
                profile_id:      profile_id.as_str(),
                conversation_id: conversation_id.as_str(),
            });
        }

        // Access rule (private ⇒ invited) and roster cap, inside the aggregate.
        let invitation = self.invitation_repo.find(&conversation_id, &profile_id).await?;
        conversation.admit_joiner(profile_id, invitation.as_ref())?;
        let participant = Participant::new(profile_id, Role::Member)?;

        self.member_repo.insert(&conversation_id, &participant).await?;
        self.conversation_repo.update(&conversation).await?;

        // Consume the invitation only once the member row is durable. Best
        // effort: the join already stands (a retry answers AlreadyMember), so a
        // failed delete must not abort before MemberJoined is published — the
        // leftover row ages out with the table TTL.
        if invitation.is_some()
            && let Err(e) = self.invitation_repo.delete(&conversation_id, &profile_id).await
        {
            tracing::warn!(
                conversation_id = %conversation_id.as_str(),
                profile_id      = %profile_id.as_str(),
                error           = %e,
                "invitation not consumed after join; it will expire with the table TTL",
            );
        }

        for event in conversation.take_events() {
            self.publisher.publish_conversation(&event).await?;
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use error::AppError as _;

    use super::*;
    use crate::application::command::fakes::{
        FakeConversations, FakeInvitations, FakeMembers, FakePublisher, Fixture,
    };

    fn handler(f: &Fixture) -> JoinAsMemberHandler<FakeConversations, FakeMembers, FakeInvitations, FakePublisher> {
        JoinAsMemberHandler {
            conversation_repo: Arc::clone(&f.conversations),
            member_repo:       Arc::clone(&f.members),
            invitation_repo:   Arc::clone(&f.invitations),
            publisher:         Arc::clone(&f.publisher),
        }
    }

    fn join(conversation_id: &ConversationId, profile: ProfileId) -> Envelope<JoinAsMemberCommand> {
        Envelope::new(uuid::Uuid::now_v7(), JoinAsMemberCommand {
            conversation_id: conversation_id.as_str(),
            profile_id:      profile.as_str(),
        })
    }

    #[tokio::test]
    async fn non_invited_join_of_a_private_group_is_refused_and_concealed() {
        let f = Fixture::private_group();
        let outsider = Fixture::profile();

        let err = handler(&f).handle(join(&f.conversation_id, outsider)).await.unwrap_err();

        assert!(matches!(err, ChatError::ConversationConcealed { .. }), "{err:?}");
        assert!(!f.members.has(&f.conversation_id, &outsider), "no roster row may be written");
        assert_eq!(f.publisher.count(), 0, "no MemberJoined may be published");
        assert_eq!(f.conversations.member_count(&f.conversation_id), 1);
    }

    #[tokio::test]
    async fn refusal_is_indistinguishable_from_a_missing_conversation() {
        let f = Fixture::private_group();
        let concealed = handler(&f).handle(join(&f.conversation_id, Fixture::profile())).await.unwrap_err();

        let missing_id = ConversationId::new();
        let missing = handler(&f).handle(join(&missing_id, Fixture::profile())).await.unwrap_err();
        assert!(matches!(missing, ChatError::ConversationNotFound { .. }), "{missing:?}");

        assert_eq!(concealed.http_status(), missing.http_status());
        assert_eq!(concealed.user_facing_message(), missing.user_facing_message());
        // Same message shape: only the (unguessable) id differs.
        assert_eq!(
            concealed.to_string().replace(&f.conversation_id.as_str(), "<id>"),
            missing.to_string().replace(&missing_id.as_str(), "<id>"),
        );
    }

    #[tokio::test]
    async fn invited_profile_joins_a_private_group_and_consumes_the_invitation() {
        let f = Fixture::private_group();
        let invitee = Fixture::profile();
        f.invite(invitee);

        handler(&f).handle(join(&f.conversation_id, invitee)).await.unwrap();

        assert!(f.members.has(&f.conversation_id, &invitee));
        assert!(!f.invitations.has(&f.conversation_id, &invitee), "invitation must be consumed");
        assert_eq!(f.conversations.member_count(&f.conversation_id), 2);
        assert_eq!(f.publisher.count(), 1);
    }

    #[tokio::test]
    async fn an_invitation_is_single_use() {
        let f = Fixture::private_group();
        let invitee = Fixture::profile();
        f.invite(invitee);
        handler(&f).handle(join(&f.conversation_id, invitee)).await.unwrap();

        // Leave, then try to come back on the consumed invitation.
        f.members.remove(&f.conversation_id, &invitee);
        let err = handler(&f).handle(join(&f.conversation_id, invitee)).await.unwrap_err();
        assert!(matches!(err, ChatError::ConversationConcealed { .. }), "{err:?}");
    }

    #[tokio::test]
    async fn a_failed_invitation_delete_still_publishes_member_joined() {
        let f = Fixture::private_group();
        let invitee = Fixture::profile();
        f.invite(invitee);
        f.invitations.fail_deletes();

        handler(&f).handle(join(&f.conversation_id, invitee)).await.unwrap();

        assert!(f.members.has(&f.conversation_id, &invitee));
        assert_eq!(f.publisher.count(), 1, "MemberJoined must not be lost to a failed delete");
    }

    #[tokio::test]
    async fn public_group_join_stays_open() {
        let f = Fixture::public_group();
        let joiner = Fixture::profile();

        handler(&f).handle(join(&f.conversation_id, joiner)).await.unwrap();

        assert!(f.members.has(&f.conversation_id, &joiner));
        assert_eq!(f.publisher.count(), 1);
    }

    #[tokio::test]
    async fn rejoining_as_a_member_conflicts() {
        let f = Fixture::private_group();
        let err = handler(&f).handle(join(&f.conversation_id, f.owner)).await.unwrap_err();
        assert!(matches!(err, ChatError::AlreadyMember { .. }), "{err:?}");
    }
}
