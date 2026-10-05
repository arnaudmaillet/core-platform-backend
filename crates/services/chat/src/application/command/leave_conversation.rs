use std::sync::Arc;

use chrono::Utc;
use cqrs::{Command, CommandHandler, Envelope};
use validate_core::{FieldViolation, Validate};

use crate::application::port::{ConversationRepository, EventPublisher, MemberRepository};
use crate::domain::value_object::{ConversationId, ProfileId};
use crate::error::ChatError;

/// A member leaves a group or channel (#656). Off the roster, its membership
/// kept with `left_at` (the GDPR export still finds it), out of its inbox
/// (`MemberLeft`). The owner may not leave — a conversation always has one —
/// nor may anyone leave a direct conversation. An outsider of a private
/// conversation is answered as if it did not exist.
pub struct LeaveConversationCommand {
    pub conversation_id: String,
    pub profile_id:      String,
}

impl Command for LeaveConversationCommand {}

impl Validate for LeaveConversationCommand {
    fn validate(&self) -> Result<(), Vec<FieldViolation>> {
        let mut v = Vec::new();
        if self.conversation_id.trim().is_empty() {
            v.push(FieldViolation::new("conversation_id", "CHT-VAL-070", "conversation_id must not be empty"));
        }
        if self.profile_id.trim().is_empty() {
            v.push(FieldViolation::new("profile_id", "CHT-VAL-071", "profile_id must not be empty"));
        }
        if v.is_empty() { Ok(()) } else { Err(v) }
    }
}

pub struct LeaveConversationHandler<CR, MR, EP> {
    pub conversation_repo: Arc<CR>,
    pub member_repo:       Arc<MR>,
    pub publisher:         Arc<EP>,
}

impl<CR, MR, EP> CommandHandler<LeaveConversationCommand> for LeaveConversationHandler<CR, MR, EP>
where
    CR: ConversationRepository,
    MR: MemberRepository,
    EP: EventPublisher,
{
    type Error = ChatError;

    async fn handle(&self, envelope: Envelope<LeaveConversationCommand>) -> Result<(), ChatError> {
        let cmd = &envelope.payload;
        let conversation_id = ConversationId::try_from(cmd.conversation_id.as_str())?;
        let profile_id = ProfileId::try_from(cmd.profile_id.as_str())?;

        let mut conversation = self
            .conversation_repo
            .find(&conversation_id)
            .await?
            .ok_or_else(|| ChatError::ConversationNotFound { conversation_id: conversation_id.as_str() })?;
        let Some(participant) = self.member_repo.find(&conversation_id, &profile_id).await? else {
            return Err(conversation.deny_outsider(profile_id));
        };

        conversation.release_member(profile_id)?;
        self.member_repo.leave(&conversation_id, &participant, Utc::now()).await?;
        self.conversation_repo.update(&conversation).await?;
        for event in conversation.take_events() {
            self.publisher.publish_conversation(&event).await?;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::application::command::fakes::{FakeConversations, FakeMembers, FakePublisher, Fixture};
    use crate::domain::aggregate::Conversation;
    use crate::domain::value_object::Role;

    fn handler(f: &Fixture) -> LeaveConversationHandler<FakeConversations, FakeMembers, FakePublisher> {
        LeaveConversationHandler {
            conversation_repo: Arc::clone(&f.conversations),
            member_repo:       Arc::clone(&f.members),
            publisher:         Arc::clone(&f.publisher),
        }
    }

    async fn leave(f: &Fixture, conversation_id: ConversationId, who: ProfileId) -> Result<(), ChatError> {
        handler(f)
            .handle(Envelope::new(uuid::Uuid::now_v7(), LeaveConversationCommand {
                conversation_id: conversation_id.as_str(),
                profile_id:      who.as_str(),
            }))
            .await
    }

    #[tokio::test]
    async fn a_member_leaves_and_its_membership_is_kept_with_left_at() {
        let f = Fixture::private_group();
        let member = f.add_member(Role::Member);
        let before = f.publisher.count();
        leave(&f, f.conversation_id, member).await.unwrap();

        assert!(!f.members.has(&f.conversation_id, &member), "off the roster");
        let membership = f.members.find_membership(&member, &f.conversation_id).await.unwrap().unwrap();
        assert!(membership.left_at.is_some(), "kept, ended");
        assert_eq!(f.publisher.count(), before + 1, "MemberLeft");
        let err = leave(&f, f.conversation_id, member).await.unwrap_err();
        assert!(matches!(err, ChatError::ConversationConcealed { .. }), "gone: an outsider now — {err:?}");
    }

    #[tokio::test]
    async fn the_owner_stays_and_a_direct_conversation_is_never_left() {
        let f = Fixture::private_group();
        let err = leave(&f, f.conversation_id, f.owner).await.unwrap_err();
        assert!(matches!(err, ChatError::DomainViolation { .. }), "{err:?}");
        assert!(f.members.has(&f.conversation_id, &f.owner));

        let (a, b) = (Fixture::profile(), Fixture::profile());
        let direct = Conversation::open_direct(ConversationId::new(), a, b, false);
        f.conversations.insert_direct(&direct).await.unwrap();
        f.members.insert(&direct.id(), &crate::domain::aggregate::Participant::new(b, Role::Member).unwrap()).await.unwrap();
        let err = leave(&f, direct.id(), b).await.unwrap_err();
        assert!(matches!(err, ChatError::DomainViolation { .. }), "{err:?}");
    }
}
