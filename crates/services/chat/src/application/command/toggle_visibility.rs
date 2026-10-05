use std::sync::Arc;

use cqrs::{Command, CommandHandler, Envelope};
use validate_core::{FieldViolation, Validate};

use crate::application::port::{ConversationRepository, EventPublisher, MemberRepository};
use crate::domain::value_object::{ConversationId, ProfileId};
use crate::error::ChatError;

/// Toggles a conversation's visibility (the `Private` <-> `Public` switch).
///
/// `make_public = true` attaches the Audience Plane and stamps the public-since
/// watermark; `false` detaches it. Authorization requires an administering role
/// (owner/admin); the aggregate enforces the monotone transition guard. A
/// non-member actor gets [`ChatError::ConversationConcealed`] on a private
/// conversation and [`ChatError::NotAMember`] on a public one.
pub struct ToggleVisibilityCommand {
    pub conversation_id: String,
    pub actor_id:        String,
    pub make_public:     bool,
}

impl Command for ToggleVisibilityCommand {}

impl Validate for ToggleVisibilityCommand {
    fn validate(&self) -> Result<(), Vec<FieldViolation>> {
        let mut v = Vec::new();
        if self.conversation_id.trim().is_empty() {
            v.push(FieldViolation::new(
                "conversation_id",
                "CHT-VAL-020",
                "conversation_id must not be empty",
            ));
        }
        if self.actor_id.trim().is_empty() {
            v.push(FieldViolation::new("actor_id", "CHT-VAL-021", "actor_id must not be empty"));
        }
        if v.is_empty() { Ok(()) } else { Err(v) }
    }
}

pub struct ToggleVisibilityHandler<CR, MR, EP> {
    pub conversation_repo: Arc<CR>,
    pub member_repo:       Arc<MR>,
    pub publisher:         Arc<EP>,
}

impl<CR, MR, EP> CommandHandler<ToggleVisibilityCommand>
    for ToggleVisibilityHandler<CR, MR, EP>
where
    CR: ConversationRepository,
    MR: MemberRepository,
    EP: EventPublisher,
{
    type Error = ChatError;

    async fn handle(
        &self,
        envelope: Envelope<ToggleVisibilityCommand>,
    ) -> Result<(), ChatError> {
        let cmd = &envelope.payload;

        let conversation_id = ConversationId::try_from(cmd.conversation_id.as_str())?;
        let actor_id        = ProfileId::try_from(cmd.actor_id.as_str())?;

        let mut conversation = self
            .conversation_repo
            .find(&conversation_id)
            .await?
            .ok_or_else(|| ChatError::ConversationNotFound {
                conversation_id: conversation_id.as_str(),
            })?;

        // Authorization: the actor must be an administering member. A
        // non-member of a private conversation must not learn that it exists.
        let Some(actor) = self.member_repo.find(&conversation_id, &actor_id).await? else {
            return Err(conversation.deny_outsider(actor_id));
        };

        if !actor.can_administer() {
            return Err(ChatError::NotAuthorized {
                profile_id:      actor_id.as_str(),
                conversation_id: conversation_id.as_str(),
            });
        }

        if cmd.make_public {
            conversation.publish()?;
        } else {
            conversation.unpublish()?;
        }

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
    use crate::application::command::fakes::{
        FakeConversations, FakeMembers, FakePublisher, Fixture,
    };
    use crate::domain::value_object::Role;

    async fn toggle(f: &Fixture, actor: ProfileId, make_public: bool) -> Result<(), ChatError> {
        let handler: ToggleVisibilityHandler<FakeConversations, FakeMembers, FakePublisher> =
            ToggleVisibilityHandler {
                conversation_repo: Arc::clone(&f.conversations),
                member_repo:       Arc::clone(&f.members),
                publisher:         Arc::clone(&f.publisher),
            };
        handler
            .handle(Envelope::new(uuid::Uuid::now_v7(), ToggleVisibilityCommand {
                conversation_id: f.conversation_id.as_str(),
                actor_id:        actor.as_str(),
                make_public,
            }))
            .await
    }

    #[tokio::test]
    async fn outsider_toggling_a_private_conversation_is_concealed() {
        let f = Fixture::private_group();
        let err = toggle(&f, Fixture::profile(), true).await.unwrap_err();
        assert!(matches!(err, ChatError::ConversationConcealed { .. }), "{err:?}");
        assert_eq!(f.publisher.count(), 0);
    }

    #[tokio::test]
    async fn outsider_toggling_a_public_conversation_is_not_a_member() {
        let f = Fixture::public_group();
        let err = toggle(&f, Fixture::profile(), false).await.unwrap_err();
        assert!(matches!(err, ChatError::NotAMember { .. }), "{err:?}");
    }

    #[tokio::test]
    async fn plain_member_is_not_authorized_and_owner_toggles() {
        let f = Fixture::private_group();
        let member = f.add_member(Role::Member);
        let err = toggle(&f, member, true).await.unwrap_err();
        assert!(matches!(err, ChatError::NotAuthorized { .. }), "{err:?}");

        toggle(&f, f.owner, true).await.unwrap();
        assert_eq!(f.publisher.count(), 1, "ConversationPublished");
    }
}
