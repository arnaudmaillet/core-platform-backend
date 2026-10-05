use std::sync::Arc;

use cqrs::{Command, CommandHandler, Envelope};
use validate_core::{FieldViolation, Validate};

use crate::application::access::deny_non_member;
use crate::application::port::{ConversationRepository, MemberRepository};
use crate::domain::value_object::{ConversationId, MessageId, ProfileId};
use crate::error::ChatError;

/// Advances a member's read-receipt horizon to `message_id`.
///
/// Read-receipts are a Member-Plane-only concept (O(members)); a non-member
/// (audience) cannot mark read — and, on a private conversation, is answered as
/// if it did not exist (see [`deny_non_member`]). The horizon is monotone — a stale acknowledgement
/// is a no-op via [`Participant::mark_read`](crate::domain::aggregate::Participant::mark_read).
pub struct MarkReadCommand {
    pub conversation_id: String,
    pub member_id:       String,
    pub message_id:      String,
}

impl Command for MarkReadCommand {}

impl Validate for MarkReadCommand {
    fn validate(&self) -> Result<(), Vec<FieldViolation>> {
        let mut v = Vec::new();
        if self.conversation_id.trim().is_empty() {
            v.push(FieldViolation::new(
                "conversation_id",
                "CHT-VAL-050",
                "conversation_id must not be empty",
            ));
        }
        if self.member_id.trim().is_empty() {
            v.push(FieldViolation::new("member_id", "CHT-VAL-051", "member_id must not be empty"));
        }
        if self.message_id.trim().is_empty() {
            v.push(FieldViolation::new("message_id", "CHT-VAL-052", "message_id must not be empty"));
        }
        if v.is_empty() { Ok(()) } else { Err(v) }
    }
}

pub struct MarkReadHandler<CR, MR> {
    pub conversation_repo: Arc<CR>,
    pub member_repo:       Arc<MR>,
}

impl<CR, MR> CommandHandler<MarkReadCommand> for MarkReadHandler<CR, MR>
where
    CR: ConversationRepository,
    MR: MemberRepository,
{
    type Error = ChatError;

    async fn handle(&self, envelope: Envelope<MarkReadCommand>) -> Result<(), ChatError> {
        let cmd = &envelope.payload;

        let conversation_id = ConversationId::try_from(cmd.conversation_id.as_str())?;
        let member_id       = ProfileId::try_from(cmd.member_id.as_str())?;
        let message_id      = MessageId::try_from(cmd.message_id.as_str())?;

        let Some(mut member) = self.member_repo.find(&conversation_id, &member_id).await? else {
            return Err(deny_non_member(&*self.conversation_repo, &conversation_id, member_id).await);
        };

        // Monotone advance; persist the resulting (possibly unchanged) horizon.
        member.mark_read(message_id);
        if let Some(horizon) = member.last_read() {
            self.member_repo
                .update_last_read(&conversation_id, &member_id, horizon)
                .await?;
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::application::command::fakes::{FakeConversations, FakeMembers, Fixture};

    async fn mark_read(f: &Fixture, member: ProfileId) -> Result<(), ChatError> {
        let handler: MarkReadHandler<FakeConversations, FakeMembers> = MarkReadHandler {
            conversation_repo: Arc::clone(&f.conversations),
            member_repo:       Arc::clone(&f.members),
        };
        handler
            .handle(Envelope::new(uuid::Uuid::now_v7(), MarkReadCommand {
                conversation_id: f.conversation_id.as_str(),
                member_id:       member.as_str(),
                message_id:      MessageId::new().as_str(),
            }))
            .await
    }

    #[tokio::test]
    async fn outsider_of_a_private_conversation_is_concealed() {
        let err = mark_read(&Fixture::private_group(), Fixture::profile()).await.unwrap_err();
        assert!(matches!(err, ChatError::ConversationConcealed { .. }), "{err:?}");
    }

    #[tokio::test]
    async fn outsider_of_a_public_conversation_is_not_a_member() {
        let err = mark_read(&Fixture::public_group(), Fixture::profile()).await.unwrap_err();
        assert!(matches!(err, ChatError::NotAMember { .. }), "{err:?}");
    }

    #[tokio::test]
    async fn member_marks_read() {
        let f = Fixture::private_group();
        mark_read(&f, f.owner).await.unwrap();
    }
}
