use std::sync::Arc;

use cqrs::{Envelope, Query, QueryHandler};
use uuid::Uuid;

use crate::application::access::deny_non_member;
use crate::application::port::{ConversationRepository, MemberRepository};
use crate::domain::value_object::{ConversationId, ProfileId, Role};
use crate::error::ChatError;

/// Read projection of a roster entry.
#[derive(Debug, Clone)]
pub struct MemberView {
    pub profile_id:   Uuid,
    pub role:         Role,
    pub joined_at_ms: i64,
    pub last_read:    Option<Uuid>,
}

/// Lists the bounded Member-Plane roster. Only members may view the roster, so a
/// requester that is not a member is denied — as if the conversation did not
/// exist when it is private (see [`deny_non_member`]). The roster is bounded, so
/// the result is unpaginated.
pub struct ListMembersQuery {
    pub conversation_id: String,
    pub requester_id:    String,
}

impl Query for ListMembersQuery {
    type Response = Vec<MemberView>;
}

pub struct ListMembersHandler<CR, MR> {
    pub conversation_repo: Arc<CR>,
    pub member_repo:       Arc<MR>,
}

impl<CR, MR> QueryHandler<ListMembersQuery> for ListMembersHandler<CR, MR>
where
    CR: ConversationRepository,
    MR: MemberRepository,
{
    type Error = ChatError;

    async fn handle(
        &self,
        envelope: Envelope<ListMembersQuery>,
    ) -> Result<Vec<MemberView>, ChatError> {
        let q = &envelope.payload;

        let conversation_id = ConversationId::try_from(q.conversation_id.as_str())?;
        let requester_id    = ProfileId::try_from(q.requester_id.as_str())?;

        if self.member_repo.find(&conversation_id, &requester_id).await?.is_none() {
            return Err(deny_non_member(&*self.conversation_repo, &conversation_id, requester_id).await);
        }

        let members = self.member_repo.list(&conversation_id).await?;

        Ok(members
            .into_iter()
            .map(|p| MemberView {
                profile_id:   p.profile_id().as_uuid(),
                role:         p.role(),
                joined_at_ms: p.joined_at().timestamp_millis(),
                last_read:    p.last_read().map(|m| m.as_uuid()),
            })
            .collect())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::application::command::fakes::{FakeConversations, FakeMembers, Fixture};

    async fn list(f: &Fixture, requester: ProfileId) -> Result<Vec<MemberView>, ChatError> {
        let handler: ListMembersHandler<FakeConversations, FakeMembers> = ListMembersHandler {
            conversation_repo: Arc::clone(&f.conversations),
            member_repo:       Arc::clone(&f.members),
        };
        handler
            .handle(Envelope::new(Uuid::now_v7(), ListMembersQuery {
                conversation_id: f.conversation_id.as_str(),
                requester_id:    requester.as_str(),
            }))
            .await
    }

    #[tokio::test]
    async fn outsider_of_a_private_conversation_is_concealed() {
        let err = list(&Fixture::private_group(), Fixture::profile()).await.unwrap_err();
        assert!(matches!(err, ChatError::ConversationConcealed { .. }), "{err:?}");
    }

    #[tokio::test]
    async fn outsider_of_a_public_conversation_is_not_a_member() {
        let err = list(&Fixture::public_group(), Fixture::profile()).await.unwrap_err();
        assert!(matches!(err, ChatError::NotAMember { .. }), "{err:?}");
    }

    #[tokio::test]
    async fn member_lists_the_roster() {
        let f = Fixture::private_group();
        let roster = list(&f, f.owner).await.unwrap();
        assert_eq!(roster.len(), 1);
        assert_eq!(roster[0].profile_id, f.owner.as_uuid());
    }
}
