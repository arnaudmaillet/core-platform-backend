use std::sync::Arc;

use cqrs::{Envelope, Query, QueryHandler};
use futures::{StreamExt, TryStreamExt};
use uuid::Uuid;

use crate::application::port::{ConversationRepository, MemberRepository, Membership};
use crate::domain::value_object::{ConversationId, ConversationKind, ProfileId};
use crate::error::ChatError;

/// The conversations a profile is a member of — mesh only: the GDPR data
/// export (#653) then reads each one's history as that member.
pub struct ListConversationsByMemberQuery {
    pub member_id: String,
    pub limit:     i32,
    /// The last conversation id of the previous page.
    pub after:     Option<String>,
}

impl Query for ListConversationsByMemberQuery {
    type Response = Vec<MemberConversation>;
}

/// A membership, with its conversation's kind: the export shows a direct
/// conversation in full (#656). `None` when the conversation row is gone.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MemberConversation {
    pub membership: Membership,
    pub kind:       Option<ConversationKind>,
}

/// Conversation reads in flight per page.
const KIND_READS: usize = 16;

pub struct ListConversationsByMemberHandler<CR, MR> {
    pub conversation_repo: Arc<CR>,
    pub member_repo:       Arc<MR>,
}

impl<CR: ConversationRepository, MR: MemberRepository> QueryHandler<ListConversationsByMemberQuery>
    for ListConversationsByMemberHandler<CR, MR>
{
    type Error = ChatError;

    async fn handle(&self, envelope: Envelope<ListConversationsByMemberQuery>) -> Result<Vec<MemberConversation>, ChatError> {
        let query = &envelope.payload;
        let member = ProfileId::try_from(query.member_id.as_str())?;
        let after = query
            .after
            .as_deref()
            .map(|a| {
                Uuid::parse_str(a).map(ConversationId::from_uuid).map_err(|_| ChatError::DomainViolation {
                    field: "page_token".into(),
                    message: "not a conversation id".into(),
                })
            })
            .transpose()?;
        let memberships = self.member_repo.list_by_member(&member, query.limit, after.as_ref()).await?;
        futures::stream::iter(memberships)
            .map(|membership| async move {
                let kind = self.conversation_repo.find(&membership.conversation_id).await?.map(|c| c.kind());
                Ok::<_, ChatError>(MemberConversation { membership, kind })
            })
            .buffered(KIND_READS)
            .try_collect()
            .await
    }
}
