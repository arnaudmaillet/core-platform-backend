use std::sync::Arc;

use cqrs::{Envelope, Query, QueryHandler};
use uuid::Uuid;

use crate::application::port::{MemberRepository, Membership};
use crate::domain::value_object::{ConversationId, ProfileId};
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
    type Response = Vec<Membership>;
}

pub struct ListConversationsByMemberHandler<MR> {
    pub member_repo: Arc<MR>,
}

impl<MR: MemberRepository> QueryHandler<ListConversationsByMemberQuery> for ListConversationsByMemberHandler<MR> {
    type Error = ChatError;

    async fn handle(&self, envelope: Envelope<ListConversationsByMemberQuery>) -> Result<Vec<Membership>, ChatError> {
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
        self.member_repo.list_by_member(&member, query.limit, after.as_ref()).await
    }
}
