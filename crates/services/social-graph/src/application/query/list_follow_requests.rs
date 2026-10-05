use std::sync::Arc;

use cqrs::{Envelope, Query, QueryHandler};

use crate::application::port::SocialGraphRepository;
use crate::domain::entity::FollowEdge;
use crate::domain::value_object::ProfileId;
use crate::error::SocialGraphError;

/// A private profile's pending follow requests, newest first. Owner-only: the
/// gRPC handler binds `owner_id` to the caller's profiles on the edge.
#[derive(Debug, Clone)]
pub struct ListFollowRequestsQuery {
    pub owner_id:   String,
    pub limit:      u32,
    pub page_token: Option<String>,
}

impl Query for ListFollowRequestsQuery {
    type Response = FollowRequestsPage;
}

/// A page of pending requests.
#[derive(Debug, Clone)]
pub struct FollowRequestsPage {
    pub requests: Vec<FollowEdge>,
    pub next_page_token: Option<String>,
    /// How many are pending in all — on the first page only (#755: the
    /// privacy screen shows the number without reading every page).
    pub pending: Option<u64>,
}

pub struct ListFollowRequestsHandler {
    repo: Arc<dyn SocialGraphRepository>,
}

impl ListFollowRequestsHandler {
    pub fn new(repo: Arc<dyn SocialGraphRepository>) -> Self {
        Self { repo }
    }
}

impl QueryHandler<ListFollowRequestsQuery> for ListFollowRequestsHandler {
    type Error = SocialGraphError;

    async fn handle(
        &self,
        envelope: Envelope<ListFollowRequestsQuery>,
    ) -> Result<FollowRequestsPage, Self::Error> {
        let q = &envelope.payload;
        let owner_id = ProfileId::try_from(q.owner_id.as_str())?;
        let limit = q.limit.clamp(1, 100) as i32;
        let (requests, next_page_token) =
            self.repo.list_follow_requests(&owner_id, limit, q.page_token.as_deref()).await?;
        let pending = match q.page_token {
            None => Some(self.repo.count_follow_requests(&owner_id).await?),
            Some(_) => None,
        };
        Ok(FollowRequestsPage { requests, next_page_token, pending })
    }
}
