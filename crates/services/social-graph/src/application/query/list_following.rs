use std::sync::Arc;

use cqrs::{Envelope, Query, QueryHandler};

use crate::application::port::SocialGraphRepository;
use crate::application::query::list_gate::may_read_list;
use crate::application::query::FollowListPage;
use crate::domain::access::Viewer;
use crate::domain::list_privacy::FollowList;
use crate::domain::value_object::ProfileId;
use crate::error::SocialGraphError;

#[derive(Debug, Clone)]
pub struct ListFollowingQuery {
    pub follower_id: String,
    pub limit:       u32,
    pub page_token:  Option<String>,
    /// Who is reading. A private profile's lists are for its followers; a block
    /// either way or a hidden profile hides them (see `AccessFacts::access`),
    /// and so does the owner's list privacy (`list_gate`).
    pub viewer:      Viewer,
}

impl Query for ListFollowingQuery {
    type Response = FollowListPage;
}

pub struct ListFollowingHandler {
    repo: Arc<dyn SocialGraphRepository>,
}

impl ListFollowingHandler {
    pub fn new(repo: Arc<dyn SocialGraphRepository>) -> Self {
        Self { repo }
    }
}

impl QueryHandler<ListFollowingQuery> for ListFollowingHandler {
    type Error = SocialGraphError;

    async fn handle(
        &self,
        envelope: Envelope<ListFollowingQuery>,
    ) -> Result<FollowListPage, Self::Error> {
        let q = &envelope.payload;

        let follower_id = ProfileId::try_from(q.follower_id.as_str())?;
        let limit       = q.limit.clamp(1, 100) as i32;

        if !may_read_list(self.repo.as_ref(), &q.viewer, &follower_id, FollowList::Following).await? {
            return Ok(FollowListPage::hidden());
        }

        let (edges, next_page_token) =
            self.repo.list_following(&follower_id, limit, q.page_token.as_deref()).await?;
        Ok(FollowListPage { edges, next_page_token, hidden: false })
    }
}
