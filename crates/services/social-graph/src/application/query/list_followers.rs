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
pub struct ListFollowersQuery {
    pub followee_id: String,
    pub limit:       u32,
    pub page_token:  Option<String>,
    /// Who is reading. A private profile's lists are for its followers; a block
    /// either way or a hidden profile hides them (see `AccessFacts::access`),
    /// and so does the owner's list privacy (`list_gate`).
    pub viewer:      Viewer,
}

impl Query for ListFollowersQuery {
    type Response = FollowListPage;
}

pub struct ListFollowersHandler {
    repo: Arc<dyn SocialGraphRepository>,
}

impl ListFollowersHandler {
    pub fn new(repo: Arc<dyn SocialGraphRepository>) -> Self {
        Self { repo }
    }
}

impl QueryHandler<ListFollowersQuery> for ListFollowersHandler {
    type Error = SocialGraphError;

    async fn handle(
        &self,
        envelope: Envelope<ListFollowersQuery>,
    ) -> Result<FollowListPage, Self::Error> {
        let q = &envelope.payload;

        let followee_id = ProfileId::try_from(q.followee_id.as_str())?;
        let limit       = q.limit.clamp(1, 100) as i32;

        if !may_read_list(self.repo.as_ref(), &q.viewer, &followee_id, FollowList::Followers).await? {
            return Ok(FollowListPage::hidden());
        }

        let (edges, next_page_token) =
            self.repo.list_followers(&followee_id, limit, q.page_token.as_deref()).await?;
        Ok(FollowListPage { edges, next_page_token, hidden: false })
    }
}
