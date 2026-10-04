use std::sync::Arc;

use cqrs::{Envelope, Query, QueryHandler};

use crate::application::port::SocialGraphRepository;
use crate::domain::access::{ContentAccess, Viewer};
use crate::domain::entity::FollowEdge;
use crate::domain::value_object::ProfileId;
use crate::error::SocialGraphError;

#[derive(Debug, Clone)]
pub struct ListFollowingQuery {
    pub follower_id: String,
    pub limit:       u32,
    pub page_token:  Option<String>,
    /// Who is reading. A private profile's lists are for its followers; a block
    /// either way or a hidden profile hides them (see `AccessFacts::access`).
    pub viewer:      Viewer,
}

impl Query for ListFollowingQuery {
    type Response = (Vec<FollowEdge>, Option<String>);
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
    ) -> Result<(Vec<FollowEdge>, Option<String>), Self::Error> {
        let q = &envelope.payload;

        let follower_id = ProfileId::try_from(q.follower_id.as_str())?;
        let limit       = q.limit.clamp(1, 100) as i32;

        if let Viewer::Profiles(viewers) = &q.viewer
            && !q.viewer.sees_everything_of(&follower_id)
        {
            let facts = self.repo.load_access_facts(viewers, &[follower_id]).await?;
            if facts.access(viewers, &follower_id) != ContentAccess::Visible {
                return Ok((Vec::new(), None));
            }
        }

        self.repo.list_following(&follower_id, limit, q.page_token.as_deref()).await
    }
}
