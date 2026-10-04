use std::sync::Arc;

use cqrs::{Envelope, Query, QueryHandler};

use crate::{
    application::port::{PostRepository, PostSummary},
    domain::value_object::{ProfileId, Viewer},
    error::PostError,
};

pub struct ListPostsByProfileQuery {
    pub profile_id: String,
    pub limit:      i32,
    pub page_token: Option<String>,
    /// Who is reading. Anyone but the author (or a trusted internal caller) gets
    /// published posts that moderation has not removed.
    pub viewer:     Viewer,
}

impl Query for ListPostsByProfileQuery {
    type Response = (Vec<PostSummary>, Option<String>);
}

pub struct ListPostsByProfileHandler<R> {
    pub repository: Arc<R>,
}

impl<R: PostRepository> QueryHandler<ListPostsByProfileQuery> for ListPostsByProfileHandler<R> {
    type Error = PostError;

    /// The filter runs on the page the store returned, so a page can come back
    /// shorter than `limit` (even empty) while `next_token` still points past it.
    /// Callers page until the token is empty, as they already must.
    async fn handle(
        &self,
        envelope: Envelope<ListPostsByProfileQuery>,
    ) -> Result<(Vec<PostSummary>, Option<String>), PostError> {
        let query      = &envelope.payload;
        let profile_id = ProfileId::try_from(query.profile_id.as_str())?;
        let (mut posts, next) = self
            .repository
            .list_by_profile(&profile_id, query.limit, query.page_token.as_deref())
            .await?;
        posts.retain(|post| query.viewer.may_see(&profile_id, post.status, post.moderation));
        Ok((posts, next))
    }
}
