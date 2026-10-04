use std::sync::Arc;

use cqrs::{Envelope, Query, QueryHandler};

use crate::{
    application::port::{
        author_visible_to, window_start, AudienceGate, AuthorWindowStore, PostRepository, PostSummary,
    },
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
    /// The reader is cleared for mature content (not a guest, not 13–17):
    /// age-gated posts are left out otherwise.
    pub mature:     bool,
}

impl Query for ListPostsByProfileQuery {
    type Response = (Vec<PostSummary>, Option<String>);
}

pub struct ListPostsByProfileHandler<R> {
    pub repository: Arc<R>,
    pub audience:   Arc<dyn AudienceGate>,
    pub windows:    Arc<dyn AuthorWindowStore>,
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
        // A private author the reader does not follow, a block either way or a
        // hidden author: no posts at all (fail closed on a check error).
        if !author_visible_to(self.audience.as_ref(), &query.viewer, &profile_id).await? {
            return Ok((Vec::new(), None));
        }
        let (mut posts, next) = self
            .repository
            .list_by_profile(&profile_id, query.limit, query.page_token.as_deref())
            .await?;
        posts.retain(|post| query.viewer.may_see_rated(&profile_id, post.status, post.moderation, query.mature));

        // The author's post window (#664): clients other than the author see
        // posts created within it only. The list is newest first, so the first
        // older post ends it.
        if !query.viewer.sees_every_post_of(&profile_id) {
            let days = self.windows.get(&profile_id).await?;
            if let Some(start) = window_start(days, chrono::Utc::now())
                && posts.iter().any(|p| p.created_at < start)
            {
                posts.retain(|p| p.created_at >= start);
                return Ok((posts, None));
            }
        }
        Ok((posts, next))
    }
}
