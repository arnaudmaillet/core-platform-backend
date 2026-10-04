use std::sync::Arc;

use cqrs::{Envelope, Query, QueryHandler};

use crate::{
    application::port::{author_visible_to, AudienceGate, AuthorLocationStore, PostRepository},
    domain::{aggregate::Post, value_object::{PostId, Viewer}},
    error::PostError,
};

pub struct GetPostQuery {
    pub post_id: String,
    /// Who is reading. A post the viewer may not see is reported as not found,
    /// so a draft's existence does not leak.
    pub viewer:  Viewer,
}

impl Query for GetPostQuery {
    type Response = Post;
}

pub struct GetPostHandler<R> {
    pub repository: Arc<R>,
    pub audience:   Arc<dyn AudienceGate>,
    pub locations:  Arc<dyn AuthorLocationStore>,
}

impl<R: PostRepository> QueryHandler<GetPostQuery> for GetPostHandler<R> {
    type Error = PostError;

    async fn handle(&self, envelope: Envelope<GetPostQuery>) -> Result<Post, PostError> {
        let query     = &envelope.payload;
        let post_id   = PostId::try_from(query.post_id.as_str())?;
        let not_found = || PostError::PostNotFound { post_id: post_id.as_str() };

        // The post's own state first (no network hop), then its author's
        // audience: private, blocked or hidden authors (fail closed).
        let mut post = self.repository.find_by_id(&post_id).await?
            .filter(|post| post.is_visible_to(&query.viewer))
            .ok_or_else(not_found)?;
        if !author_visible_to(self.audience.as_ref(), &query.viewer, post.profile_id()).await? {
            return Err(not_found());
        }
        // The author's location sharing applies to everyone else, the mesh
        // included (fail closed: a store error fails the read).
        if let Some(point) = post.location().filter(|_| !query.viewer.is_author(post.profile_id())) {
            let sharing = self.locations.get(post.profile_id()).await?;
            post.show_location(sharing.shown(point));
        }
        Ok(post)
    }
}
