use std::sync::Arc;

use cqrs::{Envelope, Query, QueryHandler};

use crate::{
    application::port::PostRepository,
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
}

impl<R: PostRepository> QueryHandler<GetPostQuery> for GetPostHandler<R> {
    type Error = PostError;

    async fn handle(&self, envelope: Envelope<GetPostQuery>) -> Result<Post, PostError> {
        let query   = &envelope.payload;
        let post_id = PostId::try_from(query.post_id.as_str())?;
        self.repository.find_by_id(&post_id).await?
            .filter(|post| post.is_visible_to(&query.viewer))
            .ok_or_else(|| PostError::PostNotFound { post_id: post_id.as_str() })
    }
}
