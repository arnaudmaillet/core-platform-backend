use std::sync::Arc;

use cqrs::{Envelope, Query, QueryHandler};

use crate::{
    application::port::{CommentRepository, ReadGate},
    domain::{aggregate::Comment, value_object::{CommentId, Viewer}},
    error::CommentError,
};

pub struct GetCommentQuery {
    pub comment_id: String,
    /// Who is reading (see [`ReadGate`]).
    pub viewer:     Viewer,
}

impl Query for GetCommentQuery {
    type Response = Comment;
}

pub struct GetCommentHandler<R> {
    pub repository: Arc<R>,
    pub gate:       Arc<dyn ReadGate>,
}

impl<R: CommentRepository> QueryHandler<GetCommentQuery> for GetCommentHandler<R> {
    type Error = CommentError;

    async fn handle(&self, envelope: Envelope<GetCommentQuery>) -> Result<Comment, CommentError> {
        let query      = &envelope.payload;
        let comment_id = CommentId::try_from(query.comment_id.as_str())?;
        let not_found  = || CommentError::CommentNotFound { comment_id: comment_id.as_str() };
        let comment = self.repository.find_by_id(&comment_id).await?.ok_or_else(not_found)?;
        if query.viewer == Viewer::Internal {
            return Ok(comment);
        }
        // The post must be readable, and the comment's author not hidden.
        let author = comment.author_id().clone();
        match self.gate.check(&query.viewer, comment.post_id(), std::slice::from_ref(&author)).await? {
            Some(hidden) if !hidden.contains(&author) => Ok(comment),
            _ => Err(not_found()),
        }
    }
}
