use std::sync::Arc;

use cqrs::{Envelope, Query, QueryHandler};

use crate::{
    application::port::{CommentRepository, OwnerFilters, ReadGate},
    domain::{aggregate::Comment, value_object::{CommentId, Viewer}},
    error::CommentError,
};

pub struct GetCommentQuery {
    pub comment_id: String,
    /// Who is reading (see [`ReadGate`]).
    pub viewer:     Viewer,
    /// The reader is cleared for mature content (not a guest, not 13–17):
    /// the comments of an age-gated post are hidden otherwise.
    pub mature:     bool,
}

impl Query for GetCommentQuery {
    type Response = Comment;
}

pub struct GetCommentHandler<R> {
    pub repository: Arc<R>,
    pub gate:       Arc<dyn ReadGate>,
    pub filters:    OwnerFilters,
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
        // The post must be readable, the comment's author not hidden, and the
        // comment not hidden by the post owner's filter.
        let author = comment.author_id().clone();
        let Some(decision) = self.gate.check(&query.viewer, query.mature, comment.post_id(), std::slice::from_ref(&author)).await? else {
            return Err(not_found());
        };
        let filter = self.filters.of(decision.post_author.as_ref()).await?;
        let body = comment.body().map(|b| b.as_str().to_owned());
        if self.filters.shows(&query.viewer, &decision, filter.as_ref(), &author, body.as_deref()) {
            Ok(comment)
        } else {
            Err(not_found())
        }
    }
}
