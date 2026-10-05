use std::sync::Arc;

use cqrs::{Envelope, Query, QueryHandler};

use crate::{
    application::port::CommentRepository,
    domain::{aggregate::Comment, value_object::ProfileId},
    error::CommentError,
};

/// A profile's own comments, newest first, as stored now (tombstones
/// included) — mesh only: the GDPR data export (#653) reads them for the
/// account that owns the profile. No read gate: the caller is trusted.
pub struct ListCommentsByAuthorQuery {
    pub author_id:  String,
    pub limit:      i32,
    pub page_token: Option<String>,
}

impl Query for ListCommentsByAuthorQuery {
    type Response = (Vec<Comment>, Option<String>);
}

pub struct ListCommentsByAuthorHandler<R> {
    pub repository: Arc<R>,
}

impl<R: CommentRepository> QueryHandler<ListCommentsByAuthorQuery> for ListCommentsByAuthorHandler<R> {
    type Error = CommentError;

    async fn handle(
        &self,
        envelope: Envelope<ListCommentsByAuthorQuery>,
    ) -> Result<(Vec<Comment>, Option<String>), CommentError> {
        let query = &envelope.payload;
        let author = ProfileId::try_from(query.author_id.as_str())?;
        self.repository.list_by_author(&author, query.limit, query.page_token.as_deref()).await
    }
}
