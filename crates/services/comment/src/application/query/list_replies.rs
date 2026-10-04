use std::sync::Arc;

use cqrs::{Envelope, Query, QueryHandler};

use crate::{
    application::port::{filter_page, CommentRepository, CommentSummary, ReadGate},
    domain::value_object::{CommentId, PostId, Viewer},
    error::CommentError,
};

pub struct ListRepliesQuery {
    pub post_id:    String,
    pub comment_id: String,
    pub limit:      i32,
    pub page_token: Option<String>,
    /// Who is reading (see [`ReadGate`]).
    pub viewer:     Viewer,
}

impl Query for ListRepliesQuery {
    type Response = (Vec<CommentSummary>, Option<String>);
}

pub struct ListRepliesHandler<R> {
    pub repository: Arc<R>,
    pub gate:       Arc<dyn ReadGate>,
}

impl<R: CommentRepository> QueryHandler<ListRepliesQuery> for ListRepliesHandler<R> {
    type Error = CommentError;

    async fn handle(
        &self,
        envelope: Envelope<ListRepliesQuery>,
    ) -> Result<(Vec<CommentSummary>, Option<String>), CommentError> {
        let q          = &envelope.payload;
        let post_id    = PostId::try_from(q.post_id.as_str())?;
        let comment_id = CommentId::try_from(q.comment_id.as_str())?;
        let page = self
            .repository
            .list_replies(&post_id, &comment_id, q.limit, q.page_token.as_deref())
            .await?;
        filter_page(self.gate.as_ref(), &q.viewer, &post_id, page).await
    }
}
