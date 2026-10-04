use std::sync::Arc;

use cqrs::{Envelope, Query, QueryHandler};

use crate::{
    application::port::{filter_page, CommentRepository, CommentSummary, OwnerFilters, ReadGate},
    domain::value_object::{PostId, Viewer},
    error::CommentError,
};

pub struct ListTopLevelQuery {
    pub post_id:    String,
    pub limit:      i32,
    pub page_token: Option<String>,
    /// Who is reading (see [`ReadGate`]).
    pub viewer:     Viewer,
    /// The reader is cleared for mature content (not a guest, not 13–17):
    /// the comments of an age-gated post are hidden otherwise.
    pub mature:     bool,
}

impl Query for ListTopLevelQuery {
    type Response = (Vec<CommentSummary>, Option<String>);
}

pub struct ListTopLevelHandler<R> {
    pub repository: Arc<R>,
    pub gate:       Arc<dyn ReadGate>,
    pub filters:    OwnerFilters,
}

impl<R: CommentRepository> QueryHandler<ListTopLevelQuery> for ListTopLevelHandler<R> {
    type Error = CommentError;

    async fn handle(
        &self,
        envelope: Envelope<ListTopLevelQuery>,
    ) -> Result<(Vec<CommentSummary>, Option<String>), CommentError> {
        let q       = &envelope.payload;
        let post_id = PostId::try_from(q.post_id.as_str())?;
        let page = self
            .repository
            .list_top_level(&post_id, q.limit, q.page_token.as_deref())
            .await?;
        filter_page(self.gate.as_ref(), &self.filters, &q.viewer, q.mature, &post_id, page).await
    }
}
