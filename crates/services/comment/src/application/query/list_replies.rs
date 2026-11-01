use std::sync::Arc;

use cqrs::{Envelope, Query, QueryHandler};

pub use crate::application::query::popularity::{CommentSort, POPULARITY_WINDOW};
use crate::{
    application::port::{filter_page, CommentLikes, CommentRepository, CommentSummary, OwnerFilters, ReadGate},
    application::query::popularity::{self, Slot},
    domain::value_object::{CommentId, PostId, Viewer},
    error::CommentError,
};

pub struct ListRepliesQuery {
    pub post_id:    String,
    pub comment_id: String,
    pub limit:      i32,
    pub page_token: Option<String>,
    pub sort:       CommentSort,
    /// Who is reading (see [`ReadGate`]).
    pub viewer:     Viewer,
    /// The reader is cleared for mature content (not a guest, not 13–17):
    /// the comments of an age-gated post are hidden otherwise.
    pub mature:     bool,
}

impl Query for ListRepliesQuery {
    type Response = (Vec<CommentSummary>, Option<String>);
}

pub struct ListRepliesHandler<R> {
    pub repository: Arc<R>,
    pub gate:       Arc<dyn ReadGate>,
    pub filters:    OwnerFilters,
    /// Like counts, for the popularity order (#832).
    pub likes:      Arc<dyn CommentLikes>,
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
        let page = popularity::page(
            self.repository.as_ref(),
            self.likes.as_ref(),
            &post_id,
            Slot::Replies(&comment_id),
            q.sort,
            q.limit,
            q.page_token.as_deref(),
        )
        .await?;
        filter_page(self.gate.as_ref(), &self.filters, &q.viewer, q.mature, &post_id, page).await
    }
}
