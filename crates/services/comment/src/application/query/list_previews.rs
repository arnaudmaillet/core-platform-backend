//! The first top-level comments of many posts at once (#976), for the tiles
//! a client opens full screen: per post, the start of what that viewer's
//! `ListTopLevel` lists, newest first, through the same read gate. Bounded
//! fan-out; a post that fails or is slow is left out (the client reads it on
//! its own), never the batch.

use std::collections::HashSet;
use std::sync::Arc;
use std::time::Duration;

use cqrs::{Envelope, Query, QueryHandler};
use futures::StreamExt;

use crate::{
    application::port::{gate_page, CommentRepository, CommentSummary, OwnerFilters, ReadGate},
    domain::value_object::{PostId, Viewer},
    error::CommentError,
};

/// Most posts per call.
pub const MAX_POSTS: usize = 50;
/// Comments per post: the default and the cap.
pub const DEFAULT_LIMIT: i32 = 5;
pub const MAX_LIMIT: i32 = 10;
/// How many rows a post's read takes per comment shown, so a post whose
/// newest comments are hidden from the viewer still fills its preview; at
/// least [`READ_FLOOR`], at most [`READ_CAP`] rows.
const READ_MARGIN: i32 = 3;
const READ_FLOOR: i32 = 15;
const READ_CAP: i32 = 30;
/// Posts read at once.
const CONCURRENT_POSTS: usize = 16;
/// A post slower than this is left out.
const POST_TIMEOUT: Duration = Duration::from_millis(800);

pub struct ListPreviewsQuery {
    pub post_ids: Vec<String>,
    /// `0` ⇒ [`DEFAULT_LIMIT`]; capped at [`MAX_LIMIT`].
    pub limit:    i32,
    /// Who is reading (see [`ReadGate`]).
    pub viewer:   Viewer,
    /// The reader is cleared for mature content (as `ListTopLevel`).
    pub mature:   bool,
}

/// A readable post's preview: its first comments for the viewer (possibly
/// none).
#[derive(Debug, Clone)]
pub struct CommentPreview {
    pub post_id:  PostId,
    pub comments: Vec<CommentSummary>,
}

impl Query for ListPreviewsQuery {
    type Response = Vec<CommentPreview>;
}

pub struct ListPreviewsHandler<R> {
    pub repository: Arc<R>,
    pub gate:       Arc<dyn ReadGate>,
    pub filters:    OwnerFilters,
}

impl<R: CommentRepository> QueryHandler<ListPreviewsQuery> for ListPreviewsHandler<R> {
    type Error = CommentError;

    async fn handle(&self, envelope: Envelope<ListPreviewsQuery>) -> Result<Vec<CommentPreview>, CommentError> {
        let q = &envelope.payload;
        let mut seen = HashSet::new();
        let posts: Vec<PostId> = q
            .post_ids
            .iter()
            .map(|id| PostId::try_from(id.as_str()))
            .collect::<Result<Vec<_>, _>>()?
            .into_iter()
            .filter(|post| seen.insert(post.as_uuid()))
            .collect();
        if posts.len() > MAX_POSTS {
            return Err(CommentError::DomainViolation {
                field:   "post_ids".to_owned(),
                message: format!("at most {MAX_POSTS} posts a call"),
            });
        }
        let limit = match q.limit {
            l if l <= 0 => DEFAULT_LIMIT,
            l => l.min(MAX_LIMIT),
        };
        let mut previews: Vec<(usize, CommentPreview)> = futures::stream::iter(posts.into_iter().enumerate())
            .map(|(i, post)| async move {
                match tokio::time::timeout(POST_TIMEOUT, self.preview(&post, limit, &q.viewer, q.mature)).await {
                    Ok(Ok(Some(comments))) => Some((i, CommentPreview { post_id: post, comments })),
                    Ok(Ok(None)) => None,
                    Ok(Err(error)) => {
                        tracing::warn!(%error, post = %post.as_str(), "comment preview left out");
                        None
                    }
                    Err(_) => {
                        tracing::warn!(post = %post.as_str(), "comment preview timed out: left out");
                        None
                    }
                }
            })
            .buffer_unordered(CONCURRENT_POSTS)
            .filter_map(std::future::ready)
            .collect()
            .await;
        previews.sort_by_key(|(i, _)| *i);
        Ok(previews.into_iter().map(|(_, preview)| preview).collect())
    }
}

impl<R: CommentRepository> ListPreviewsHandler<R> {
    /// The post's first `limit` comments the viewer's `ListTopLevel` lists,
    /// with their reply counts; `None` when the viewer may not read the post.
    async fn preview(
        &self,
        post_id: &PostId,
        limit:   i32,
        viewer:  &Viewer,
        mature:  bool,
    ) -> Result<Option<Vec<CommentSummary>>, CommentError> {
        let rows = (limit * READ_MARGIN).clamp(READ_FLOOR, READ_CAP);
        let page = self.repository.list_top_level(post_id, rows, None).await?;
        let Some((mut comments, _)) = gate_page(self.gate.as_ref(), &self.filters, viewer, mature, post_id, page).await? else {
            return Ok(None);
        };
        comments.truncate(limit as usize);
        let ids: Vec<_> = comments.iter().map(|c| c.comment_id.clone()).collect();
        let counts = self.repository.reply_counts(post_id, &ids).await?;
        for comment in &mut comments {
            comment.reply_count = counts.get(&comment.comment_id).copied().unwrap_or(0);
        }
        Ok(Some(comments))
    }
}
