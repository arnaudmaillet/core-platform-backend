//! A post's comments as one timeline (#988): top-level comments and replies
//! alike, newest first, for a conversation-style reading. Each reply quotes
//! the comment it answers, so a client draws the quote without a `GetComment`
//! per reply. The `ListTopLevel` read rules apply, to the quotes too: a quote
//! never shows what the reader could not read on its own.

use std::collections::HashMap;
use std::sync::Arc;

use cqrs::{Envelope, Query, QueryHandler};
use futures::{StreamExt, TryStreamExt};

use crate::{
    application::port::{CommentRepository, CommentSummary, OwnerFilters, ReadGate},
    domain::value_object::{CommentId, CommentStatus, PostId, ProfileId, Viewer},
    error::CommentError,
};

/// The most characters of a quoted body.
pub const EXCERPT_CHARS: usize = 120;
/// Quoted comments read at once.
const CONCURRENT_PARENT_READS: usize = 16;

pub struct ListTimelineQuery {
    pub post_id:    String,
    pub limit:      i32,
    pub page_token: Option<String>,
    /// Who is reading (see [`ReadGate`]).
    pub viewer:     Viewer,
    /// The reader is cleared for mature content (not a guest, not 13–17):
    /// the comments of an age-gated post are hidden otherwise.
    pub mature:     bool,
}

/// A comment of the timeline, and the quote of the comment it answers.
#[derive(Debug, Clone)]
pub struct TimelineComment {
    pub comment:  CommentSummary,
    /// `Some` on a reply.
    pub reply_to: Option<ReplyQuote>,
}

/// The comment a reply answers.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReplyQuote {
    pub comment_id: CommentId,
    /// `None` when nothing is quoted: the comment is deleted, or one the
    /// reader may not see — the two are not told apart.
    pub quoted:     Option<Quoted>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Quoted {
    pub author_id: ProfileId,
    /// The start of its body, at most [`EXCERPT_CHARS`] characters; empty
    /// without a body.
    pub excerpt:   String,
    pub has_gif:   bool,
}

impl Query for ListTimelineQuery {
    type Response = (Vec<TimelineComment>, Option<String>);
}

pub struct ListTimelineHandler<R> {
    pub repository: Arc<R>,
    pub gate:       Arc<dyn ReadGate>,
    pub filters:    OwnerFilters,
}

/// A quoted comment as the quote needs it.
struct Parent {
    author_id: ProfileId,
    status:    CommentStatus,
    body:      Option<String>,
    has_gif:   bool,
    held:      bool,
}

impl From<&CommentSummary> for Parent {
    fn from(c: &CommentSummary) -> Self {
        Self {
            author_id: c.author_id.clone(),
            status:    c.status,
            body:      c.body.clone(),
            has_gif:   c.gif_url.is_some(),
            held:      c.held,
        }
    }
}

impl<R: CommentRepository> QueryHandler<ListTimelineQuery> for ListTimelineHandler<R> {
    type Error = CommentError;

    async fn handle(
        &self,
        envelope: Envelope<ListTimelineQuery>,
    ) -> Result<(Vec<TimelineComment>, Option<String>), CommentError> {
        let q       = &envelope.payload;
        let post_id = PostId::try_from(q.post_id.as_str())?;
        let (mut page, next) = self.repository.list_timeline(&post_id, q.limit, q.page_token.as_deref()).await?;
        let parents = self.parents(&post_id, &page).await?;

        let gate = if q.viewer == Viewer::Internal {
            None
        } else {
            // One check for the page's authors and the quoted ones.
            let mut authors: Vec<ProfileId> =
                page.iter().map(|c| c.author_id.clone()).chain(parents.values().map(|p| p.author_id.clone())).collect();
            authors.sort_by_key(|a| a.as_str());
            authors.dedup();
            let Some(decision) = self.gate.check(&q.viewer, q.mature, &post_id, &authors).await? else {
                return Ok((Vec::new(), None));
            };
            let filter = self.filters.of(decision.post_author.as_ref()).await?;
            Some((decision, filter))
        };
        let shows = |author: &ProfileId, body: Option<&str>, held: bool| match &gate {
            None => true,
            Some((decision, filter)) => self.filters.shows(&q.viewer, decision, filter.as_ref(), author, body, held),
        };

        page.retain(|c| shows(&c.author_id, c.body.as_deref(), c.held));
        let timeline = page
            .into_iter()
            .map(|comment| {
                let reply_to = comment.parent_id.clone().map(|comment_id| {
                    let quoted = parents
                        .get(&comment_id)
                        .filter(|p| p.status == CommentStatus::Published)
                        .filter(|p| shows(&p.author_id, p.body.as_deref(), p.held))
                        .map(|p| Quoted {
                            author_id: p.author_id.clone(),
                            excerpt:   excerpt(p.body.as_deref().unwrap_or_default()),
                            has_gif:   p.has_gif,
                        });
                    ReplyQuote { comment_id, quoted }
                });
                TimelineComment { comment, reply_to }
            })
            .collect();
        Ok((timeline, next))
    }
}

impl<R: CommentRepository> ListTimelineHandler<R> {
    /// The comments the page's replies answer: from the page when it holds
    /// them, else read ([`CONCURRENT_PARENT_READS`] at a time, once each). A
    /// purged one is absent.
    async fn parents(&self, post_id: &PostId, page: &[CommentSummary]) -> Result<HashMap<CommentId, Parent>, CommentError> {
        let mut parents: HashMap<CommentId, Parent> = HashMap::new();
        let mut missing: Vec<CommentId> = Vec::new();
        for id in page.iter().filter_map(|c| c.parent_id.as_ref()) {
            if parents.contains_key(id) || missing.contains(id) {
                continue;
            }
            match page.iter().find(|c| &c.comment_id == id) {
                Some(parent) => {
                    parents.insert(id.clone(), parent.into());
                }
                None => missing.push(id.clone()),
            }
        }
        // The futures built first: a closure inside the stream would not be
        // `Send` for every lifetime the handler's future needs.
        let reads: Vec<_> = missing.iter().map(|id| self.repository.find_by_id(id)).collect();
        let read: Vec<_> = futures::stream::iter(reads)
            .buffer_unordered(CONCURRENT_PARENT_READS)
            .try_collect()
            .await?;
        for comment in read.into_iter().flatten().filter(|c| c.post_id() == post_id) {
            parents.insert(comment.id().clone(), Parent {
                author_id: comment.author_id().clone(),
                status:    comment.status(),
                body:      comment.body().map(|b| b.as_str().to_owned()),
                has_gif:   comment.gif().is_some(),
                held:      comment.held(),
            });
        }
        Ok(parents)
    }
}

/// The start of `body`, at most [`EXCERPT_CHARS`] characters.
fn excerpt(body: &str) -> String {
    body.chars().take(EXCERPT_CHARS).collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn an_excerpt_cuts_on_characters_not_bytes() {
        assert_eq!(excerpt("short"), "short");
        let long = "é".repeat(EXCERPT_CHARS + 10);
        let cut = excerpt(&long);
        assert_eq!(cut.chars().count(), EXCERPT_CHARS);
        assert!(long.starts_with(&cut));
    }
}
