use std::collections::HashSet;
use std::sync::Arc;

use async_trait::async_trait;

use crate::application::port::{CommentFilterStore, CommentSummary};
use crate::domain::comment_filter::{CommentFilter, TermList};
use crate::domain::value_object::{PostId, ProfileId, Viewer};
use crate::error::CommentError;

/// What a reader may see of a readable post's comments.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ReadDecision {
    /// Comment authors the reader must not see (blocks, hidden profiles,
    /// commenters the post's owner restricted).
    pub hidden_authors: HashSet<ProfileId>,
    /// The post's author, whose comment filter applies (`None` when unknown).
    pub post_author:    Option<ProfileId>,
}

/// Decides what a reader may see of a post's comments: the post itself (its
/// status, a moderation takedown, its author's audience) and the comment
/// authors (blocks, hidden profiles, and commenters the post's owner restricted:
/// seen only by themselves and the owner). Backed by post `GetPost` and
/// social-graph `CheckAccess` / `ListRestrictedAmong`; errors are
/// `AccessCheckUnavailable` and readers fail closed.
#[async_trait]
pub trait ReadGate: Send + Sync + 'static {
    /// `None` when the viewer may not read `post_id` at all (missing, draft,
    /// deleted, removed, or an author they may not see). Otherwise the subset
    /// of `comment_authors` whose comments the viewer must not see, and the
    /// post's author.
    /// `mature`: the reader is cleared for mature content; an age-gated post
    /// is unreadable otherwise (unless the reader wrote it).
    async fn check(
        &self,
        viewer: &Viewer,
        mature: bool,
        post_id: &PostId,
        comment_authors: &[ProfileId],
    ) -> Result<Option<ReadDecision>, CommentError>;

    /// May `author` comment on `post_id`? The post must be one they can read
    /// (published and visible to them — or their own), and its author's
    /// interaction settings must let them comment (social-graph
    /// `CheckInteraction`: everyone / followers / mutuals / no one, a block
    /// either way refuses). Errors are `AccessCheckUnavailable`; the write
    /// fails closed.
    async fn may_comment(&self, author: &ProfileId, post_id: &PostId) -> Result<CommentAdmission, CommentError>;
}

/// The answer to [`ReadGate::may_comment`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CommentAdmission {
    Allowed,
    /// Missing, or not one the author may read: indistinguishable on purpose.
    PostUnavailable,
    /// The post's author does not take comments from this profile.
    Restricted,
}

/// The post owners' comment filters (#660): their hidden words, and the
/// offensive-term list for owners who keep the offensive filter on.
#[derive(Clone)]
pub struct OwnerFilters {
    pub store:     Arc<dyn CommentFilterStore>,
    pub offensive: Arc<TermList>,
}

impl OwnerFilters {
    /// The filter of the post's author (none when unknown). Errors fail the
    /// read closed, like the gate.
    pub async fn of(&self, post_author: Option<&ProfileId>) -> Result<Option<CommentFilter>, CommentError> {
        match post_author {
            Some(owner) => Ok(Some(self.store.get(owner).await?)),
            None => Ok(None),
        }
    }

    /// May `viewer` see a comment by `author` with `body`, under the decision
    /// and the owner's filter? A commenter always sees their own comment.
    pub fn shows(
        &self,
        viewer: &Viewer,
        decision: &ReadDecision,
        filter: Option<&CommentFilter>,
        author: &ProfileId,
        body: Option<&str>,
    ) -> bool {
        if decision.hidden_authors.contains(author) {
            return false;
        }
        let own = matches!(viewer, Viewer::Profiles(ids) if ids.contains(author));
        own || !matches!((filter, body), (Some(f), Some(b)) if f.hides(b, &self.offensive))
    }
}

/// Applies the gate to a page of comments on `post_id`: the whole page goes when
/// the post is not readable (no next token either), and the comments of hidden
/// authors, or that the post owner's filter hides (#660), are dropped. A
/// trusted internal caller gets the page untouched. The filter can shorten a
/// page while `next` stays valid.
pub async fn filter_page(
    gate: &dyn ReadGate,
    filters: &OwnerFilters,
    viewer: &Viewer,
    mature: bool,
    post_id: &PostId,
    (mut page, next): (Vec<CommentSummary>, Option<String>),
) -> Result<(Vec<CommentSummary>, Option<String>), CommentError> {
    if *viewer == Viewer::Internal {
        return Ok((page, next));
    }
    let mut authors: Vec<ProfileId> = page.iter().map(|c| c.author_id.clone()).collect();
    authors.sort_by_key(|a| a.as_str());
    authors.dedup();
    match gate.check(viewer, mature, post_id, &authors).await? {
        None => Ok((Vec::new(), None)),
        Some(decision) => {
            let filter = filters.of(decision.post_author.as_ref()).await?;
            page.retain(|c| filters.shows(viewer, &decision, filter.as_ref(), &c.author_id, c.body.as_deref()));
            Ok((page, next))
        }
    }
}
