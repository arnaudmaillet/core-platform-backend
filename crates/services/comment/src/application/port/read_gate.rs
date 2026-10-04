use std::collections::HashSet;

use async_trait::async_trait;

use crate::application::port::CommentSummary;
use crate::domain::value_object::{PostId, ProfileId, Viewer};
use crate::error::CommentError;

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
    /// of `comment_authors` whose comments the viewer must not see.
    async fn check(
        &self,
        viewer: &Viewer,
        post_id: &PostId,
        comment_authors: &[ProfileId],
    ) -> Result<Option<HashSet<ProfileId>>, CommentError>;

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

/// Applies the gate to a page of comments on `post_id`: the whole page goes when
/// the post is not readable (no next token either), and the comments of hidden
/// authors are dropped. A trusted internal caller gets the page untouched. The
/// filter can shorten a page while `next` stays valid.
pub async fn filter_page(
    gate: &dyn ReadGate,
    viewer: &Viewer,
    post_id: &PostId,
    (mut page, next): (Vec<CommentSummary>, Option<String>),
) -> Result<(Vec<CommentSummary>, Option<String>), CommentError> {
    if *viewer == Viewer::Internal {
        return Ok((page, next));
    }
    let mut authors: Vec<ProfileId> = page.iter().map(|c| c.author_id.clone()).collect();
    authors.sort_by_key(|a| a.as_str());
    authors.dedup();
    match gate.check(viewer, post_id, &authors).await? {
        None => Ok((Vec::new(), None)),
        Some(hidden) => {
            page.retain(|c| !hidden.contains(&c.author_id));
            Ok((page, next))
        }
    }
}
