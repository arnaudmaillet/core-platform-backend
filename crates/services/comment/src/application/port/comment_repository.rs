use std::collections::HashMap;

use async_trait::async_trait;
use chrono::{DateTime, Utc};

use crate::{
    domain::{
        aggregate::Comment,
        value_object::{CommentId, CommentStatus, PostId, ProfileId},
    },
    error::CommentError,
};

/// Feed-optimised projection returned by list operations.
///
/// Sourced from `comment.comments_by_post` to avoid a secondary point-read
/// per row. `body` and `gif_*` fields are `None` on tombstoned comments.
#[derive(Debug, Clone)]
pub struct CommentSummary {
    pub comment_id: CommentId,
    pub author_id:  ProfileId,
    pub status:     CommentStatus,
    pub body:       Option<String>,
    pub gif_url:    Option<String>,
    pub gif_width:  Option<u32>,
    pub gif_height: Option<u32>,
    pub created_at: DateTime<Utc>,
    /// Held for the post owner's review (#669).
    pub held:       bool,
    /// A top-level comment's replies (#832); filled by ListTopLevel, 0 elsewhere.
    pub reply_count: u64,
}

#[async_trait]
pub trait CommentRepository: Send + Sync + 'static {
    /// Inserts a new comment into both `comment.comments` and
    /// `comment.comments_by_post`. Both writes must succeed atomically from the
    /// caller's perspective — failure leaves retry responsibility with the caller.
    /// A reply also counts on its parent (#832).
    async fn insert(&self, comment: &Comment) -> Result<(), CommentError>;

    /// Point-reads a comment by its ID from `comment.comments`.
    async fn find_by_id(&self, id: &CommentId) -> Result<Option<Comment>, CommentError>;

    /// Sets the comment's held flag in both tables (#669).
    async fn set_held(&self, comment: &Comment, held: bool) -> Result<(), CommentError>;

    /// Returns `true` if `comment.comments_by_post` contains at least one
    /// published row where `parent_id = comment_id`. Used to choose between
    /// `Tombstone` and `Purge` deletion strategies.
    async fn has_active_replies(
        &self,
        post_id:    &PostId,
        comment_id: &CommentId,
    ) -> Result<bool, CommentError>;

    /// Soft-deletes: updates status and nulls content fields in both tables.
    async fn soft_delete(&self, comment: &Comment) -> Result<(), CommentError>;

    /// Physical delete: removes rows from both tables. A purged reply stops
    /// counting on its parent (#832).
    async fn purge(&self, comment: &Comment) -> Result<(), CommentError>;

    /// The replies of each of `parents` (top-level comments of `post_id`,
    /// #832). A parent with none is absent.
    async fn reply_counts(
        &self,
        post_id: &PostId,
        parents: &[CommentId],
    ) -> Result<HashMap<CommentId, u64>, CommentError>;

    /// The `max` newest replies to a top-level comment, newest first — the
    /// window a popularity page ranks (#832).
    async fn list_reply_window(
        &self,
        post_id:    &PostId,
        comment_id: &CommentId,
        max:        i32,
    ) -> Result<Vec<CommentSummary>, CommentError>;

    /// The `list_replies` page token that resumes right after `last` — how a
    /// popularity page carries on, newest first, past its ranked window.
    fn page_token_after(&self, last: &CommentSummary) -> String;

    /// A profile's own comments, newest first, as `comment.comments` holds
    /// them now (tombstones included, purged ones gone) — from
    /// `comments_by_author` (#653). Returns `(comments, next_page_token)`.
    async fn list_by_author(
        &self,
        author_id:  &ProfileId,
        limit:      i32,
        page_token: Option<&str>,
    ) -> Result<(Vec<Comment>, Option<String>), CommentError>;

    /// Indexes every comment in `comment.comments` by its author (comments
    /// written before `comments_by_author` existed). Idempotent; returns the
    /// rows written.
    async fn backfill_author_index(&self) -> Result<u64, CommentError>;

    /// Paginates top-level comments for a post from `comments_by_post`,
    /// ordered by `created_at DESC`. Returns `(summaries, next_page_token)`.
    async fn list_top_level(
        &self,
        post_id:    &PostId,
        limit:      i32,
        page_token: Option<&str>,
    ) -> Result<(Vec<CommentSummary>, Option<String>), CommentError>;

    /// Paginates direct replies to a top-level comment from `comments_by_post`,
    /// ordered by `created_at DESC`. Returns `(summaries, next_page_token)`.
    async fn list_replies(
        &self,
        post_id:    &PostId,
        comment_id: &CommentId,
        limit:      i32,
        page_token: Option<&str>,
    ) -> Result<(Vec<CommentSummary>, Option<String>), CommentError>;
}
