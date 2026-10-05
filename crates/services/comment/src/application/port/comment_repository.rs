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
}

#[async_trait]
pub trait CommentRepository: Send + Sync + 'static {
    /// Inserts a new comment into both `comment.comments` and
    /// `comment.comments_by_post`. Both writes must succeed atomically from the
    /// caller's perspective — failure leaves retry responsibility with the caller.
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

    /// Physical delete: removes rows from both tables.
    async fn purge(&self, comment: &Comment) -> Result<(), CommentError>;

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
