use async_trait::async_trait;

use crate::domain::aggregate::FeedEntry;
use crate::domain::value_object::{AuthorId, AuthorTier, PostId};
use crate::error::TimelineError;

/// Port for the ScyllaDB per-author reverse index: `timeline.posts_by_author`.
///
/// Serves two roles:
///   1. VIP cold-start source: reconstruct `timeline:vip:{author_id}` after eviction.
///   2. Follow-backfill source: inject a followee's recent posts into a new
///      follower's feed on `follow.created` for Standard/Premium authors.
///
/// Written for ALL authors on `PostPublished` (one row per post).
#[async_trait]
pub trait AuthorPostRepository: Send + Sync + 'static {
    /// Inserts or updates an author's post entry. Last-write-wins. Idempotent.
    async fn insert(
        &self,
        author_id:   &AuthorId,
        post_id:     &PostId,
        tier:        AuthorTier,
        published_at_ms: i64,
    ) -> Result<(), TimelineError>;

    /// Deletes an author's post entry. Idempotent (no-op if absent).
    async fn delete(
        &self,
        author_id:  &AuthorId,
        post_id:    &PostId,
        published_at_ms: i64,
    ) -> Result<(), TimelineError>;

    /// Reads at most `limit` most-recent posts by an author.
    ///
    /// Used during:
    ///   - Follow backfill: `limit` = `TIMELINE_BACKFILL_LIMIT` (default 100)
    ///   - VIP cold-start:  `limit` = `vip_registry_cap` (default 200)
    ///
    /// Cursor semantics: pass `before_ms = i64::MAX` for the first page.
    async fn list_recent(
        &self,
        author_id: &AuthorId,
        before_ms: i64,
        limit:     i32,
    ) -> Result<Vec<FeedEntry>, TimelineError>;
}
