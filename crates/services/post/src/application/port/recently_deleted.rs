use async_trait::async_trait;
use chrono::{DateTime, Utc};

use crate::domain::value_object::{PostId, ProfileId};
use crate::error::PostError;

/// An author's "Recently deleted" index (#663): entries expire after the
/// 30-day restore window.
#[async_trait]
pub trait RecentlyDeleted: Send + Sync + 'static {
    async fn add(&self, author: &ProfileId, deleted_at: DateTime<Utc>, post: &PostId) -> Result<(), PostError>;

    async fn remove(&self, author: &ProfileId, deleted_at: DateTime<Utc>, post: &PostId) -> Result<(), PostError>;

    /// Newest deletion first. The page token is opaque.
    async fn list(
        &self,
        author: &ProfileId,
        limit: i32,
        page_token: Option<&str>,
    ) -> Result<(Vec<PostId>, Option<String>), PostError>;
}
