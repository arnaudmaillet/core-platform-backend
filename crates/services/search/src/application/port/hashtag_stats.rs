use async_trait::async_trait;

use crate::error::SearchError;

/// One tag's use among the searchable posts (#831).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TagUsage {
    pub tag:       String,
    /// Searchable posts carrying it.
    pub total:     i64,
    /// Of those, created in the last 24 hours.
    pub last_day:  i64,
    /// Of those, created in the last 7 days.
    pub last_week: i64,
}

/// A trending hashtag, as the hashtag documents hold it.
#[derive(Debug, Clone, PartialEq)]
pub struct TrendingTag {
    pub tag:        String,
    pub post_count: i64,
}

/// The hashtags' own data (#831), derived from the post documents: their use,
/// and the documents the refresh keeps.
#[async_trait]
pub trait HashtagStats: Send + Sync + 'static {
    /// The `max` most used tags among the searchable posts — the same posts a
    /// search may return (moderation, owner, discovery, post window).
    async fn tag_usage(&self, max: usize) -> Result<Vec<TagUsage>, SearchError>;

    /// Removes the hashtag documents written before `revision` (tags no longer
    /// used). Returns how many went.
    async fn purge_hashtags_before(&self, revision: u64) -> Result<u64, SearchError>;

    /// The hashtags rising now, most first; tags unused this week are left out.
    async fn trending(&self, limit: usize) -> Result<Vec<TrendingTag>, SearchError>;
}
