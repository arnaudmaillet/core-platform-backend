use async_trait::async_trait;
use chrono::{DateTime, Utc};

use crate::domain::recent_search::RecentSearch;
use crate::error::SearchError;

/// Each profile's recent searches (#663): newest first, at most
/// [`MAX_RECENT_SEARCHES`](crate::domain::recent_search::MAX_RECENT_SEARCHES),
/// none older than the retention.
#[async_trait]
pub trait RecentSearches: Send + Sync + 'static {
    /// Puts `query` (normalized) first — once: an earlier identical search,
    /// whatever its case, moves up rather than repeats.
    async fn record(&self, profile_id: &str, query: &str, at: DateTime<Utc>) -> Result<(), SearchError>;

    /// The profile's recent searches as of `now`, newest first.
    async fn list(&self, profile_id: &str, now: DateTime<Utc>) -> Result<Vec<RecentSearch>, SearchError>;

    /// Removes one (case-insensitive).
    async fn delete(&self, profile_id: &str, query: &str) -> Result<(), SearchError>;

    /// Removes them all.
    async fn clear(&self, profile_id: &str) -> Result<(), SearchError>;
}
