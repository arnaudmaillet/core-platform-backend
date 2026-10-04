use async_trait::async_trait;
use chrono::{DateTime, Duration, Utc};

use crate::domain::value_object::ProfileId;
use crate::error::PostError;

/// The authors' post history window (#664), projected from
/// `profile.v1.events` (`ProfileTabSettingsChanged`): how far back anyone but
/// the author sees their posts. Older posts are hidden, never deleted.
#[async_trait]
pub trait AuthorWindowStore: Send + Sync + 'static {
    /// The window in days; `None` for every post (the default).
    async fn get(&self, profile_id: &ProfileId) -> Result<Option<u32>, PostError>;

    async fn set(&self, profile_id: &ProfileId, window_days: Option<u32>) -> Result<(), PostError>;
}

/// The oldest creation time a window of `days` shows at `now`.
pub fn window_start(days: Option<u32>, now: DateTime<Utc>) -> Option<DateTime<Utc>> {
    days.map(|d| now - Duration::days(i64::from(d)))
}
