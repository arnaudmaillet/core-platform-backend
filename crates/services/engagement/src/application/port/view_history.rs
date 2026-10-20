use async_trait::async_trait;

use crate::error::EngagementError;

/// How long a view stays in its profile's history (#944): 90 days.
pub const HISTORY_RETENTION_DAYS: i64 = 90;

/// A re-view this close to the last one changes nothing (a feed scrolled
/// back and forth).
pub const REVIEW_DEBOUNCE_MICROS: i64 = 60 * 1_000_000;

/// One viewed post (#944): the last time the profile viewed it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ViewedPost {
    pub profile_id:   String,
    pub post_id:      String,
    /// When it was last viewed (ms).
    pub viewed_at_ms: i64,
}

/// Where a profile's history resumes: the last view of the page before.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ViewCursor {
    pub viewed_at_ms: i64,
    pub post_id:      String,
}

impl ViewCursor {
    /// `<viewed_at_ms>:<post_id>`.
    pub fn encode(&self) -> String {
        format!("{}:{}", self.viewed_at_ms, self.post_id)
    }

    pub fn decode(token: &str) -> Option<Self> {
        let (at, post) = token.split_once(':')?;
        Some(Self { viewed_at_ms: at.parse().ok()?, post_id: post.to_owned() }).filter(|c| !c.post_id.is_empty())
    }
}

/// The viewing history (Scylla), per profile and per account; every view
/// expires after [`HISTORY_RETENTION_DAYS`].
#[async_trait]
pub trait ViewHistory: Send + Sync + 'static {
    /// `profile_id` (of `account`) viewed `post_id` at `at_micros`: the post
    /// moves to the top of its history, once. A re-view within
    /// [`REVIEW_DEBOUNCE_MICROS`] of the last is a no-op.
    async fn record(&self, account: &str, profile_id: &str, post_id: &str, at_micros: i64) -> Result<(), EngagementError>;

    /// A page of `profile_id`'s history, most recently viewed first, after
    /// `after`; and where the next page starts (`None`: the last). The page
    /// may be shorter than `limit` and still be followed.
    async fn list(
        &self,
        profile_id: &str,
        limit: i32,
        after: Option<&ViewCursor>,
    ) -> Result<(Vec<ViewedPost>, Option<ViewCursor>), EngagementError>;

    /// Takes `post_id` out of the history as of `at_micros`; a no-op when
    /// it isn't there.
    async fn remove(&self, account: &str, profile_id: &str, post_id: &str, at_micros: i64) -> Result<(), EngagementError>;

    /// Clears `profile_id`'s history as of `at_micros`.
    async fn clear(&self, account: &str, profile_id: &str, at_micros: i64) -> Result<(), EngagementError>;

    /// What `account` viewed, by profile then post, up to `limit` after
    /// `after` (`(profile_id, post_id)`; the GDPR export).
    async fn list_by_account(
        &self,
        account: &str,
        limit: i32,
        after: Option<(&str, &str)>,
    ) -> Result<Vec<ViewedPost>, EngagementError>;

    /// Forgets a deleted profile's history as of `at_micros`.
    async fn forget_profile(&self, profile_id: &str, at_micros: i64) -> Result<(), EngagementError>;

    /// Forgets a deleted account's history, every profile's, as of `at_micros`.
    async fn forget_account(&self, account: &str, at_micros: i64) -> Result<(), EngagementError>;
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_cursor_round_trips_and_garbage_is_refused() {
        let cursor = ViewCursor { viewed_at_ms: 1_700_000_000_000, post_id: "p:1".into() };
        assert_eq!(ViewCursor::decode(&cursor.encode()), Some(cursor));
        assert_eq!(ViewCursor::decode("x:p"), None);
        assert_eq!(ViewCursor::decode("12:"), None);
    }
}
