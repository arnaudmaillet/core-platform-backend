use async_trait::async_trait;

use crate::error::EngagementError;

/// One saved post (#872).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SavedPost {
    pub profile_id:  String,
    pub post_id:     String,
    /// When it was first saved (ms).
    pub saved_at_ms: i64,
}

/// Where a profile's Saved tab resumes: the last save of the page before.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SavedCursor {
    pub saved_at_ms: i64,
    pub post_id:     String,
}

impl SavedCursor {
    /// `<saved_at_ms>:<post_id>`.
    pub fn encode(&self) -> String {
        format!("{}:{}", self.saved_at_ms, self.post_id)
    }

    pub fn decode(token: &str) -> Option<Self> {
        let (at, post) = token.split_once(':')?;
        Some(Self { saved_at_ms: at.parse().ok()?, post_id: post.to_owned() }).filter(|c| !c.post_id.is_empty())
    }
}

/// The saved posts (Scylla), per profile and per account.
#[async_trait]
pub trait SavedPosts: Send + Sync + 'static {
    /// Saves `post_id` for `profile_id` (of `account`) at `at_micros`, unless
    /// it is saved already (its first save time stays).
    async fn save(&self, account: &str, profile_id: &str, post_id: &str, at_micros: i64) -> Result<(), EngagementError>;

    /// Unsaves it as of `at_micros`; a no-op when it isn't saved.
    async fn unsave(&self, account: &str, profile_id: &str, post_id: &str, at_micros: i64)
        -> Result<(), EngagementError>;

    /// A page of `profile_id`'s saves, most recently saved first, after
    /// `after`; and where the next page starts (`None`: the last). The page
    /// may be shorter than `limit` and still be followed.
    async fn list(
        &self,
        profile_id: &str,
        limit: i32,
        after: Option<&SavedCursor>,
    ) -> Result<(Vec<SavedPost>, Option<SavedCursor>), EngagementError>;

    /// What `account` saved, by profile then post, up to `limit` after
    /// `after` (`(profile_id, post_id)`; the GDPR export).
    async fn list_by_account(
        &self,
        account: &str,
        limit: i32,
        after: Option<(&str, &str)>,
    ) -> Result<Vec<SavedPost>, EngagementError>;

    /// Forgets a deleted profile's saves as of `at_micros`.
    async fn forget_profile(&self, profile_id: &str, at_micros: i64) -> Result<(), EngagementError>;

    /// Forgets a deleted account's saves, every profile's, as of `at_micros`.
    async fn forget_account(&self, account: &str, at_micros: i64) -> Result<(), EngagementError>;
}
