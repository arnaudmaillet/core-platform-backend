use async_trait::async_trait;

use crate::error::EngagementError;

/// Whose post it is and whether that author hides like counts (#809).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PostLikeVisibility {
    pub author_id: String,
    pub hidden:    bool,
}

/// Asks post (the owner of post → author and of the authors' settings).
#[async_trait]
pub trait LikeVisibility: Send + Sync + 'static {
    /// `None`: post does not know the post. An error: it could not tell —
    /// the caller withholds likes (fail closed).
    async fn of(&self, post_id: &str) -> Result<Option<PostLikeVisibility>, EngagementError>;

    /// [`Self::of`] for several posts, in order (#873: a Likes-tab page).
    async fn of_many(&self, post_ids: &[String]) -> Result<Vec<Option<PostLikeVisibility>>, EngagementError> {
        let mut answers = Vec::with_capacity(post_ids.len());
        for post_id in post_ids {
            answers.push(self.of(post_id).await?);
        }
        Ok(answers)
    }
}
