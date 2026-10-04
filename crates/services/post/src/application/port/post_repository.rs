use async_trait::async_trait;
use chrono::{DateTime, Utc};
use crate::{
    domain::{
        aggregate::Post,
        value_object::{ModerationRestriction, PostId, PostKind, PostStatus, ProfileId},
    },
    error::PostError,
};

pub struct PostSummary {
    pub post_id:    PostId,
    pub kind:       PostKind,
    pub status:     PostStatus,
    pub moderation: ModerationRestriction,
    pub created_at: DateTime<Utc>,
}

#[async_trait]
pub trait PostRepository: Send + Sync + 'static {
    async fn insert(&self, post: &Post) -> Result<(), PostError>;
    async fn update_content(&self, post: &Post) -> Result<(), PostError>;
    async fn update_lifecycle(&self, post: &Post) -> Result<(), PostError>;
    /// Persists the post's moderation state to both tables.
    async fn update_moderation(&self, post: &Post) -> Result<(), PostError>;
    async fn find_by_id(&self, id: &PostId) -> Result<Option<Post>, PostError>;
    async fn list_by_profile(
        &self,
        profile_id:  &ProfileId,
        limit:       i32,
        page_token:  Option<&str>,
    ) -> Result<(Vec<PostSummary>, Option<String>), PostError>;
}
