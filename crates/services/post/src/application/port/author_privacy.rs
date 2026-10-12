use async_trait::async_trait;

use crate::domain::value_object::ProfileId;
use crate::error::PostError;

/// Which authors are private (#919 step 3), projected from `profile.v1.events`
/// (`ProfileVisibilityChanged`): a private author's post media is delivered
/// SIGNED only.
#[async_trait]
pub trait AuthorPrivacy: Send + Sync + 'static {
    /// Public when the projection has not seen the author.
    async fn is_private(&self, author: &ProfileId) -> Result<bool, PostError>;

    /// Upserts (last writer wins).
    async fn set_private(&self, author: &ProfileId, private: bool) -> Result<(), PostError>;
}
