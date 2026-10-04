use async_trait::async_trait;

use crate::domain::comment_filter::CommentFilter;
use crate::domain::value_object::ProfileId;
use crate::error::CommentError;

/// The post owners' comment filters, projected from `profile.v1.events`.
#[async_trait]
pub trait CommentFilterStore: Send + Sync + 'static {
    /// `owner`'s filter; the default when the projection has none.
    async fn get(&self, owner: &ProfileId) -> Result<CommentFilter, CommentError>;

    async fn set(&self, owner: &ProfileId, filter: &CommentFilter) -> Result<(), CommentError>;
}
