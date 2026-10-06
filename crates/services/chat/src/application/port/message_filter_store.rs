use async_trait::async_trait;
use text_filter::ContentFilter;

use crate::domain::value_object::ProfileId;
use crate::error::ChatError;

/// What each member hides from the message requests they get (#810): their
/// hidden words and the offensive filter, projected from `profile.v1.events`.
#[async_trait]
pub trait MessageFilterStore: Send + Sync + 'static {
    async fn set(&self, profile: &ProfileId, filter: &ContentFilter) -> Result<(), ChatError>;

    /// `profile`'s filter (absent ⇒ the default: offensive filter on).
    async fn get(&self, profile: &ProfileId) -> Result<ContentFilter, ChatError>;
}
