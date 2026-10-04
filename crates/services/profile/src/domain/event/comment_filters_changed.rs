use chrono::{DateTime, Utc};
use uuid::Uuid;

use crate::domain::value_object::{CommentFilters, ProfileId};

/// The owner changed their hidden words or the offensive-comment filter.
/// comment projects it and applies it to the comments on their posts.
#[derive(Debug, Clone)]
pub struct CommentFiltersChanged {
    pub profile_id: ProfileId,
    pub filters: CommentFilters,
    pub occurred_at: DateTime<Utc>,
    pub correlation_id: Uuid,
}
