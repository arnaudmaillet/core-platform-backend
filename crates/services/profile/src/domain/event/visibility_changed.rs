use chrono::{DateTime, Utc};
use uuid::Uuid;

use crate::domain::value_object::{ProfileId, ProfileVisibility};

/// The owner switched the profile between public and private. Consumers that
/// enforce audience (social-graph's access check) project it.
#[derive(Debug, Clone)]
pub struct VisibilityChanged {
    pub profile_id: ProfileId,
    pub visibility: ProfileVisibility,
    pub occurred_at: DateTime<Utc>,
    pub correlation_id: Uuid,
}
