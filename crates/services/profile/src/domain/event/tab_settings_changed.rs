use chrono::{DateTime, Utc};
use uuid::Uuid;

use crate::domain::value_object::{ProfileId, TabSettings};

/// The owner changed the post history window or a tab's visibility. post
/// projects the window and applies it to everyone but the owner.
#[derive(Debug, Clone)]
pub struct TabSettingsChanged {
    pub profile_id: ProfileId,
    pub settings: TabSettings,
    pub occurred_at: DateTime<Utc>,
    pub correlation_id: Uuid,
}
