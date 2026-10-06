use chrono::{DateTime, Utc};
use uuid::Uuid;

use crate::domain::value_object::{FeedSettings, ProfileId};

/// The owner changed the feed controls (#662), or a teen's profile was born
/// with personalisation off. timeline projects `non_personalized`: it stops
/// learning interest tags and erases them, and stops ranking For You.
#[derive(Debug, Clone)]
pub struct FeedSettingsChanged {
    pub profile_id: ProfileId,
    pub settings: FeedSettings,
    pub occurred_at: DateTime<Utc>,
    pub correlation_id: Uuid,
}
