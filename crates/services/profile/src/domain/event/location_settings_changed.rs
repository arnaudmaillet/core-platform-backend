use chrono::{DateTime, Utc};
use uuid::Uuid;

use crate::domain::value_object::{LocationSettings, ProfileId};

/// The owner changed ghost mode or location precision. geo-discovery projects
/// it and applies it to every map surface.
#[derive(Debug, Clone)]
pub struct LocationSettingsChanged {
    pub profile_id: ProfileId,
    pub settings: LocationSettings,
    pub occurred_at: DateTime<Utc>,
    pub correlation_id: Uuid,
}
