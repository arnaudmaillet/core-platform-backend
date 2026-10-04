use chrono::{DateTime, Utc};
use uuid::Uuid;

use crate::domain::value_object::{DiscoverySettings, ProfileId};

/// The owner changed presence or discoverability. search projects
/// `by_handle_search`; chat, the presence flags.
#[derive(Debug, Clone)]
pub struct DiscoverySettingsChanged {
    pub profile_id: ProfileId,
    pub settings: DiscoverySettings,
    pub occurred_at: DateTime<Utc>,
    pub correlation_id: Uuid,
}
