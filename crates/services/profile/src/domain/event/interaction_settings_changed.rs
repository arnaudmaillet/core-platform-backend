use chrono::{DateTime, Utc};
use uuid::Uuid;

use crate::domain::value_object::{InteractionSettings, ProfileId};

/// The owner changed who may comment, mention or message, or download / like
/// counts. social-graph projects it for `CheckInteraction`.
#[derive(Debug, Clone)]
pub struct InteractionSettingsChanged {
    pub profile_id: ProfileId,
    pub settings: InteractionSettings,
    pub occurred_at: DateTime<Utc>,
    pub correlation_id: Uuid,
}
