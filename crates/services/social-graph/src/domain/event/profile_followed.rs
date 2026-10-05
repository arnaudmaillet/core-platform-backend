use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

use crate::domain::value_object::ProfileId;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ProfileFollowed {
    pub actor_id:    ProfileId,
    pub target_id:   ProfileId,
    pub followed_at: DateTime<Utc>,
    /// The follow is a private profile's owner approving a request (#655): the
    /// requester is told it was accepted. Absent from older events.
    #[serde(default)]
    pub via_request: bool,
}
