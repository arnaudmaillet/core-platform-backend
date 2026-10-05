use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

use crate::domain::value_object::ProfileId;

/// `actor_id` asked to follow `target_id`, a private profile: a pending request
/// until its owner approves it (then a [`super::ProfileFollowed`] with
/// `via_request`) or declines it (silently).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FollowRequested {
    pub actor_id:     ProfileId,
    pub target_id:    ProfileId,
    pub requested_at: DateTime<Utc>,
}
