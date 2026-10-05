use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

use crate::domain::value_object::ProfileId;

/// `actor_id`'s pending request to follow `target_id` is gone without becoming
/// a follow: cancelled by the actor, declined by the owner, cut by a block, or
/// moot once the profile is public and the actor follows it. `requested_at`
/// names the request, so `notification` retracts the "X asked to follow you"
/// it sent for it.
///
/// Published on `social-graph.follow_requested` with the request's key, so it
/// is never read before the request it withdraws.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FollowRequestWithdrawn {
    pub actor_id:     ProfileId,
    pub target_id:    ProfileId,
    pub requested_at: DateTime<Utc>,
    pub withdrawn_at: DateTime<Utc>,
}
