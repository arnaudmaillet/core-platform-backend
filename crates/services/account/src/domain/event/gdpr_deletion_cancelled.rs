use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::domain::value_object::AccountId;

/// A requested erasure was withdrawn within its grace period — by the holder
/// signing back in, or through `CancelGdprDeletion`. Nothing was erased.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GdprDeletionCancelled {
    pub account_id: AccountId,
    /// When the erasure would have become permanent.
    pub was_scheduled_at: DateTime<Utc>,
    pub occurred_at: DateTime<Utc>,
    pub correlation_id: Uuid,
}
