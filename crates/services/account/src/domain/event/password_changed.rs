use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::domain::value_object::AccountId;

/// No longer emitted: passwords are gone (#881) and nothing changes one. Kept
/// because `audit` still decodes `account.password_changed` on
/// `account.v1.events` (its Authentication category); retire both together.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PasswordChanged {
    pub account_id: AccountId,
    pub occurred_at: DateTime<Utc>,
    pub correlation_id: Uuid,
}
