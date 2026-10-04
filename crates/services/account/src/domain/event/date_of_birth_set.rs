use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::domain::value_object::AccountId;

/// The holder recorded a date of birth. The date itself stays in account (it is
/// private); consumers read the age bracket from the edge token's `age` claim.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DateOfBirthSet {
    pub account_id: AccountId,
    pub occurred_at: DateTime<Utc>,
    pub correlation_id: Uuid,
}
