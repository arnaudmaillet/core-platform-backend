use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::domain::value_object::{AccountId, AccountRole, AccountStatus, CountryCode, EmailAddress, IdentityId};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AccountCreated {
    pub account_id: AccountId,
    pub identity_id: IdentityId,
    /// Absent for a phone-only account.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub email: Option<EmailAddress>,
    pub role: AccountRole,
    pub status: AccountStatus,
    pub country_of_residence: Option<CountryCode>,
    pub occurred_at: DateTime<Utc>,
    pub correlation_id: Uuid,
}
