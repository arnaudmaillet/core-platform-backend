use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::domain::value_object::{AccountId, EmailAddress};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EmailChanged {
    pub account_id: AccountId,
    /// `None` when the account had no email (a phone-only account adding one).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub old_email: Option<EmailAddress>,
    pub new_email: EmailAddress,
    pub occurred_at: DateTime<Utc>,
    pub correlation_id: Uuid,
}
