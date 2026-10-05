use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::domain::value_object::AccountId;

/// The holder's GDPR data export (Art. 15/20) is ready to download until
/// `expires_at` (#653). The link itself is never on the event (it is a
/// credential): it is on the holder's GDPR record (`GetGdprRecord`), where
/// auth reads it to email the holder.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GdprDataExportCompleted {
    pub account_id: AccountId,
    pub expires_at: DateTime<Utc>,
    pub occurred_at: DateTime<Utc>,
    pub correlation_id: Uuid,
}
