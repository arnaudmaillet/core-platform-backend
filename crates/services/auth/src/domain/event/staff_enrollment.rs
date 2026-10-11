use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::domain::value_object::AccountId;

/// A staff passkey enrollment grant was issued for `account_id` (#880): by an
/// admin in a staff passkey session, or by the one-off bootstrap
/// (`granted_by` absent). Recorded by audit as a privileged action. The grant
/// itself (a secret) never leaves auth; `grant_id` names it.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StaffEnrollmentGranted {
    pub account_id:     AccountId,
    pub grant_id:       Uuid,
    pub granted_by:     Option<AccountId>,
    pub expires_at:     DateTime<Utc>,
    pub occurred_at:    DateTime<Utc>,
    pub correlation_id: Uuid,
}

/// A passkey was registered as a staff passkey with grant `grant_id` (#880):
/// sessions opened with it carry the account's staff permissions.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StaffPasskeyEnrolled {
    pub account_id:     AccountId,
    pub grant_id:       Uuid,
    pub occurred_at:    DateTime<Utc>,
    pub correlation_id: Uuid,
}
