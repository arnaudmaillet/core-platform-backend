use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::domain::value_object::{AccountId, ConsentPurpose};

/// One consent given or withdrawn.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct ConsentChange {
    pub purpose: ConsentPurpose,
    pub granted: bool,
}

/// The holder changed one or more consents (GDPR Art. 7(1): the controller must
/// be able to demonstrate consent — this, and the persisted history, is that
/// record). Only effective changes are listed.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ConsentsUpdated {
    pub account_id: AccountId,
    pub changes: Vec<ConsentChange>,
    /// The privacy-policy version the holder saw, when the client sent one.
    pub policy_version: Option<String>,
    pub occurred_at: DateTime<Utc>,
    pub correlation_id: Uuid,
}
