use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::domain::value_object::AccountId;

/// A guest's device became a member (#827): it signed up or signed in.
/// Emitted once per guest. The wallet credits the guest's welcome gift from
/// it — once per device, whoever signed in.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GuestUpgraded {
    /// The account the guest became.
    pub account_id: AccountId,
    /// SHA-256 (lowercase hex) of the device: its App Attest key when one
    /// vouched for the install, else its device id. Never the raw id.
    pub device_key_hash: String,
    /// When the guest first opened the app. Informational: the welcome gift
    /// is a flat amount (#943).
    pub guest_since: DateTime<Utc>,
    pub occurred_at: DateTime<Utc>,
    pub correlation_id: Uuid,
}
