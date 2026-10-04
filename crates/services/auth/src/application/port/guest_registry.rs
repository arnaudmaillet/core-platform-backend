use async_trait::async_trait;
use chrono::{DateTime, Utc};

use crate::domain::value_object::AccountId;
use crate::error::AuthError;

/// What auth records about a guest when its session starts: the device it came
/// from and when. Sign-up (B4) credits the welcome gift once per device from
/// it; abuse controls (B5) verify the attestation and limit per device.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GuestRecord {
    pub guest_id:         AccountId,
    pub device_id:        String,
    /// An App Attest / DeviceCheck assertion was sent. Not verified yet (B5).
    pub attestation_sent: bool,
    pub locale:           Option<String>,
    pub region_hint:      Option<String>,
    /// ISO country derived on the device (B10 verifies it against GeoIP).
    pub current_country:  Option<String>,
    pub first_seen_at:    DateTime<Utc>,
}

/// Durable record of guests (Postgres `guest_principals`).
#[async_trait]
pub trait GuestRegistry: Send + Sync + 'static {
    /// Records a new guest. Idempotent on `guest_id`.
    async fn record(&self, guest: &GuestRecord) -> Result<(), AuthError>;
}
