use std::collections::HashMap;

use async_trait::async_trait;

use crate::domain::value_object::ProfileId;
use crate::error::ChatError;

/// What a member shares with the other members (#661, from profile's
/// discovery settings).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PresenceSettings {
    /// Online / offline signals reach the other members.
    pub activity_status: bool,
    /// Read receipts reach the other members.
    pub read_receipts:   bool,
}

impl Default for PresenceSettings {
    fn default() -> Self {
        Self { activity_status: true, read_receipts: true }
    }
}

impl PresenceSettings {
    /// What to apply when the settings cannot be read: share nothing.
    pub const WITHHELD: Self = Self { activity_status: false, read_receipts: false };
}

/// The members' presence settings, projected from `profile.v1.events`.
#[async_trait]
pub trait PresenceSettingsStore: Send + Sync + 'static {
    async fn set(&self, profile: &ProfileId, settings: PresenceSettings) -> Result<(), ChatError>;

    /// The settings of each of `profiles` (absent ⇒ the default).
    async fn get_many(&self, profiles: &[ProfileId]) -> Result<HashMap<ProfileId, PresenceSettings>, ChatError>;

    /// `profile`'s settings, or [`PresenceSettings::WITHHELD`] when they cannot
    /// be read (a privacy setting fails closed).
    async fn get_or_withheld(&self, profile: &ProfileId) -> PresenceSettings {
        match self.get_many(std::slice::from_ref(profile)).await {
            Ok(map) => map.get(profile).copied().unwrap_or_default(),
            Err(error) => {
                tracing::warn!(%error, "presence settings unavailable; withholding presence");
                PresenceSettings::WITHHELD
            }
        }
    }
}
