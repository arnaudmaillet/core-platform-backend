use async_trait::async_trait;

use crate::domain::device::Device;
use crate::domain::preferences::NotificationPreferences;
use crate::domain::value_object::ProfileId;
use crate::error::NotificationError;

/// The devices registered for push, per profile.
#[async_trait]
pub trait DeviceRegistry: Send + Sync + 'static {
    /// Registers (or refreshes) `device` for `profile` on behalf of `account`.
    /// The token leaves every profile of another account it was registered
    /// for, and the device's previous token (if it changed) is forgotten.
    async fn register(&self, profile: &ProfileId, account: &str, device: &Device) -> Result<(), NotificationError>;

    /// Forgets `device_id` for `profile` (absent is fine).
    async fn unregister(&self, profile: &ProfileId, device_id: &str) -> Result<(), NotificationError>;

    /// The profile's devices.
    async fn devices(&self, profile: &ProfileId) -> Result<Vec<Device>, NotificationError>;
}

/// The holders' notification preferences.
#[async_trait]
pub trait PreferenceStore: Send + Sync + 'static {
    /// The stored preferences; `None` when the holder never saved any.
    async fn get(&self, profile: &ProfileId) -> Result<Option<NotificationPreferences>, NotificationError>;

    async fn put(&self, profile: &ProfileId, preferences: &NotificationPreferences) -> Result<(), NotificationError>;
}
