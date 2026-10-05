use async_trait::async_trait;

use crate::domain::aggregate::Session;
use crate::domain::value_object::{AccountId, SessionId};
use crate::error::AuthError;

/// What an account's sessions — every one issued, whatever its status — say of
/// a device (#649: a sign-in from a device never seen is announced).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct DeviceHistory {
    /// The account had a session before.
    pub any_session: bool,
    /// One of them was on this device.
    pub seen_device: bool,
}

impl DeviceHistory {
    /// A sign-in worth telling the holder about: from a new device, on an
    /// account that signed in before (not its very first sign-in).
    pub fn is_new_device(&self) -> bool {
        self.any_session && !self.seen_device
    }
}

/// Durable persistence port for the [`Session`] aggregate (Postgres adapter,
/// sharded by `account_id`).
///
/// `save` upserts with optimistic-lock semantics on the aggregate `version`.
#[async_trait]
pub trait SessionRepository: Send + Sync + 'static {
    async fn save(&self, session: &Session) -> Result<(), AuthError>;

    async fn find_by_id(&self, id: &SessionId) -> Result<Option<Session>, AuthError>;

    /// Active sessions for an account — the device-management view and the set a
    /// global sign-out iterates over.
    async fn list_active_by_account(
        &self,
        account_id: &AccountId,
    ) -> Result<Vec<Session>, AuthError>;

    /// The account's sessions, of any status, against `device_id`.
    async fn device_history(&self, account_id: &AccountId, device_id: &str) -> Result<DeviceHistory, AuthError>;
}
