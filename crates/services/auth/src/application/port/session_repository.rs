use async_trait::async_trait;

use crate::domain::aggregate::Session;
use crate::domain::value_object::{AccountId, SessionId};
use crate::error::AuthError;

/// How many of the account's latest sessions decide whether its own client
/// sends no device id (#649).
pub const RECENT_SESSIONS: i64 = 5;

/// What an account's sessions — every one issued, whatever its status — say of
/// a device (#649: a sign-in from a device never seen is announced).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct DeviceHistory {
    /// The account had a session before.
    pub any_session: bool,
    /// One of them was on this device (never, for a sign-in without a device id).
    pub seen_device: bool,
    /// Its latest [`RECENT_SESSIONS`] sessions all came without a device id:
    /// the holder's own client sends none.
    pub recent_without_device_id: bool,
}

impl DeviceHistory {
    /// A sign-in worth telling the holder about, on an account that signed in
    /// before (not its very first sign-in): from a device never seen, or with
    /// no device id at all — the id is client-written, so leaving it out must
    /// not silence the alert — unless the holder's own client sends none.
    pub fn announces(&self, with_device_id: bool) -> bool {
        self.any_session && if with_device_id { !self.seen_device } else { !self.recent_without_device_id }
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

    /// The account's sessions, of any status, against `device_id` (`None`: the
    /// sign-in sent none).
    async fn device_history(&self, account_id: &AccountId, device_id: Option<&str>)
    -> Result<DeviceHistory, AuthError>;
}
