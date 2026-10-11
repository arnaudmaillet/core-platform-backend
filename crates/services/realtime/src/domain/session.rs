use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

use crate::domain::value_object::{ChannelRef, DeviceId, UserId};
use crate::error::RealtimeError;

/// The pinned identity of an authenticated connection.
///
/// A `Session` is created once, at the WebSocket handshake, from the verified
/// edge token — and is then immutable for the connection's life. Authentication
/// is never re-checked per frame; the token's `expires_at` bounds the session
/// (see [`Session::is_expired`]), and the gateway re-checks its revocation on
/// the heartbeat (#903, [`Session::token`]). This is the linchpin of the plane's
/// security model: every authorization decision is made against this fixed
/// identity, so a frame can never act as another user.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Session {
    pub user_id: UserId,
    pub device_id: DeviceId,
    /// Absolute expiry of the edge token that authenticated this session.
    pub expires_at: DateTime<Utc>,
    /// The token's `auth` session and revocation generation (`sid`, `gen`):
    /// what the gateway re-checks against `auth`'s revocation state while the
    /// socket lives (#903). `None` for a token without them.
    #[serde(default)]
    pub token: Option<TokenIds>,
}

/// The ids a verified edge token is revoked by.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TokenIds {
    pub session_id: String,
    pub generation: i64,
}

impl Session {
    pub fn new(user_id: UserId, device_id: DeviceId, expires_at: DateTime<Utc>) -> Self {
        Self {
            user_id,
            device_id,
            expires_at,
            token: None,
        }
    }

    /// Records the token's `sid` and `gen` (#903).
    pub fn with_token(mut self, session_id: impl Into<String>, generation: i64) -> Self {
        self.token = Some(TokenIds { session_id: session_id.into(), generation });
        self
    }

    /// Whether the session's edge token has lapsed as of `now`. When true, the
    /// gateway sends a re-auth directive; the client refreshes and re-presents a
    /// token (or re-handshakes) — see `RTM-1002`.
    pub fn is_expired(&self, now: DateTime<Utc>) -> bool {
        now >= self.expires_at
    }

    /// The plane's **one and only** authorization rule: a connection may subscribe
    /// to a channel only if it *owns* the channel's key.
    ///
    /// * Identity-scoped classes (DM / NOTIFICATION / PRESENCE): the key must
    ///   equal this session's `user_id` — you cannot tap another user's stream.
    ///   A mismatch is `RTM-3001 ChannelForbidden` (a security-relevant denial).
    /// * Public classes (COUNTER / FEED): any authenticated identity may
    ///   subscribe; content visibility was already decided upstream at emit time.
    pub fn authorize(&self, channel: &ChannelRef) -> Result<(), RealtimeError> {
        if channel.class.is_identity_scoped() && channel.key.as_str() != self.user_id.as_str() {
            return Err(RealtimeError::ChannelForbidden {
                channel: channel.to_string(),
            });
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use chrono::TimeZone;
    use error::AppError;

    use super::*;
    use crate::domain::value_object::{ChannelClass, ChannelKey};

    fn session_for(user: &str) -> Session {
        Session::new(
            UserId::new(user).unwrap(),
            DeviceId::new("dev-1").unwrap(),
            Utc.with_ymd_and_hms(2026, 6, 26, 12, 0, 0).unwrap(),
        )
    }

    fn channel(class: ChannelClass, key: &str) -> ChannelRef {
        ChannelRef::new(class, ChannelKey::new(key).unwrap())
    }

    #[test]
    fn expiry_is_inclusive_of_now() {
        let s = session_for("alice");
        let before = Utc.with_ymd_and_hms(2026, 6, 26, 11, 59, 59).unwrap();
        let at = Utc.with_ymd_and_hms(2026, 6, 26, 12, 0, 0).unwrap();
        assert!(!s.is_expired(before));
        assert!(s.is_expired(at));
    }

    #[test]
    fn owns_its_own_identity_scoped_channels() {
        let s = session_for("alice");
        s.authorize(&channel(ChannelClass::Dm, "alice")).unwrap();
        s.authorize(&channel(ChannelClass::Notification, "alice"))
            .unwrap();
        s.authorize(&channel(ChannelClass::Presence, "alice"))
            .unwrap();
    }

    #[test]
    fn cannot_tap_another_users_stream() {
        let s = session_for("alice");
        let err = s.authorize(&channel(ChannelClass::Dm, "bob")).unwrap_err();
        assert_eq!(err.error_code(), "RTM-3001");
    }

    #[test]
    fn any_identity_may_subscribe_to_public_channels() {
        let s = session_for("alice");
        // A public entity stream is keyed by entity id, not the subscriber.
        s.authorize(&channel(ChannelClass::Counter, "post-42"))
            .unwrap();
        s.authorize(&channel(ChannelClass::Feed, "author-9")).unwrap();
    }
}
