use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

use super::{AccountId, AgeBracket, Generation, Permission, ProfileId, SessionId, SessionKind};

/// The normalized claim set for an edge access token, produced by
/// [`Session::mint_access_token`](crate::domain::aggregate::Session::mint_access_token).
///
/// This is the **domain** view of a token — not its wire form. The infrastructure
/// `TokenMinterPort` (Phase 4) serializes it into an ES256 JWT (PASETO later);
/// the `auth-context` library reconstructs the same shape on verification. By
/// construction `expires_at` is clamped to the session's horizon, so the token's
/// lifetime is always a subset of its session's lifetime.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AccessTokenClaims {
    /// `sub` — the internal account id (never the IdP subject).
    pub account_id: AccountId,
    /// `sid` — the session this token belongs to.
    pub session_id: SessionId,
    /// `gen` — the revocation epoch checked at the edge for instant logout.
    pub generation: Generation,
    /// Normalized authorization grants.
    pub permissions: Vec<Permission>,
    /// `pids` — the profiles the account owns at mint time. Client-facing
    /// services bind profile-keyed actors to this set (see `transport::grpc::edge`).
    /// A profile created after the mint appears at the next refresh.
    pub profile_ids: Vec<ProfileId>,
    /// `did` — the stable device id the session was bound to at login
    /// (`DeviceContext.device_id`). The realtime gateway keys a socket by it and
    /// rejects a handshake without it. `None` when the client sent no device id.
    #[serde(default)]
    pub device_id: Option<String>,
    /// `kind` — a member, or a guest (then `account_id` is a guest id, the
    /// wire `sub` is `guest:<id>`, and `profile_ids` is empty).
    #[serde(default)]
    pub kind: SessionKind,
    /// `auth_time` — when the holder last proved a credential. Set only on a
    /// token minted right after that proof (`Login`, the `VerifyCredentials`
    /// step-up); `None` on a refreshed token. Step-up-gated RPCs elsewhere read
    /// it (`transport::grpc::edge::require_recent_auth`).
    #[serde(default)]
    pub auth_time: Option<DateTime<Utc>>,
    /// `age` — the holder's age bracket, re-read from `account` at every
    /// member mint; `None` for a guest or when no date of birth is on file.
    #[serde(default)]
    pub age_bracket: Option<AgeBracket>,
    pub issued_at: DateTime<Utc>,
    /// Always ≤ the session's sliding and absolute expiry.
    pub expires_at: DateTime<Utc>,
}

impl AccessTokenClaims {
    #[allow(clippy::too_many_arguments)] // one positional arg per claim
    pub(crate) fn new(
        account_id: AccountId,
        session_id: SessionId,
        generation: Generation,
        permissions: Vec<Permission>,
        profile_ids: Vec<ProfileId>,
        device_id: Option<String>,
        kind: SessionKind,
        issued_at: DateTime<Utc>,
        expires_at: DateTime<Utc>,
    ) -> Self {
        Self {
            account_id,
            session_id,
            generation,
            permissions,
            profile_ids,
            device_id,
            kind,
            auth_time: None,
            age_bracket: None,
            issued_at,
            expires_at,
        }
    }

    /// Remaining lifetime in whole seconds at `now` (saturating at zero).
    pub fn expires_in_secs(&self, now: DateTime<Utc>) -> i64 {
        (self.expires_at - now).num_seconds().max(0)
    }
}
