//! The handshake token verifier over the shared `auth-context` JWT decoder.
//!
//! Verifies the ES256 edge token at the WebSocket handshake and distils it into a
//! pinned [`Session`]: `sub` → `user_id`, `exp` → `expires_at`, and a configured
//! custom claim → `device_id`. The edge token is minted by `auth` per device
//! session, so the device binding rides as a claim (default key `did`).
//!
//! Error mapping: an expired token is `RTM-1002 TokenExpired` (the client should
//! refresh and re-handshake); anything else — bad signature, malformed, missing
//! claim — is `RTM-1001 HandshakeRejected`. The plane never reveals *why* beyond
//! these two, and logs nothing token-bearing.
//!
//! With [`Revocations`] attached (#903), a token `auth` revoked since it was
//! minted is `RTM-1001` too, and the session keeps the token's `sid` / `gen` so
//! the gateway re-checks it while the socket lives. Realtime is TIER-1: an
//! unreadable revocation state admits the token (until `exp`).

use std::sync::Arc;

use async_trait::async_trait;
use auth_context::{AuthError, JwtDecoder, OidcClaims, OidcClaimsExtractor};
use chrono::{DateTime, Utc};

use transport::grpc::revocation::Revocations;

use crate::application::port::TokenVerifier;
use crate::domain::{DeviceId, Session, UserId};
use crate::error::RealtimeError;

/// The OIDC decoder specialization the realtime gateway uses.
type EdgeDecoder = JwtDecoder<OidcClaims, OidcClaimsExtractor>;

pub struct AuthContextTokenVerifier {
    decoder: Arc<EdgeDecoder>,
    /// The custom JWT claim carrying the device/session id (default: `did`).
    device_claim: String,
    revocations: Option<Revocations>,
}

impl AuthContextTokenVerifier {
    pub fn new(decoder: Arc<EdgeDecoder>, device_claim: impl Into<String>) -> Self {
        Self {
            decoder,
            device_claim: device_claim.into(),
            revocations: None,
        }
    }

    /// Also refuses a token revoked in `auth` (#903).
    pub fn with_revocations(mut self, revocations: Revocations) -> Self {
        self.revocations = Some(revocations);
        self
    }
}

fn map_auth_err(e: AuthError) -> RealtimeError {
    match e {
        AuthError::TokenExpired => RealtimeError::TokenExpired,
        other => RealtimeError::HandshakeRejected {
            reason: other.to_string(),
        },
    }
}

#[async_trait]
impl TokenVerifier for AuthContextTokenVerifier {
    async fn verify(
        &self,
        edge_token: &str,
        now: DateTime<Utc>,
    ) -> Result<Session, RealtimeError> {
        let principal = self.decoder.decode(edge_token).await.map_err(map_auth_err)?;
        reject_guest(&principal.raw_claims)?;
        admit(self.revocations.as_ref(), &principal.raw_claims).await?;

        let user_id = UserId::new(principal.user_id.as_str().to_owned())?;

        let expires_at = DateTime::from_timestamp(principal.raw_claims.exp, 0).ok_or_else(|| {
            RealtimeError::HandshakeRejected {
                reason: "edge token has an invalid 'exp'".to_owned(),
            }
        })?;
        // Defense in depth: the decoder already rejects expired tokens, but pin the
        // session against our own clock too.
        if now >= expires_at {
            return Err(RealtimeError::TokenExpired);
        }

        let device = principal
            .raw_claims
            .extra
            .get(&self.device_claim)
            .and_then(|v| v.as_str())
            .ok_or_else(|| RealtimeError::HandshakeRejected {
                reason: format!("edge token is missing the '{}' device claim", self.device_claim),
            })?;
        let device_id = DeviceId::new(device.to_owned())?;

        Ok(with_token_ids(Session::new(user_id, device_id, expires_at), &principal.raw_claims))
    }
}

/// A token `auth` revoked since it was minted is `RTM-1001` (#903).
async fn admit(revocations: Option<&Revocations>, claims: &auth_context::OidcClaims) -> Result<(), RealtimeError> {
    let Some(revocations) = revocations else {
        return Ok(());
    };
    revocations
        .admit(claims)
        .await
        .map_err(|refusal| RealtimeError::HandshakeRejected { reason: refusal.reason().to_owned() })
}

/// Keeps the token's `sid` / `gen` on the session, for the heartbeat re-check.
fn with_token_ids(session: Session, claims: &auth_context::OidcClaims) -> Session {
    match (auth_context::edge::session_id(claims), auth_context::edge::generation(claims)) {
        (Some(sid), Some(generation)) => session.with_token(sid, generation),
        _ => session,
    }
}

/// Guests (anonymous, read-only sessions) get no realtime connection in v1:
/// live delivery is per account and device, and a guest has no account.
fn reject_guest(claims: &auth_context::OidcClaims) -> Result<(), RealtimeError> {
    if auth_context::edge::is_guest(claims) {
        return Err(RealtimeError::HandshakeRejected {
            reason: "guest sessions cannot open a realtime connection".to_owned(),
        });
    }
    Ok(())
}

#[cfg(test)]
mod guest_tests {
    use super::*;

    fn claims(json: serde_json::Value) -> auth_context::OidcClaims {
        serde_json::from_value(json).unwrap()
    }

    #[test]
    fn a_guest_token_is_refused_a_member_token_is_not() {
        let guest = claims(serde_json::json!({ "sub": "guest:g-1", "exp": 4_102_444_800_i64, "kind": "guest", "did": "d" }));
        assert!(matches!(reject_guest(&guest), Err(RealtimeError::HandshakeRejected { .. })));
        let member = claims(serde_json::json!({ "sub": "acct-1", "exp": 4_102_444_800_i64, "did": "d" }));
        assert!(reject_guest(&member).is_ok());
    }

    /// Revoked when `gen` is below 2; `None` = unreadable.
    struct State(Option<()>);

    impl transport::grpc::revocation::RevocationCheck for State {
        fn is_revoked<'a>(&'a self, _: &'a str, _: &'a str, generation: i64) -> transport::grpc::revocation::RevocationLookup<'a> {
            Box::pin(async move { self.0.ok_or(transport::grpc::revocation::RevocationUnavailable).map(|_| generation < 2) })
        }
    }

    /// #903: a revoked token is refused at the handshake (`RTM-1001`); an
    /// unreadable state admits it (fail open); unconfigured, nothing is checked.
    #[tokio::test]
    async fn the_handshake_refuses_a_revoked_token_and_fails_open() {
        use transport::grpc::revocation::{RevocationFailure, Revocations};
        let live = claims(serde_json::json!({ "sub": "acct-1", "exp": 4_102_444_800_i64, "sid": "s-1", "gen": 2 }));
        let revoked = claims(serde_json::json!({ "sub": "acct-1", "exp": 4_102_444_800_i64, "sid": "s-1", "gen": 1 }));
        let up = Revocations::new(std::sync::Arc::new(State(Some(()))), RevocationFailure::Open);
        assert!(admit(Some(&up), &live).await.is_ok());
        assert!(matches!(admit(Some(&up), &revoked).await, Err(RealtimeError::HandshakeRejected { .. })));
        let down = Revocations::new(std::sync::Arc::new(State(None)), RevocationFailure::Open);
        assert!(admit(Some(&down), &live).await.is_ok());
        assert!(admit(None, &revoked).await.is_ok());
    }

    #[test]
    fn the_session_keeps_the_token_ids() {
        let session = || {
            Session::new(
                UserId::new("acct-1".to_owned()).unwrap(),
                DeviceId::new("d".to_owned()).unwrap(),
                DateTime::from_timestamp(4_102_444_800, 0).unwrap(),
            )
        };
        let with = with_token_ids(session(), &claims(serde_json::json!({ "sub": "acct-1", "exp": 1, "sid": "s-1", "gen": 3 })));
        let token = with.token.unwrap();
        assert_eq!((token.session_id.as_str(), token.generation), ("s-1", 3));
        assert!(with_token_ids(session(), &claims(serde_json::json!({ "sub": "acct-1", "exp": 1 }))).token.is_none());
    }
}
