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

use std::sync::Arc;

use async_trait::async_trait;
use auth_context::{AuthError, JwtDecoder, OidcClaims, OidcClaimsExtractor};
use chrono::{DateTime, Utc};

use crate::application::port::TokenVerifier;
use crate::domain::{DeviceId, Session, UserId};
use crate::error::RealtimeError;

/// The OIDC decoder specialization the realtime gateway uses.
type EdgeDecoder = JwtDecoder<OidcClaims, OidcClaimsExtractor>;

pub struct AuthContextTokenVerifier {
    decoder: Arc<EdgeDecoder>,
    /// The custom JWT claim carrying the device/session id (default: `did`).
    device_claim: String,
}

impl AuthContextTokenVerifier {
    pub fn new(decoder: Arc<EdgeDecoder>, device_claim: impl Into<String>) -> Self {
        Self {
            decoder,
            device_claim: device_claim.into(),
        }
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

        Ok(Session::new(user_id, device_id, expires_at))
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
}
