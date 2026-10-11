//! The caller gate for the privileged `audit.v1` surface.
//!
//! Audit is the fleet's compliance evidence plane, so its gRPC surface is
//! **need-to-know**: every RPC requires a verified caller identity (an ES256
//! edge token minted by `auth`) carrying the matching `audit:*` permission.
//! The gate fails CLOSED at every layer:
//!
//! * no / malformed / expired / unverifiable token → `AUD-3004`
//!   ([`AuditError::CallerUnauthenticated`], gRPC `UNAUTHENTICATED`);
//! * authenticated but missing the RPC's permission → the RPC's own AUD-3xxx
//!   denial (gRPC `PERMISSION_DENIED`);
//! * **no verifier configured** (no `AUDIT_JWKS_URL`) → [`DenyAllGate`] — the
//!   surface denies everything rather than silently serving unauthenticated
//!   reads. Health and reflection are runtime-owned and stay open.
//!
//! Nothing token-bearing is ever logged; on success only the principal id and
//! the RPC name are traced (the interim access trail until each read is
//! recorded as its own `DATA_ACCESS` event — see the README deferral).

use transport::grpc::revocation::{RevocationFailure, RevocationRefusal, Revocations};
use std::collections::HashSet;
use std::sync::Arc;

use async_trait::async_trait;
use auth_context::{
    spawn_edge_decoder, AuthContextConfig, JwtDecoder, OidcClaims, OidcClaimsExtractor,
};
use tonic::metadata::MetadataMap;

use crate::error::AuditError;

/// The `audit:*` permission tokens the gate recognises. Minted into the edge
/// token by `auth`; opaque strings by contract (see `auth-context::Permission`).
pub mod perm {
    /// Record on the synchronous privileged lane (break-glass / legal-hold).
    pub const RECORD: &str = "audit:record";
    /// Query ledger records (need-to-know read).
    pub const READ: &str = "audit:read";
    /// Export evidence bundles (stricter than read — bulk egress).
    pub const EXPORT: &str = "audit:export";
    /// Run integrity verification (chain / checkpoint reports, no record data).
    pub const VERIFY: &str = "audit:verify";
}

/// A verified caller of the privileged surface: the platform principal id and
/// the normalised permission set extracted from the verified token.
#[derive(Debug, Clone)]
pub struct Caller {
    pub principal: String,
    permissions: HashSet<String>,
}

impl Caller {
    pub fn new(principal: impl Into<String>, permissions: impl IntoIterator<Item = String>) -> Self {
        Self {
            principal: principal.into(),
            permissions: permissions.into_iter().collect(),
        }
    }

    /// `Ok(())` when the caller holds `permission`; `Err(denial)` otherwise.
    pub fn require(&self, permission: &str, denial: AuditError) -> Result<(), AuditError> {
        if self.permissions.contains(permission) {
            Ok(())
        } else {
            Err(denial)
        }
    }
}

/// Authenticates a Bearer token into a [`Caller`]. Authentication only — the
/// per-RPC permission check stays with the handler so each surface maps a
/// missing permission onto its own AUD-3xxx denial.
#[async_trait]
pub trait CallerGate: Send + Sync {
    /// Verify `bearer` (the token with the `Bearer ` scheme already stripped).
    /// `None` means the request carried no usable `authorization` metadata.
    async fn verify(&self, bearer: Option<&str>) -> Result<Caller, AuditError>;
}

/// Extract the Bearer token from gRPC request metadata, verify it through the
/// gate, and require `permission` — the single guard every RPC runs first.
pub async fn authorize(
    gate: &dyn CallerGate,
    metadata: &MetadataMap,
    permission: &str,
    denial: AuditError,
) -> Result<Caller, AuditError> {
    let bearer = metadata
        .get("authorization")
        .and_then(|v| v.to_str().ok())
        .and_then(|v| {
            let (scheme, token) = v.split_once(' ')?;
            scheme.eq_ignore_ascii_case("bearer").then_some(token.trim())
        })
        .filter(|t| !t.is_empty());

    let caller = gate.verify(bearer).await?;
    caller.require(permission, denial)?;
    Ok(caller)
}

/// The production gate: verifies the ES256 edge token against the `auth`
/// JWKS via the shared `auth-context` decoder (same pattern as the realtime
/// gateway's handshake verification).
pub struct AuthContextCallerGate {
    decoder: Arc<JwtDecoder<OidcClaims, OidcClaimsExtractor>>,
    /// `auth`'s revocation state (#903): a token revoked since it was minted
    /// (a staff member whose role was revoked, a signed-out session) is
    /// refused. Always fail closed (TIER-0).
    revocations: Option<Revocations>,
}

impl AuthContextCallerGate {
    pub fn new(decoder: Arc<JwtDecoder<OidcClaims, OidcClaimsExtractor>>) -> Self {
        Self { decoder, revocations: None }
    }

    /// Also refuses a revoked token (`AUD-3004`); an unreadable revocation
    /// state is `AUD-3006`, whatever posture `revocations` carries.
    pub fn with_revocations(mut self, revocations: Revocations) -> Self {
        self.revocations = Some(revocations.with_failure(RevocationFailure::Closed));
        self
    }
}

#[async_trait]
impl CallerGate for AuthContextCallerGate {
    async fn verify(&self, bearer: Option<&str>) -> Result<Caller, AuditError> {
        let token = bearer.ok_or(AuditError::CallerUnauthenticated)?;
        // Never surface decoder detail to the caller (and never log the token):
        // every verification failure collapses into the one 401.
        let principal = self
            .decoder
            .decode(token)
            .await
            .map_err(|_| AuditError::CallerUnauthenticated)?;
        if let Some(revocations) = &self.revocations {
            revocations.admit(&principal.raw_claims).await.map_err(|refusal| match refusal {
                RevocationRefusal::Revoked => AuditError::CallerUnauthenticated,
                RevocationRefusal::Unavailable => AuditError::CallerCheckUnavailable,
            })?;
        }
        Ok(Caller::new(
            principal.user_id.as_str().to_owned(),
            principal.permissions.into_iter().map(|p| p.0),
        ))
    }
}

/// The fail-closed fallback when no verifier is configured: every call is
/// `AUD-3004`. Deliberately NOT an allow-all — an unconfigured TIER-0 read
/// surface must deny, not open.
pub struct DenyAllGate;

#[async_trait]
impl CallerGate for DenyAllGate {
    async fn verify(&self, _bearer: Option<&str>) -> Result<Caller, AuditError> {
        Err(AuditError::CallerUnauthenticated)
    }
}

/// Build the gate from the resolved authz config: a JWKS-backed verifier when
/// configured, the deny-all fallback otherwise. The refresher task is detached
/// (dropping the handle keeps it running); a cold start does not require the
/// JWKS endpoint to be reachable — verification fails closed until the first
/// successful fetch.
pub fn build_gate(authz: &Option<AuthContextConfig>) -> Arc<dyn CallerGate> {
    match authz {
        Some(auth) => {
            // The shared edge decoder: JWKS refresher, the platform extractor (the
            // grants live in `perms`, which the default OIDC extractor never read,
            // so every real token used to be PERMISSION_DENIED) and ES256 only
            // (listing RS256 next to it made `jsonwebtoken` reject every valid
            // token as InvalidAlgorithm — see `auth_context::EDGE_ALGORITHMS`).
            let gate = AuthContextCallerGate::new(spawn_edge_decoder(auth));
            Arc::new(match service_runtime::edge_revocation() {
                Some(check) => gate.with_revocations(Revocations::new(check, RevocationFailure::Closed)),
                None => {
                    tracing::warn!("EDGE_REVOCATION_REDIS_HOSTS unset: a revoked token keeps reading audit until exp (#903)");
                    gate
                }
            })
        }
        None => {
            tracing::warn!(
                "AUDIT_JWKS_URL is not set — the audit.v1 surface DENIES all calls \
                 (AUD-3004) until a token verifier is configured"
            );
            Arc::new(DenyAllGate)
        }
    }
}

/// A fixed-outcome gate for composing the handler in tests.
#[cfg(test)]
pub struct StaticCallerGate {
    caller: Option<Caller>,
}

#[cfg(test)]
impl StaticCallerGate {
    /// Every call authenticates as `principal` holding `permissions`.
    pub fn allowing(principal: &str, permissions: &[&str]) -> Arc<Self> {
        Arc::new(Self {
            caller: Some(Caller::new(
                principal,
                permissions.iter().map(|p| (*p).to_owned()),
            )),
        })
    }

    /// Every call fails authentication (AUD-3004).
    pub fn denying() -> Arc<Self> {
        Arc::new(Self { caller: None })
    }
}

#[cfg(test)]
#[async_trait]
impl CallerGate for StaticCallerGate {
    async fn verify(&self, _bearer: Option<&str>) -> Result<Caller, AuditError> {
        self.caller.clone().ok_or(AuditError::CallerUnauthenticated)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use auth_context::JwksCache;
    use jsonwebtoken::Algorithm;

    fn metadata_with_auth(value: &str) -> MetadataMap {
        let mut m = MetadataMap::new();
        m.insert("authorization", value.parse().unwrap());
        m
    }

    #[tokio::test]
    async fn a_missing_authorization_header_is_unauthenticated() {
        let gate = AuthContextCallerGate::new(unreachable_decoder());
        let err = authorize(&gate, &MetadataMap::new(), perm::READ, AuditError::QueryForbidden)
            .await
            .unwrap_err();
        assert!(matches!(err, AuditError::CallerUnauthenticated));
    }

    #[tokio::test]
    async fn a_non_bearer_scheme_is_unauthenticated() {
        let gate = AuthContextCallerGate::new(unreachable_decoder());
        let meta = metadata_with_auth("Basic dXNlcjpwYXNz");
        let err = authorize(&gate, &meta, perm::READ, AuditError::QueryForbidden)
            .await
            .unwrap_err();
        assert!(matches!(err, AuditError::CallerUnauthenticated));
    }

    #[tokio::test]
    async fn the_bearer_scheme_is_case_insensitive_and_the_token_reaches_the_gate() {
        // A static gate ignores the token; this asserts the scheme parse accepts
        // "bearer" and the permission check then runs.
        let gate = StaticCallerGate::allowing("ops-1", &[perm::READ]);
        let meta = metadata_with_auth("bearer some-token");
        let caller = authorize(gate.as_ref(), &meta, perm::READ, AuditError::QueryForbidden)
            .await
            .unwrap();
        assert_eq!(caller.principal, "ops-1");
    }

    #[tokio::test]
    async fn a_caller_without_the_permission_gets_the_rpc_denial() {
        let gate = StaticCallerGate::allowing("ops-1", &[perm::READ]);
        let meta = metadata_with_auth("Bearer some-token");
        let err = authorize(gate.as_ref(), &meta, perm::EXPORT, AuditError::ExportForbidden)
            .await
            .unwrap_err();
        assert!(matches!(err, AuditError::ExportForbidden));
    }

    #[tokio::test]
    async fn the_deny_all_gate_rejects_even_a_well_formed_bearer() {
        let meta = metadata_with_auth("Bearer some-token");
        let err = authorize(&DenyAllGate, &meta, perm::READ, AuditError::QueryForbidden)
            .await
            .unwrap_err();
        assert!(matches!(err, AuditError::CallerUnauthenticated));
    }

    /// Revoked when `gen` is below 2; `None` = unreadable.
    struct State(Option<()>);

    impl transport::grpc::revocation::RevocationCheck for State {
        fn is_revoked<'a>(&'a self, _: &'a str, _: &'a str, generation: i64) -> transport::grpc::revocation::RevocationLookup<'a> {
            Box::pin(async move { self.0.ok_or(transport::grpc::revocation::RevocationUnavailable).map(|_| generation < 2) })
        }
    }

    /// #903: a token verified by the gate but revoked in `auth` since it was
    /// minted is `AUD-3004`; an unreadable revocation state is `AUD-3006` —
    /// fail closed even when the check was handed an open posture.
    #[tokio::test]
    async fn a_revoked_token_is_unauthenticated_and_an_unreadable_state_fails_closed() {
        use jsonwebtoken::{encode, DecodingKey, EncodingKey, Header};
        let secret = b"audit-gate-test-secret";
        let cache = JwksCache::new();
        cache.replace([("k1".to_owned(), DecodingKey::from_secret(secret))].into_iter().collect()).await;
        let decoder = Arc::new(JwtDecoder::with_algorithms(
            &AuthContextConfig::default(),
            cache,
            OidcClaimsExtractor::platform_edge(),
            vec![Algorithm::HS256],
        ));
        let token = |generation: i64| {
            let mut header = Header::new(Algorithm::HS256);
            header.kid = Some("k1".to_owned());
            let claims = serde_json::json!({
                "sub": "staff-1", "sid": "s-1", "gen": generation,
                "exp": 4_102_444_800_i64, "perms": [perm::READ],
            });
            encode(&header, &claims, &EncodingKey::from_secret(secret)).unwrap()
        };
        let gate = |state| {
            AuthContextCallerGate::new(Arc::clone(&decoder)).with_revocations(Revocations::new(
                Arc::new(State(state)),
                RevocationFailure::Open,
            ))
        };
        let meta = |generation| metadata_with_auth(&format!("Bearer {}", token(generation)));

        let caller = authorize(&gate(Some(())), &meta(2), perm::READ, AuditError::QueryForbidden).await;
        assert!(caller.is_ok(), "a live token reads: {caller:?}");
        let revoked = authorize(&gate(Some(())), &meta(1), perm::READ, AuditError::QueryForbidden).await;
        assert!(matches!(revoked, Err(AuditError::CallerUnauthenticated)));
        let unreadable = authorize(&gate(None), &meta(2), perm::READ, AuditError::QueryForbidden).await;
        assert!(matches!(unreadable, Err(AuditError::CallerCheckUnavailable)));
    }

    /// A decoder whose JWKS cache is empty — any decode attempt fails, which is
    /// fine because these tests never get past the metadata parse.
    fn unreachable_decoder() -> Arc<JwtDecoder<OidcClaims, OidcClaimsExtractor>> {
        let cfg = AuthContextConfig::default();
        Arc::new(JwtDecoder::with_algorithms(
            &cfg,
            JwksCache::new(),
            OidcClaimsExtractor::default(),
            vec![Algorithm::ES256],
        ))
    }
}
