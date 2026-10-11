use serde::{Deserialize, Serialize};
use std::fmt;

/// Opaque, platform-canonical user identifier.
///
/// The raw value is the `sub` claim string as received from the IdP.
/// Subs come in many shapes (`google-oauth2|123456`, UUID strings, the
/// platform's account ids). Keep it as a `String` so both layouts work without a lossy
/// coercion step at the boundary. Services that require a [`uuid::Uuid`]
/// internally should call [`PrincipalId::try_as_uuid`].
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct PrincipalId(pub String);

impl PrincipalId {
    pub fn new(value: impl Into<String>) -> Self {
        Self(value.into())
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }

    /// Attempts to parse the inner value as a [`uuid::Uuid`].
    ///
    /// Returns `None` when the IdP uses a non-UUID subject format (Auth0,
    /// federated providers, etc.).
    pub fn try_as_uuid(&self) -> Option<uuid::Uuid> {
        self.0.parse().ok()
    }
}

impl fmt::Display for PrincipalId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

impl From<String> for PrincipalId {
    fn from(s: String) -> Self {
        Self(s)
    }
}

impl From<&str> for PrincipalId {
    fn from(s: &str) -> Self {
        Self(s.to_owned())
    }
}

/// A single, normalised platform permission token.
///
/// The string is opaque to the auth layer — business meaning is defined by
/// the consuming service. Examples: `"posts:write"`, `"admin"`, `"ROLE_USER"`.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct Permission(pub String);

impl Permission {
    pub fn new(value: impl Into<String>) -> Self {
        Self(value.into())
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for Permission {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

impl From<String> for Permission {
    fn from(s: String) -> Self {
        Self(s)
    }
}

impl From<&str> for Permission {
    fn from(s: &str) -> Self {
        Self(s.to_owned())
    }
}

/// The platform-canonical, strongly-typed identity extracted from a verified JWT.
///
/// `C` is the provider-specific raw claims struct (e.g. [`crate::OidcClaims`]).
/// It is preserved verbatim so that services requiring non-standard fields do
/// not need an additional decode pass.
///
/// # Thread safety
///
/// `CurrentPrincipal<C>` is `Send + Sync` whenever `C: Send + Sync`, which is
/// true for all generated claim structs. It is safe to share via `Arc` across
/// async task boundaries.
#[derive(Debug, Clone)]
pub struct CurrentPrincipal<C> {
    /// Platform-canonical user identifier, derived from the `sub` JWT claim.
    pub user_id: PrincipalId,

    /// Optional tenant scope for multi-tenant deployments.
    ///
    /// Populated from a configurable claim key (default: `tid`) if present.
    pub tenant_id: Option<String>,

    /// Unified, deduplicated permission set produced by [`ClaimsExtractor`].
    ///
    /// All authorisation checks in business logic must operate exclusively on
    /// this normalised set, never on `raw_claims` directly.
    ///
    /// [`ClaimsExtractor`]: crate::ClaimsExtractor
    pub permissions: Vec<Permission>,

    /// The raw decoded claims, exactly as they came out of the JWT payload.
    pub raw_claims: C,
}

impl<C: Send + Sync + 'static> CurrentPrincipal<C> {
    /// Returns `true` if `permission` is present in the normalised set.
    pub fn has_permission(&self, permission: &str) -> bool {
        self.permissions.iter().any(|p| p.0 == permission)
    }

    /// Returns `true` if all listed permissions are present.
    pub fn has_all_permissions(&self, required: &[&str]) -> bool {
        required.iter().all(|r| self.has_permission(r))
    }

    /// Returns `true` if at least one of the listed permissions is present.
    pub fn has_any_permission(&self, candidates: &[&str]) -> bool {
        candidates.iter().any(|c| self.has_permission(c))
    }
}
