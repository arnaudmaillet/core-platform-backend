use std::collections::{HashMap, HashSet};

use serde::{Deserialize, Serialize};

use crate::{AuthError, ClaimsExtractor, CurrentPrincipal, Permission, PrincipalId};

// ── Raw claim types ──────────────────────────────────────────────────────────

/// Standard OIDC JWT payload, extended to cover the proprietary role/scope
/// layouts in common use (`scope`, `realm_access.roles`, `permissions`).
///
/// Unknown claims fall through into [`extra`][OidcClaims::extra] via `#[serde(flatten)]`
/// so no information is silently dropped.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OidcClaims {
    /// Subject — the unique user identifier issued by the IdP.
    pub sub: String,

    /// Issuer URL.
    pub iss: Option<String>,

    /// Audience — a single string or a JSON array of strings.
    pub aud: Option<serde_json::Value>,

    /// Expiration timestamp (Unix seconds). Required by RFC 7519.
    pub exp: i64,

    /// Not-before timestamp (Unix seconds).
    pub nbf: Option<i64>,

    /// Issued-at timestamp (Unix seconds).
    pub iat: Option<i64>,

    /// JWT ID — unique identifier for this specific token instance.
    pub jti: Option<String>,

    /// Space-separated scope string (standard OIDC and Auth0).
    pub scope: Option<String>,

    /// Realm-level roles (`realm_access.roles` layout).
    pub realm_access: Option<RealmAccess>,

    /// Per-resource (client) roles, keyed by client ID (`resource_access` layout).
    pub resource_access: Option<HashMap<String, RealmAccess>>,

    /// Auth0-style flat permissions array.
    pub permissions: Option<Vec<String>>,

    /// Okta / generic groups array.
    pub groups: Option<Vec<String>>,

    /// Tenant identifier — provider-specific claim key, resolved at runtime by
    /// [`OidcExtractorConfig::tenant_id_claim`].
    pub tid: Option<String>,

    /// Catch-all for provider-specific or custom claims not listed above.
    #[serde(flatten)]
    pub extra: HashMap<String, serde_json::Value>,
}

/// A realm or resource access block containing a `roles` array.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RealmAccess {
    pub roles: Option<Vec<String>>,
}

// ── Extractor configuration ──────────────────────────────────────────────────

/// Which JWT fields the [`OidcClaimsExtractor`] harvests permissions from.
///
/// Sources are evaluated in declaration order; duplicates are removed while
/// preserving first-seen insertion order.
#[derive(Debug, Clone)]
pub enum RoleSource {
    /// Space-separated `scope` claim (standard OIDC, Auth0).
    Scope,

    /// `realm_access.roles` array.
    RealmAccessRoles,

    /// `permissions` array (Auth0).
    PermissionsClaim,

    /// `groups` array (Okta, Azure AD, custom providers).
    Groups,

    /// Arbitrary top-level claim containing a JSON string array.
    Custom(String),
}

/// Configuration for [`OidcClaimsExtractor`].
#[derive(Debug, Clone)]
pub struct OidcExtractorConfig {
    /// Top-level claim key used to resolve the tenant identifier.
    ///
    /// The extractor first looks in [`OidcClaims::extra`] under this key, then
    /// falls back to the typed [`OidcClaims::tid`] field.
    /// Default: `"tid"`.
    pub tenant_id_claim: String,

    /// Ordered list of sources from which permissions are aggregated.
    ///
    /// Default: `[Scope, RealmAccessRoles, PermissionsClaim]` — covers standard
    /// OIDC scopes, `realm_access.roles` and a flat `permissions` array without any
    /// configuration change.
    pub role_sources: Vec<RoleSource>,
}

impl Default for OidcExtractorConfig {
    fn default() -> Self {
        Self {
            tenant_id_claim: "tid".to_owned(),
            role_sources: vec![
                RoleSource::Scope,
                RoleSource::RealmAccessRoles,
                RoleSource::PermissionsClaim,
            ],
        }
    }
}

/// The claim the platform's `auth` service mints its normalised permissions into
/// (`EdgeClaims::perms` in `crates/services/auth`). Not a standard OIDC claim, so
/// the [`Default`] extractor config never reads it — use
/// [`OidcExtractorConfig::platform_edge`] for edge tokens.
pub const EDGE_PERMISSIONS_CLAIM: &str = "perms";

impl OidcExtractorConfig {
    /// Extractor config for the platform's own ES256 **edge tokens** (minted by
    /// `auth`, ADR-0005): `sub` is the internal account id and permissions live in
    /// the [`EDGE_PERMISSIONS_CLAIM`] array. The standard sources are kept after it
    /// so a token from a federated IdP still yields its scopes.
    pub fn platform_edge() -> Self {
        Self {
            tenant_id_claim: "tid".to_owned(),
            role_sources: vec![
                RoleSource::Custom(EDGE_PERMISSIONS_CLAIM.to_owned()),
                RoleSource::Scope,
                RoleSource::PermissionsClaim,
            ],
        }
    }
}

// ── Extractor implementation ─────────────────────────────────────────────────

/// Default [`ClaimsExtractor`] for standard OIDC-compliant providers.
///
/// Supports the common claim layouts (`scope`, `realm_access.roles`, a flat
/// `permissions` array) and any custom issuer that follows the OIDC
/// core specification. The permission aggregation strategy is fully configurable
/// via [`OidcExtractorConfig`].
///
/// ## Permission deduplication
///
/// Permissions are collected from all configured [`RoleSource`] entries and
/// deduplicated (preserving first-seen order) before being stored in
/// [`CurrentPrincipal::permissions`].
pub struct OidcClaimsExtractor {
    config: OidcExtractorConfig,
}

impl OidcClaimsExtractor {
    pub fn new(config: OidcExtractorConfig) -> Self {
        Self { config }
    }
}

impl Default for OidcClaimsExtractor {
    fn default() -> Self {
        Self::new(OidcExtractorConfig::default())
    }
}

impl OidcClaimsExtractor {
    /// The extractor for the platform's edge tokens — see
    /// [`OidcExtractorConfig::platform_edge`].
    pub fn platform_edge() -> Self {
        Self::new(OidcExtractorConfig::platform_edge())
    }
}

impl ClaimsExtractor<OidcClaims> for OidcClaimsExtractor {
    fn extract(&self, raw: OidcClaims) -> Result<CurrentPrincipal<OidcClaims>, AuthError> {
        if raw.sub.is_empty() {
            return Err(AuthError::ClaimsExtractionFailed(
                "JWT 'sub' claim is absent or empty".to_owned(),
            ));
        }

        let user_id = PrincipalId::new(&raw.sub);

        let tenant_id = raw
            .extra
            .get(&self.config.tenant_id_claim)
            .and_then(|v| v.as_str())
            .map(str::to_owned)
            .or_else(|| raw.tid.clone());

        let mut permissions: Vec<Permission> = Vec::new();
        let mut seen: HashSet<String> = HashSet::new();

        let mut push = |value: &str| {
            if seen.insert(value.to_owned()) {
                permissions.push(Permission::new(value));
            }
        };

        for source in &self.config.role_sources {
            match source {
                RoleSource::Scope => {
                    if let Some(ref scope) = raw.scope {
                        for token in scope.split_whitespace() {
                            push(token);
                        }
                    }
                }

                RoleSource::RealmAccessRoles => {
                    if let Some(ref realm) = raw.realm_access
                        && let Some(ref roles) = realm.roles {
                            for role in roles {
                                push(role);
                            }
                        }
                }

                RoleSource::PermissionsClaim => {
                    if let Some(ref perms) = raw.permissions {
                        for perm in perms {
                            push(perm);
                        }
                    }
                }

                RoleSource::Groups => {
                    if let Some(ref groups) = raw.groups {
                        for group in groups {
                            push(group);
                        }
                    }
                }

                RoleSource::Custom(key) => {
                    if let Some(val) = raw.extra.get(key)
                        && let Some(arr) = val.as_array() {
                            for item in arr.iter().filter_map(|v| v.as_str()) {
                                push(item);
                            }
                        }
                }
            }
        }

        Ok(CurrentPrincipal {
            user_id,
            tenant_id,
            permissions,
            raw_claims: raw,
        })
    }
}
