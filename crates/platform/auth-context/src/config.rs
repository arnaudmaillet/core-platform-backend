use std::time::Duration;

/// Runtime configuration for the auth-context layer.
///
/// Build one instance per process and share it (via `Arc<AuthContextConfig>` or
/// by cloning) wherever the [`JwksRefresher`] and [`JwtDecoder`] are constructed.
///
/// [`JwksRefresher`]: crate::JwksRefresher
/// [`JwtDecoder`]: crate::JwtDecoder
#[derive(Debug, Clone)]
pub struct AuthContextConfig {
    /// Full URL of the OIDC JWKS endpoint.
    ///
    /// Examples:
    /// - the platform's `auth`: `http://auth-server:8081/.well-known/jwks.json`
    /// - Auth0:    `https://your-tenant.auth0.com/.well-known/jwks.json`
    /// - Okta:     `https://your-org.okta.com/oauth2/default/v1/keys`
    pub jwks_url: String,

    /// Interval between successful JWKS refreshes.
    ///
    /// Production default: 5 minutes. Key rotation events typically have a
    /// multi-hour propagation window, so this interval is intentionally
    /// conservative to avoid thundering-herd against the IdP.
    pub refresh_interval: Duration,

    /// Maximum backoff applied when the JWKS endpoint is unreachable.
    ///
    /// The refresher starts at 1 s and doubles on each consecutive failure,
    /// capping at this value. Stale keys remain in the cache throughout.
    pub max_backoff: Duration,

    /// Expected `aud` claim value.
    ///
    /// Set to `Some("my-api-resource-identifier")` to enable audience validation.
    /// `None` disables the check — acceptable only when the JWT never leaves an
    /// internal, fully-trusted network boundary.
    pub expected_audience: Option<String>,

    /// Expected `iss` claim value.
    ///
    /// Set to the exact issuer URL advertised in the OIDC discovery document.
    /// `None` disables issuer validation (not recommended for production).
    pub expected_issuer: Option<String>,

    /// Tolerance applied to `exp` and `nbf` timestamp checks.
    ///
    /// Absorbs minor clock drift between the token issuer and this service.
    /// Keep ≤ 60 s in production to avoid issuing meaningful grace windows.
    pub clock_skew: Duration,

    /// Per-request HTTP timeout for JWKS fetch calls.
    pub fetch_timeout: Duration,
}

impl Default for AuthContextConfig {
    fn default() -> Self {
        Self {
            jwks_url: String::new(),
            refresh_interval: Duration::from_secs(300),
            max_backoff: Duration::from_secs(60),
            expected_audience: None,
            expected_issuer: None,
            clock_skew: Duration::from_secs(5),
            fetch_timeout: Duration::from_secs(10),
        }
    }
}
