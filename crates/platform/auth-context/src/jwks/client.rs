use std::collections::HashMap;
use std::path::PathBuf;
use std::time::Duration;

use jsonwebtoken::DecodingKey;
use reqwest::Client;
use serde::Deserialize;

use crate::AuthError;

// ── Wire types ───────────────────────────────────────────────────────────────

#[derive(Deserialize)]
struct JwksResponse {
    keys: Vec<JwkKey>,
}

#[derive(Deserialize)]
struct JwkKey {
    kid: String,
    kty: String,
    // RSA public key components (base64url-encoded, no padding)
    n: Option<String>,
    e: Option<String>,
    // EC public key components
    crv: Option<String>,
    x: Option<String>,
    y: Option<String>,
}

// ── Public API ───────────────────────────────────────────────────────────────

/// Stateless HTTP client that fetches a JWKS document and converts each key
/// into a [`DecodingKey`] ready for use by [`JwtDecoder`].
///
/// A single `JwksClient` instance is shared by the [`JwksRefresher`] background
/// task across its entire lifetime. It carries no mutable state between fetches.
///
/// [`JwtDecoder`]: crate::JwtDecoder
/// [`JwksRefresher`]: crate::JwksRefresher
pub struct JwksClient {
    http: Client,
    url: String,
    /// A bearer token file sent with each fetch, re-read every time (it may
    /// rotate): the in-cluster API server's JWKS asks the caller's own
    /// ServiceAccount token (#852).
    bearer_file: Option<PathBuf>,
}

impl JwksClient {
    /// Constructs a client targeting `url` with the given per-request `timeout`.
    ///
    /// The underlying `reqwest::Client` is built once and reused across all
    /// fetches, sharing the connection pool.
    ///
    /// # Panics
    ///
    /// Panics if the TLS backend cannot be initialised (extremely unlikely in
    /// a correctly linked binary).
    pub fn new(url: impl Into<String>, timeout: Duration) -> Self {
        let http = Client::builder()
            .timeout(timeout)
            .https_only(false) // allow http:// in local/test deployments
            .build()
            .expect("failed to build JWKS HTTP client — TLS backend unavailable");

        Self {
            http,
            url: url.into(),
            bearer_file: None,
        }
    }

    /// A client for a JWKS behind a private CA and a bearer token — the
    /// in-cluster API server's `/openid/v1/jwks` (mesh caller identity,
    /// #852): `ca_file` (PEM) is trusted on top of the system roots, and
    /// `bearer_file` is read and sent with every fetch.
    ///
    /// # Errors
    ///
    /// [`AuthError::JwksUnavailable`] when the CA file cannot be read or
    /// parsed, or the HTTP client cannot be built.
    pub fn with_ca_and_bearer(
        url: impl Into<String>,
        timeout: Duration,
        ca_file: Option<PathBuf>,
        bearer_file: Option<PathBuf>,
    ) -> Result<Self, AuthError> {
        let mut builder = Client::builder().timeout(timeout).https_only(false);
        if let Some(ca_file) = ca_file {
            let pem = std::fs::read(&ca_file)
                .map_err(|e| AuthError::JwksUnavailable(format!("JWKS CA file {}: {e}", ca_file.display())))?;
            let ca = reqwest::Certificate::from_pem(&pem)
                .map_err(|e| AuthError::JwksUnavailable(format!("JWKS CA file {}: {e}", ca_file.display())))?;
            builder = builder.add_root_certificate(ca);
        }
        let http = builder.build().map_err(|e| AuthError::JwksUnavailable(format!("JWKS HTTP client: {e}")))?;
        Ok(Self { http, url: url.into(), bearer_file })
    }

    /// Fetches the JWKS document and returns a map of `kid → DecodingKey`.
    ///
    /// Keys whose `kty` is unsupported (e.g. `oct`, `OKP`) are skipped with a
    /// `WARN` log rather than causing the entire fetch to fail. This tolerates
    /// mixed-type JWKS responses from providers that publish both signing and
    /// encryption keys in the same set.
    ///
    /// # Errors
    ///
    /// Returns [`AuthError::JwksUnavailable`] on any HTTP or parse failure.
    pub async fn fetch(&self) -> Result<HashMap<String, DecodingKey>, AuthError> {
        let mut request = self.http.get(&self.url);
        if let Some(bearer_file) = &self.bearer_file {
            // Off the async worker: a filesystem read, however small.
            let path = bearer_file.clone();
            let token = tokio::task::spawn_blocking(move || std::fs::read_to_string(path))
                .await
                .map_err(|e| AuthError::JwksUnavailable(format!("JWKS bearer file: {e}")))?
                .map_err(|e| AuthError::JwksUnavailable(format!("JWKS bearer file {}: {e}", bearer_file.display())))?;
            request = request.bearer_auth(token.trim());
        }
        let response = request.send().await.map_err(|e| AuthError::JwksUnavailable(e.to_string()))?;

        let status = response.status();
        if !status.is_success() {
            return Err(AuthError::JwksUnavailable(format!(
                "JWKS endpoint returned HTTP {status}"
            )));
        }

        let body: JwksResponse = response
            .json()
            .await
            .map_err(|e| AuthError::JwksUnavailable(format!("JWKS parse error: {e}")))?;

        let mut keys: HashMap<String, DecodingKey> = HashMap::with_capacity(body.keys.len());

        for jwk in body.keys {
            match Self::build_decoding_key(&jwk) {
                Ok(key) => {
                    tracing::debug!(kid = %jwk.kid, kty = %jwk.kty, "JWKS key loaded");
                    keys.insert(jwk.kid, key);
                }
                Err(reason) => {
                    tracing::warn!(kid = %jwk.kid, kty = %jwk.kty, %reason, "skipping undecodable JWKS key");
                }
            }
        }

        Ok(keys)
    }

    fn build_decoding_key(jwk: &JwkKey) -> Result<DecodingKey, String> {
        match jwk.kty.as_str() {
            "RSA" => {
                let n = jwk.n.as_deref().ok_or("RSA key missing 'n'")?;
                let e = jwk.e.as_deref().ok_or("RSA key missing 'e'")?;
                DecodingKey::from_rsa_components(n, e)
                    .map_err(|e| format!("RSA key construction failed: {e}"))
            }

            "EC" => {
                let _crv = jwk.crv.as_deref().unwrap_or("P-256");
                let x = jwk.x.as_deref().ok_or("EC key missing 'x'")?;
                let y = jwk.y.as_deref().ok_or("EC key missing 'y'")?;
                DecodingKey::from_ec_components(x, y)
                    .map_err(|e| format!("EC key construction failed: {e}"))
            }

            kty => Err(format!("unsupported key type: {kty}")),
        }
    }
}
