use async_trait::async_trait;
use base64::Engine;
use serde::Deserialize;

use crate::application::port::{AuthnGrant, IdentityProvider, NormalizedClaims};
use crate::error::AuthError;

/// Connection + client-credential config for the Keycloak OIDC token endpoint.
#[derive(Debug, Clone)]
pub struct KeycloakConfig {
    /// Full token endpoint, e.g. `https://idp/realms/app/protocol/openid-connect/token`.
    pub token_endpoint: String,
    pub client_id: String,
    pub client_secret: String,
    /// OIDC scope requested; must include `openid` so the response carries `sub`.
    pub scope: String,
}

impl Default for KeycloakConfig {
    fn default() -> Self {
        Self {
            token_endpoint: String::new(),
            client_id: String::new(),
            client_secret: String::new(),
            scope: "openid".to_owned(),
        }
    }
}

/// Keycloak implementation of [`IdentityProvider`].
///
/// Brokers the grant to the OIDC token endpoint over TLS and normalizes the
/// returned identity. The access token is trusted as transport-authenticated (it
/// came directly from the IdP over TLS); its `iss`/`sub` are read from the
/// payload without a second signature check. Edge tokens this service *mints* are,
/// of course, signature-verified by downstream services.
pub struct KeycloakIdentityProvider {
    http: reqwest::Client,
    config: KeycloakConfig,
}

impl KeycloakIdentityProvider {
    pub fn new(http: reqwest::Client, config: KeycloakConfig) -> Self {
        Self { http, config }
    }
}

#[derive(Debug, Deserialize)]
struct TokenResponse {
    access_token: String,
}

/// The two claims auth needs from the IdP token.
#[derive(Debug, Deserialize)]
struct IdentityClaims {
    iss: String,
    sub: String,
}

/// Reads `iss`/`sub` from a JWT payload without verifying the signature.
fn extract_identity(token: &str) -> Result<IdentityClaims, AuthError> {
    let payload = token
        .split('.')
        .nth(1)
        .ok_or_else(|| AuthError::ClaimsNormalizationFailed("token is not a JWT".into()))?;
    let bytes = base64::engine::general_purpose::URL_SAFE_NO_PAD
        .decode(payload)
        .map_err(|e| AuthError::ClaimsNormalizationFailed(format!("base64: {e}")))?;
    serde_json::from_slice::<IdentityClaims>(&bytes)
        .map_err(|e| AuthError::ClaimsNormalizationFailed(format!("claims: {e}")))
}

#[async_trait]
impl IdentityProvider for KeycloakIdentityProvider {
    async fn authenticate(&self, grant: AuthnGrant) -> Result<NormalizedClaims, AuthError> {
        // Common confidential-client + scope params.
        let mut form: Vec<(&str, String)> = vec![
            ("client_id", self.config.client_id.clone()),
            ("client_secret", self.config.client_secret.clone()),
            ("scope", self.config.scope.clone()),
        ];
        match grant {
            AuthnGrant::AuthorizationCode { code, redirect_uri, code_verifier } => {
                form.push(("grant_type", "authorization_code".to_owned()));
                form.push(("code", code));
                form.push(("redirect_uri", redirect_uri));
                form.push(("code_verifier", code_verifier));
            }
            AuthnGrant::Password { username, password } => {
                form.push(("grant_type", "password".to_owned()));
                form.push(("username", username));
                form.push(("password", password));
            }
            // Verified by auth itself (FederatedTokenVerifier), never brokered.
            AuthnGrant::IdToken { .. } | AuthnGrant::Code { .. } | AuthnGrant::Passkey(_) => {
                return Err(AuthError::IdpAuthenticationFailed);
            }
        }

        let response = self
            .http
            .post(&self.config.token_endpoint)
            .form(&form)
            .send()
            .await
            .map_err(|_| AuthError::IdpUnavailable)?;

        if !response.status().is_success() {
            // 4xx ⇒ bad credentials / invalid grant; 5xx ⇒ IdP trouble.
            return Err(if response.status().is_server_error() {
                AuthError::IdpUnavailable
            } else {
                AuthError::IdpAuthenticationFailed
            });
        }

        let body: TokenResponse = response
            .json()
            .await
            .map_err(|e| AuthError::ClaimsNormalizationFailed(format!("token response: {e}")))?;

        let claims = extract_identity(&body.access_token)?;
        Ok(NormalizedClaims { issuer: claims.iss, subject: claims.sub })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use base64::Engine;

    fn jwt_with(payload: &str) -> String {
        let b64 = |s: &str| base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(s.as_bytes());
        format!("{}.{}.{}", b64("{\"alg\":\"RS256\"}"), b64(payload), "sig")
    }

    #[test]
    fn extracts_iss_and_sub() {
        let token = jwt_with("{\"iss\":\"https://idp/realms/app\",\"sub\":\"user-1\"}");
        let claims = extract_identity(&token).unwrap();
        assert_eq!(claims.iss, "https://idp/realms/app");
        assert_eq!(claims.sub, "user-1");
    }

    #[test]
    fn rejects_non_jwt() {
        assert!(matches!(
            extract_identity("not-a-jwt").unwrap_err(),
            AuthError::ClaimsNormalizationFailed(_)
        ));
    }

    #[test]
    fn rejects_payload_without_claims() {
        let token = jwt_with("{\"foo\":\"bar\"}");
        assert!(matches!(
            extract_identity(&token).unwrap_err(),
            AuthError::ClaimsNormalizationFailed(_)
        ));
    }
}
