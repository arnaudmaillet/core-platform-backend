use async_trait::async_trait;

use crate::error::AuthError;

/// A credential the caller presents at our boundary, to be brokered to the IdP.
///
/// Deliberately small and IdP-neutral: the adapter translates it into whatever
/// the concrete provider's protocol needs. Mirrors the `auth.v1` `oneof`.
#[derive(Debug, Clone)]
pub enum AuthnGrant {
    /// OIDC authorization-code flow with PKCE.
    AuthorizationCode {
        code: String,
        redirect_uri: String,
        code_verifier: String,
    },
    /// Resource-owner password grant (first-party trusted clients only). The
    /// password is forwarded to the IdP and never stored.
    Password { username: String, password: String },
    /// A native Sign in with Apple / Google id_token. Verified by auth itself
    /// ([`FederatedTokenVerifier`](super::FederatedTokenVerifier)), never sent
    /// to the IdP broker.
    IdToken {
        provider: crate::domain::value_object::FederatedProvider,
        id_token: String,
        nonce: String,
    },
    /// A one-time code sent to the account's email address (passwordless).
    /// Checked by auth itself, never sent to the IdP broker.
    Code { challenge_id: String, code: String },
    /// A passkey assertion (#808). Checked by auth itself, never sent to the
    /// IdP broker.
    Passkey(crate::application::command::PasskeyAssertion),
}

/// The normalized identity an IdP returns after authenticating a grant.
///
/// Only what auth needs to bind a session: the `(issuer, subject)` that keys the
/// [`SubjectLink`](crate::domain::aggregate::SubjectLink). Authorization (roles /
/// permissions) is intentionally absent here — that is the `account` service's
/// concern, resolved via [`AccountDirectory`](super::AccountDirectory).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NormalizedClaims {
    pub issuer: String,
    pub subject: String,
}

/// Outbound port brokering authentication to the external IdP (Keycloak today).
///
/// The single seam behind which a provider swap (Cognito/Okta/custom) is a new
/// adapter with no change above `infrastructure`. Phase 4's adapter adds the
/// concrete OIDC calls; later phases may extend this with IdP-side refresh /
/// revoke, which the federated model does not require on the hot path.
#[async_trait]
pub trait IdentityProvider: Send + Sync + 'static {
    /// Exchanges a grant for a normalized identity, or fails with
    /// [`AuthError::IdpAuthenticationFailed`] / [`AuthError::IdpUnavailable`] /
    /// [`AuthError::ClaimsNormalizationFailed`].
    async fn authenticate(&self, grant: AuthnGrant) -> Result<NormalizedClaims, AuthError>;
}
