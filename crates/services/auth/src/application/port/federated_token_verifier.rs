use async_trait::async_trait;

use crate::domain::value_object::FederatedProvider;
use crate::error::AuthError;

/// What a verified Apple / Google id_token says about the person.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FederatedIdentity {
    pub provider:       FederatedProvider,
    /// The token's `iss` (keys the subject link with `subject`).
    pub issuer:         String,
    pub subject:        String,
    pub email:          Option<String>,
    /// The provider vouches for the address.
    pub email_verified: bool,
    /// An Apple "Hide My Email" relay address: never matched to another account.
    pub private_relay:  bool,
}

/// Verifies native Sign in with Apple / Google id_tokens against the provider's
/// published keys: signature, issuer, audience (the app's client ids), expiry and
/// nonce. No call to the provider beyond fetching its keys.
#[async_trait]
pub trait FederatedTokenVerifier: Send + Sync + 'static {
    /// Errors: [`AuthError::FederatedProviderNotConfigured`] (no client id set for
    /// the provider), [`AuthError::IdTokenRejected`] (anything wrong with the token),
    /// [`AuthError::IdpUnavailable`] (keys unreachable).
    async fn verify(
        &self,
        provider: FederatedProvider,
        id_token: &str,
        nonce: &str,
    ) -> Result<FederatedIdentity, AuthError>;
}
