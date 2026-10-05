//! Server-issued, single-use nonces for native Sign in with Apple / Google.
//!
//! A client-generated nonce only proves the token was minted for *some* sign-in
//! of this app: a stolen id_token could be replayed until it expires. With
//! `StartFederatedSignIn` the server mints the nonce, and SignUp / Login redeem
//! it once (the token's `nonce` claim must match it, as before). While clients
//! roll out, an unknown nonce is only logged (`required = false`); once they all
//! call StartFederatedSignIn, `AUTH_FEDERATED_NONCE_REQUIRED` refuses it.

use std::sync::Arc;

use async_trait::async_trait;
use base64::Engine;
use chrono::Duration;
use rand::RngCore;
use sha2::{Digest, Sha256};

use crate::application::port::{FederatedIdentity, FederatedNonceStore, FederatedTokenVerifier};
use crate::domain::value_object::FederatedProvider;
use crate::error::AuthError;

/// How long a nonce may wait for its SignUp / Login.
pub const FEDERATED_NONCE_TTL_SECS: i64 = 600;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StartedFederatedSignIn {
    /// Raw, base64url without padding (32 random bytes).
    pub nonce:           String,
    pub expires_in_secs: i64,
}

fn nonce_hash(raw: &str) -> String {
    Sha256::digest(raw.trim().as_bytes()).iter().map(|b| format!("{b:02x}")).collect()
}

/// Issues and redeems sign-in nonces.
pub struct FederatedNonces {
    store:    Arc<dyn FederatedNonceStore>,
    required: bool,
}

impl FederatedNonces {
    pub fn new(store: Arc<dyn FederatedNonceStore>, required: bool) -> Self {
        Self { store, required }
    }

    pub async fn start(&self) -> Result<StartedFederatedSignIn, AuthError> {
        let mut bytes = [0u8; 32];
        rand::rng().fill_bytes(&mut bytes);
        let nonce = base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(bytes);
        self.store.issue(&nonce_hash(&nonce), Duration::seconds(FEDERATED_NONCE_TTL_SECS)).await?;
        Ok(StartedFederatedSignIn { nonce, expires_in_secs: FEDERATED_NONCE_TTL_SECS })
    }

    /// Consumes `raw`. An unknown, expired or used nonce is refused when
    /// required, else logged (client-generated nonces during the rollout).
    pub async fn redeem(&self, raw: &str) -> Result<(), AuthError> {
        if self.store.consume(&nonce_hash(raw)).await? {
            return Ok(());
        }
        if self.required {
            return Err(AuthError::IdTokenRejected { reason: "unknown or already used nonce".into() });
        }
        tracing::warn!("id_token with a nonce the server did not issue (or already used): accepted until AUTH_FEDERATED_NONCE_REQUIRED");
        Ok(())
    }
}

/// A [`FederatedTokenVerifier`] that also redeems the nonce, once the token
/// itself verified (a forged token never burns a nonce).
pub struct NonceBoundVerifier {
    inner:  Arc<dyn FederatedTokenVerifier>,
    nonces: Arc<FederatedNonces>,
}

impl NonceBoundVerifier {
    pub fn new(inner: Arc<dyn FederatedTokenVerifier>, nonces: Arc<FederatedNonces>) -> Self {
        Self { inner, nonces }
    }
}

#[async_trait]
impl FederatedTokenVerifier for NonceBoundVerifier {
    async fn verify(
        &self,
        provider: FederatedProvider,
        id_token: &str,
        nonce: &str,
    ) -> Result<FederatedIdentity, AuthError> {
        let identity = self.inner.verify(provider, id_token, nonce).await?;
        self.nonces.redeem(nonce).await?;
        Ok(identity)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::application::fakes::InMemoryNonceStore;

    /// Accepts any token, as if signature, audience, expiry and nonce matched.
    struct AnyToken;

    #[async_trait]
    impl FederatedTokenVerifier for AnyToken {
        async fn verify(&self, provider: FederatedProvider, id_token: &str, _: &str) -> Result<FederatedIdentity, AuthError> {
            if id_token == "forged" {
                return Err(AuthError::IdTokenRejected { reason: "bad signature".into() });
            }
            Ok(FederatedIdentity {
                provider,
                issuer: "https://appleid.apple.com".into(),
                subject: "s".into(),
                email: None,
                email_verified: false,
                private_relay: false,
            })
        }
    }

    fn verifier(required: bool) -> (Arc<FederatedNonces>, NonceBoundVerifier) {
        let nonces = Arc::new(FederatedNonces::new(Arc::new(InMemoryNonceStore::default()), required));
        (Arc::clone(&nonces), NonceBoundVerifier::new(Arc::new(AnyToken), nonces))
    }

    #[tokio::test]
    async fn an_issued_nonce_signs_in_once() {
        let (nonces, v) = verifier(true);
        let started = nonces.start().await.unwrap();
        assert_eq!(started.nonce.len(), 43, "32 bytes, base64url without padding");
        assert_eq!(started.expires_in_secs, FEDERATED_NONCE_TTL_SECS);

        assert!(v.verify(FederatedProvider::Apple, "token", &started.nonce).await.is_ok());
        // The same id_token (and nonce) replayed.
        assert!(matches!(
            v.verify(FederatedProvider::Apple, "token", &started.nonce).await,
            Err(AuthError::IdTokenRejected { .. })
        ));
        // A nonce the server never issued.
        assert!(v.verify(FederatedProvider::Google, "token", "client-made").await.is_err());
    }

    #[tokio::test]
    async fn a_forged_token_does_not_burn_the_nonce() {
        let (nonces, v) = verifier(true);
        let started = nonces.start().await.unwrap();
        assert!(v.verify(FederatedProvider::Apple, "forged", &started.nonce).await.is_err());
        assert!(v.verify(FederatedProvider::Apple, "token", &started.nonce).await.is_ok());
    }

    #[tokio::test]
    async fn until_required_a_client_nonce_is_only_logged() {
        let (_, v) = verifier(false);
        assert!(v.verify(FederatedProvider::Apple, "token", "client-made").await.is_ok());
    }
}
