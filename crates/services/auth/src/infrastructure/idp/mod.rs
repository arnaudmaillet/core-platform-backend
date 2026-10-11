//! Third-party identity verification: native Sign in with Apple / Google
//! id_tokens, checked here against the provider's published keys. There is no
//! external IdP broker any more (#881): every credential is checked by auth.

pub mod federated_token_verifier;

pub use federated_token_verifier::{JwksFederatedTokenVerifier, ProviderKeys};
