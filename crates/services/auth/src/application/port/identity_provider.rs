/// A credential the caller presents at our boundary. Every one is checked by
/// auth itself — there is no external IdP broker and no password (#881).
/// Mirrors the `auth.v1` `oneof`.
#[derive(Debug, Clone)]
pub enum AuthnGrant {
    /// A native Sign in with Apple / Google id_token, verified against the
    /// provider's published keys
    /// ([`FederatedTokenVerifier`](super::FederatedTokenVerifier)).
    IdToken {
        provider: crate::domain::value_object::FederatedProvider,
        id_token: String,
        nonce: String,
    },
    /// A one-time code sent to the account's email address or phone number.
    Code { challenge_id: String, code: String },
    /// A passkey assertion (#808).
    Passkey(crate::application::command::PasskeyAssertion),
}
