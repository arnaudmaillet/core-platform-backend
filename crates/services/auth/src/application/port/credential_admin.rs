use async_trait::async_trait;

use crate::domain::value_object::IdpSubject;
use crate::error::AuthError;

/// Outbound port to the IdP's **credential management** (the Keycloak Admin
/// API): what auth needs to change a password the holder signs in with.
///
/// The password itself lives at the IdP, never in auth or account: proving the
/// current one goes through [`IdentityProvider::authenticate`](super::IdentityProvider::authenticate),
/// setting the new one through here.
#[async_trait]
pub trait CredentialAdmin: Send + Sync + 'static {
    /// The login name the IdP knows `subject` by — what a password grant for
    /// that subject must present.
    async fn login_name(&self, subject: &IdpSubject) -> Result<String, AuthError>;

    /// Sets `subject`'s password. [`AuthError::PasswordRejected`] when the IdP's
    /// password policy refuses it.
    async fn set_password(&self, subject: &IdpSubject, new_password: &str) -> Result<(), AuthError>;

    /// Deletes `subject`'s IdP user (GDPR erasure: its email, username and
    /// password hash). Idempotent: an unknown user is `Ok`.
    async fn delete_user(&self, subject: &IdpSubject) -> Result<(), AuthError>;
}
