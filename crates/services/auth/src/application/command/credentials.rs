//! Shared steps of the credential use-cases (`ChangePassword`,
//! `VerifyCredentials`): resolve the caller's live session, and prove a password
//! for the IdP subject behind it.

use std::sync::Arc;

use chrono::{DateTime, Utc};

use crate::application::port::{
    AuthnGrant, CredentialAdmin, IdentityProvider, SessionCache, SessionRepository,
};
use crate::domain::aggregate::Session;
use crate::domain::value_object::{AccountId, SessionId, SessionKind};
use crate::error::AuthError;

/// How long a credential proof counts as recent for step-up-gated RPCs. Must
/// equal `auth_context::edge::STEP_UP_MAX_AGE_SECS`, which the verifiers use
/// (asserted in `tests/edge_token_verify.rs`); auth only reports it to clients.
pub const STEP_UP_WINDOW_SECS: i64 = 300;

/// The caller's own session, live: it exists, belongs to `account_id`, is a
/// member's, and is still valid under the account's generation and not
/// blacklisted. Anything else reads as signed out.
pub(crate) async fn caller_session(
    sessions: &Arc<dyn SessionRepository>,
    cache: &Arc<dyn SessionCache>,
    account_id: &AccountId,
    session_id: &SessionId,
    now: DateTime<Utc>,
) -> Result<Session, AuthError> {
    let session = sessions
        .find_by_id(session_id)
        .await?
        .filter(|s| s.account_id() == *account_id && s.kind() == SessionKind::Member)
        .ok_or(AuthError::SessionNotFound { id: session_id.as_str() })?;
    let generation = cache.current_generation(account_id).await?;
    if !session.is_valid_under(generation, now) || cache.is_blacklisted(session_id).await? {
        return Err(AuthError::SessionRevoked);
    }
    Ok(session)
}

/// Proves `password` for the IdP subject behind `session`: a password grant
/// under the subject's own login name, which must come back as that subject.
pub(crate) async fn prove_password(
    admin: &Arc<dyn CredentialAdmin>,
    idp: &Arc<dyn IdentityProvider>,
    session: &Session,
    password: String,
) -> Result<(), AuthError> {
    let subject = session.subject();
    let username = admin.login_name(subject).await?;
    let claims = idp.authenticate(AuthnGrant::Password { username, password }).await?;
    if claims.issuer != subject.issuer() || claims.subject != subject.subject() {
        return Err(AuthError::IdpAuthenticationFailed);
    }
    Ok(())
}
