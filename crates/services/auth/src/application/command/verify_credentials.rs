use std::sync::Arc;

use chrono::{DateTime, Utc};
use cqrs::Envelope;
use validate_core::{FieldViolation, Validate};

use super::credentials::{caller_session, prove_password, STEP_UP_WINDOW_SECS};
use crate::application::ensure_valid;
use crate::application::policy::SessionPolicy;
use crate::application::port::{
    profile_ids_or_empty, AccountActivation, AccountDirectory, CredentialAdmin, IdentityProvider,
    ProfileDirectory, SessionCache, SessionRepository, TokenMinter,
};
use crate::domain::value_object::{AccountId, Permission, SessionId};
use crate::error::AuthError;

/// What the holder proves to step up.
#[derive(Clone)]
pub enum StepUpCredential {
    Password(String),
    /// A TOTP or backup code — once MFA enrolment exists (#649).
    MfaCode(String),
}

/// The holder re-proves a credential before a destructive action. `account_id`
/// and `session_id` are the caller's, from the verified token.
#[derive(Clone)]
pub struct VerifyCredentialsCommand {
    pub account_id: String,
    pub session_id: String,
    pub credential: StepUpCredential,
}

impl std::fmt::Debug for VerifyCredentialsCommand {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("VerifyCredentialsCommand")
            .field("account_id", &self.account_id)
            .field("session_id", &self.session_id)
            .finish_non_exhaustive()
    }
}

impl Validate for VerifyCredentialsCommand {
    fn validate(&self) -> Result<(), Vec<FieldViolation>> {
        let mut v = Vec::new();
        if self.account_id.trim().is_empty() || self.session_id.trim().is_empty() {
            v.push(FieldViolation::new(
                "session",
                "AUT-VAL-022",
                "verifying credentials needs the caller's session",
            ));
        }
        let empty = match &self.credential {
            StepUpCredential::Password(p) | StepUpCredential::MfaCode(p) => p.is_empty(),
        };
        if empty {
            v.push(FieldViolation::new("credential", "AUT-VAL-027", "a credential is required"));
        }
        if v.is_empty() { Ok(()) } else { Err(v) }
    }
}

/// A fresh access token for the caller's session carrying `auth_time = now`.
#[derive(Debug, Clone)]
pub struct SteppedUpToken {
    pub access_token: String,
    pub access_expires_in: i64,
    /// How long step-up-gated RPCs accept the proof.
    pub step_up_expires_in: i64,
}

/// Re-proves a credential with the IdP and re-mints the caller's access token
/// with `auth_time = now`, which step-up-gated RPCs elsewhere require to be
/// recent. The session and its refresh token are unchanged.
pub struct VerifyCredentialsHandler {
    idp: Arc<dyn IdentityProvider>,
    admin: Arc<dyn CredentialAdmin>,
    directory: Arc<dyn AccountDirectory>,
    profiles: Arc<dyn ProfileDirectory>,
    sessions: Arc<dyn SessionRepository>,
    cache: Arc<dyn SessionCache>,
    minter: Arc<dyn TokenMinter>,
    policy: SessionPolicy,
}

impl VerifyCredentialsHandler {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        idp: Arc<dyn IdentityProvider>,
        admin: Arc<dyn CredentialAdmin>,
        directory: Arc<dyn AccountDirectory>,
        profiles: Arc<dyn ProfileDirectory>,
        sessions: Arc<dyn SessionRepository>,
        cache: Arc<dyn SessionCache>,
        minter: Arc<dyn TokenMinter>,
        policy: SessionPolicy,
    ) -> Self {
        Self { idp, admin, directory, profiles, sessions, cache, minter, policy }
    }

    pub async fn handle(
        &self,
        envelope: Envelope<VerifyCredentialsCommand>,
        now: DateTime<Utc>,
    ) -> Result<SteppedUpToken, AuthError> {
        ensure_valid(&envelope.payload)?;
        let cmd = envelope.payload;
        let account_id = AccountId::try_from(cmd.account_id.as_str())?;
        let session_id = SessionId::try_from(cmd.session_id.as_str())?;

        let session = caller_session(&self.sessions, &self.cache, &account_id, &session_id, now).await?;
        match cmd.credential {
            StepUpCredential::Password(password) => {
                prove_password(&self.admin, &self.idp, &session, password).await?;
            }
            // No MFA enrolment exists yet (#649).
            StepUpCredential::MfaCode(_) => return Err(AuthError::VerificationMethodUnavailable),
        }

        // Re-read the authoritative grants, as a refresh would.
        let snapshot = self.directory.lookup(&account_id).await?;
        let permissions = match snapshot.activation {
            AccountActivation::Active => Permission::with_read_public(snapshot.permissions),
            AccountActivation::Deactivated => {
                return Err(AuthError::AccountNotActive { current: "deactivated".into() });
            }
            AccountActivation::Inactive { reason } => {
                return Err(AuthError::AccountNotActive { current: reason });
            }
        };
        let profile_ids = profile_ids_or_empty(&self.profiles, &account_id).await;
        let mut claims =
            session.mint_access_token(now, self.policy.access_ttl, permissions, profile_ids)?;
        claims.auth_time = Some(now);
        let access_token = self.minter.mint_access(&claims).await?;

        let access_expires_in = claims.expires_in_secs(now);
        Ok(SteppedUpToken {
            access_token,
            access_expires_in,
            step_up_expires_in: STEP_UP_WINDOW_SECS.min(access_expires_in),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::application::command::{IssuedSession, LoginCommand};
    use crate::application::fakes::{t0, Fixture};
    use crate::application::port::AuthnGrant;
    use crate::domain::value_object::DeviceFingerprint;
    use chrono::Duration;
    use uuid::Uuid;

    async fn login(fx: &Fixture) -> IssuedSession {
        let cmd = LoginCommand {
            grant: AuthnGrant::Password { username: "user".into(), password: "secret-1".into() },
            device: DeviceFingerprint::default(),
        };
        fx.login_handler().handle(Envelope::new(Uuid::now_v7(), cmd), t0()).await.unwrap()
    }

    fn verify(on: &IssuedSession, credential: StepUpCredential) -> Envelope<VerifyCredentialsCommand> {
        Envelope::new(
            Uuid::now_v7(),
            VerifyCredentialsCommand {
                account_id: on.account_id.as_str(),
                session_id: on.session_id.as_str(),
                credential,
            },
        )
    }

    #[tokio::test]
    async fn a_proved_password_re_mints_this_sessions_token_with_auth_time_now() {
        let fx = Fixture::new();
        fx.idp.with_password("secret-1");
        let session = login(&fx).await;
        let later = t0() + Duration::minutes(7);

        let token = fx
            .verify_credentials_handler()
            .handle(verify(&session, StepUpCredential::Password("secret-1".into())), later)
            .await
            .unwrap();

        let claims = fx.minter.verify_access(&token.access_token).await.unwrap();
        assert_eq!(claims.session_id, session.session_id);
        assert_eq!(claims.auth_time, Some(later));
        assert_eq!(token.step_up_expires_in, STEP_UP_WINDOW_SECS);
    }

    #[tokio::test]
    async fn a_wrong_password_or_an_unenrolled_mfa_code_proves_nothing() {
        let fx = Fixture::new();
        fx.idp.with_password("secret-1");
        let session = login(&fx).await;
        let handler = fx.verify_credentials_handler();

        let err = handler
            .handle(verify(&session, StepUpCredential::Password("nope".into())), t0())
            .await
            .unwrap_err();
        assert!(matches!(err, AuthError::IdpAuthenticationFailed));
        let err = handler
            .handle(verify(&session, StepUpCredential::MfaCode("123456".into())), t0())
            .await
            .unwrap_err();
        assert!(matches!(err, AuthError::VerificationMethodUnavailable));
    }

    #[tokio::test]
    async fn login_proves_a_credential_and_refresh_does_not() {
        let fx = Fixture::new();
        let session = login(&fx).await;
        let claims = fx.minter.verify_access(&session.access_token).await.unwrap();
        assert_eq!(claims.auth_time, Some(t0()));

        let refreshed = fx
            .refresh_handler()
            .handle(
                Envelope::new(
                    Uuid::now_v7(),
                    crate::application::command::RefreshCommand {
                        refresh_token: session.refresh_token.clone(),
                        device: DeviceFingerprint::default(),
                    },
                ),
                t0() + Duration::minutes(1),
            )
            .await
            .unwrap();
        let claims = fx.minter.verify_access(&refreshed.access_token).await.unwrap();
        assert_eq!(claims.auth_time, None);
    }
}
