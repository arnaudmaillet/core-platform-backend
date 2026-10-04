use std::sync::Arc;

use chrono::{DateTime, Utc};
use cqrs::Envelope;
use validate_core::{FieldViolation, Validate};

use super::credentials::{caller_session, prove_password};
use crate::application::ensure_valid;
use crate::application::policy::SessionPolicy;
use crate::application::port::{
    CredentialAdmin, EventPublisher, IdentityProvider, RefreshTokenRepository, SessionCache,
    SessionRepository,
};
use crate::domain::value_object::{AccountId, RevocationReason, SessionId, SessionStatus};
use crate::error::AuthError;

/// Bounds of a new password, in characters. The IdP's own policy may be stricter
/// (it answers [`AuthError::PasswordRejected`]).
pub const PASSWORD_MIN_CHARS: usize = 8;
pub const PASSWORD_MAX_CHARS: usize = 128;

/// The holder changes the password they sign in with. `account_id` and
/// `session_id` are the caller's, from the verified token — never the request.
#[derive(Clone)]
pub struct ChangePasswordCommand {
    pub account_id: String,
    pub session_id: String,
    pub current_password: String,
    pub new_password: String,
    /// Sign every other session of the account out (this one stays).
    pub sign_out_other_sessions: bool,
}

impl std::fmt::Debug for ChangePasswordCommand {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ChangePasswordCommand")
            .field("account_id", &self.account_id)
            .field("session_id", &self.session_id)
            .field("sign_out_other_sessions", &self.sign_out_other_sessions)
            .finish_non_exhaustive()
    }
}

impl Validate for ChangePasswordCommand {
    fn validate(&self) -> Result<(), Vec<FieldViolation>> {
        let mut v = Vec::new();
        if self.account_id.trim().is_empty() || self.session_id.trim().is_empty() {
            v.push(FieldViolation::new(
                "session",
                "AUT-VAL-022",
                "changing a password needs the caller's session",
            ));
        }
        if self.current_password.is_empty() {
            v.push(FieldViolation::new(
                "current_password",
                "AUT-VAL-023",
                "current_password must not be empty",
            ));
        }
        let chars = self.new_password.chars().count();
        if chars < PASSWORD_MIN_CHARS {
            v.push(FieldViolation::new(
                "new_password",
                "AUT-VAL-024",
                format!("new_password must be at least {PASSWORD_MIN_CHARS} characters"),
            ));
        } else if chars > PASSWORD_MAX_CHARS {
            v.push(FieldViolation::new(
                "new_password",
                "AUT-VAL-025",
                format!("new_password must be at most {PASSWORD_MAX_CHARS} characters"),
            ));
        }
        if !self.new_password.is_empty() && self.new_password == self.current_password {
            v.push(FieldViolation::new(
                "new_password",
                "AUT-VAL-026",
                "new_password must differ from current_password",
            ));
        }
        if v.is_empty() { Ok(()) } else { Err(v) }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ChangePasswordOutcome {
    /// Other sessions signed out (0 unless asked).
    pub sessions_revoked: i32,
}

/// Proves the current password with the IdP, sets the new one there, then
/// optionally signs the account's other sessions out. The password never
/// touches auth's storage.
pub struct ChangePasswordHandler {
    idp: Arc<dyn IdentityProvider>,
    admin: Arc<dyn CredentialAdmin>,
    sessions: Arc<dyn SessionRepository>,
    refresh_tokens: Arc<dyn RefreshTokenRepository>,
    cache: Arc<dyn SessionCache>,
    publisher: Arc<dyn EventPublisher>,
    policy: SessionPolicy,
}

impl ChangePasswordHandler {
    pub fn new(
        idp: Arc<dyn IdentityProvider>,
        admin: Arc<dyn CredentialAdmin>,
        sessions: Arc<dyn SessionRepository>,
        refresh_tokens: Arc<dyn RefreshTokenRepository>,
        cache: Arc<dyn SessionCache>,
        publisher: Arc<dyn EventPublisher>,
        policy: SessionPolicy,
    ) -> Self {
        Self { idp, admin, sessions, refresh_tokens, cache, publisher, policy }
    }

    pub async fn handle(
        &self,
        envelope: Envelope<ChangePasswordCommand>,
        now: DateTime<Utc>,
    ) -> Result<ChangePasswordOutcome, AuthError> {
        ensure_valid(&envelope.payload)?;
        let correlation_id = envelope.correlation_id;
        let cmd = envelope.payload;
        let account_id = AccountId::try_from(cmd.account_id.as_str())?;
        let session_id = SessionId::try_from(cmd.session_id.as_str())?;

        let session = caller_session(&self.sessions, &self.cache, &account_id, &session_id, now).await?;
        prove_password(&self.admin, &self.idp, &session, cmd.current_password).await?;
        self.admin.set_password(session.subject(), &cmd.new_password).await?;

        let mut revoked = 0;
        if cmd.sign_out_other_sessions {
            for mut other in self.sessions.list_active_by_account(&account_id).await? {
                if other.id() == session_id || other.status() != SessionStatus::Active {
                    continue;
                }
                let other_id = other.id();
                other.revoke(now, RevocationReason::PasswordChanged, correlation_id)?;
                self.sessions.save(&other).await?;
                self.cache.blacklist_session(&other_id, self.policy.access_ttl).await?;
                self.refresh_tokens.revoke_all_for_session(&other_id).await?;
                for event in &other.drain_events() {
                    self.publisher.publish(event).await?;
                }
                revoked += 1;
            }
        }
        Ok(ChangePasswordOutcome { sessions_revoked: revoked })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::application::command::{IssuedSession, LoginCommand, LogoutCommand};
    use crate::application::fakes::{t0, Fixture};
    use crate::application::port::AuthnGrant;
    use crate::domain::value_object::DeviceFingerprint;
    use uuid::Uuid;

    async fn login(fx: &Fixture) -> IssuedSession {
        let cmd = LoginCommand {
            grant: AuthnGrant::Password { username: "user".into(), password: "old-password".into() },
            device: DeviceFingerprint::default(),
            guest_refresh_token: None,
        };
        fx.login_handler().handle(Envelope::new(Uuid::now_v7(), cmd), t0()).await.unwrap()
    }

    fn change(on: &IssuedSession, current: &str, new: &str, sign_out: bool) -> Envelope<ChangePasswordCommand> {
        Envelope::new(
            Uuid::now_v7(),
            ChangePasswordCommand {
                account_id: on.account_id.as_str(),
                session_id: on.session_id.as_str(),
                current_password: current.into(),
                new_password: new.into(),
                sign_out_other_sessions: sign_out,
            },
        )
    }

    fn violation_codes(err: AuthError) -> Vec<String> {
        match err {
            AuthError::Validation(e) => e.violations().iter().map(|v| v.code.to_string()).collect(),
            other => panic!("expected a validation error, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn sets_the_new_password_at_the_idp_and_signs_only_the_other_sessions_out() {
        let fx = Fixture::new();
        fx.idp.with_password("old-password");
        let here = login(&fx).await;
        let other = login(&fx).await;
        let before = fx.publisher.count();

        let out = fx
            .change_password_handler()
            .handle(change(&here, "old-password", "new-password-1", true), t0())
            .await
            .unwrap();

        assert_eq!(out.sessions_revoked, 1);
        let set = fx.credentials.passwords_set();
        assert_eq!(set.len(), 1);
        assert_eq!(set[0].0.subject(), "sub-123");
        assert_eq!(set[0].1, "new-password-1");
        let here_now = fx.sessions.find_by_id(&here.session_id).await.unwrap().unwrap();
        let other_now = fx.sessions.find_by_id(&other.session_id).await.unwrap().unwrap();
        assert_eq!(here_now.status(), SessionStatus::Active);
        assert_eq!(other_now.status(), SessionStatus::Revoked);
        assert!(fx.cache.is_blacklisted(&other.session_id).await.unwrap());
        assert_eq!(fx.publisher.event_types()[before..], ["auth.session_revoked"]);
    }

    #[tokio::test]
    async fn keeps_the_other_sessions_unless_asked() {
        let fx = Fixture::new();
        let here = login(&fx).await;
        let other = login(&fx).await;

        let out = fx
            .change_password_handler()
            .handle(change(&here, "old-password", "new-password-1", false), t0())
            .await
            .unwrap();

        assert_eq!(out.sessions_revoked, 0);
        let other = fx.sessions.find_by_id(&other.session_id).await.unwrap().unwrap();
        assert_eq!(other.status(), SessionStatus::Active);
    }

    #[tokio::test]
    async fn a_wrong_current_password_changes_nothing() {
        let fx = Fixture::new();
        fx.idp.with_password("old-password");
        let here = login(&fx).await;

        let err = fx
            .change_password_handler()
            .handle(change(&here, "guess", "new-password-1", true), t0())
            .await
            .unwrap_err();

        assert!(matches!(err, AuthError::IdpAuthenticationFailed));
        assert!(fx.credentials.passwords_set().is_empty());
    }

    #[tokio::test]
    async fn a_weak_or_unchanged_password_is_refused_with_its_rule() {
        let fx = Fixture::new();
        let here = login(&fx).await;
        let handler = fx.change_password_handler();

        let err = handler.handle(change(&here, "old-password", "short", true), t0()).await.unwrap_err();
        assert_eq!(violation_codes(err), vec!["AUT-VAL-024"]);
        let err = handler
            .handle(change(&here, "old-password", &"x".repeat(PASSWORD_MAX_CHARS + 1), true), t0())
            .await
            .unwrap_err();
        assert_eq!(violation_codes(err), vec!["AUT-VAL-025"]);
        let err = handler
            .handle(change(&here, "old-password", "old-password", true), t0())
            .await
            .unwrap_err();
        assert_eq!(violation_codes(err), vec!["AUT-VAL-026"]);

        // The IdP's own policy has the last word.
        fx.credentials.refuse_with("Invalid password: must contain 1 digit.");
        let err = handler
            .handle(change(&here, "old-password", "no-digits-here", true), t0())
            .await
            .unwrap_err();
        assert!(matches!(err, AuthError::PasswordRejected { reason } if reason.contains("digit")));
        assert!(fx.credentials.passwords_set().is_empty());
    }

    #[tokio::test]
    async fn a_signed_out_or_foreign_session_cannot_change_the_password() {
        let fx = Fixture::new();
        let here = login(&fx).await;
        fx.logout_handler()
            .handle(
                Envelope::new(
                    Uuid::now_v7(),
                    LogoutCommand { session_id: here.session_id.as_str(), actor: None },
                ),
                t0(),
            )
            .await
            .unwrap();
        let err = fx
            .change_password_handler()
            .handle(change(&here, "old-password", "new-password-1", true), t0())
            .await
            .unwrap_err();
        assert!(matches!(err, AuthError::SessionRevoked));

        // Someone else's session id under my account id: not found.
        let mut foreign = change(&login(&fx).await, "old-password", "new-password-1", true);
        foreign.payload.account_id = AccountId::from_uuid(Uuid::now_v7()).as_str();
        let err = fx.change_password_handler().handle(foreign, t0()).await.unwrap_err();
        assert!(matches!(err, AuthError::SessionNotFound { .. }));
        assert!(fx.credentials.passwords_set().is_empty());
    }
}
