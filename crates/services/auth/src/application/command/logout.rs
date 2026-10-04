use std::sync::Arc;

use chrono::{DateTime, Utc};
use cqrs::Envelope;
use validate_core::{FieldViolation, Validate};

use crate::application::ensure_valid;
use crate::application::policy::SessionPolicy;
use crate::application::port::{
    EventPublisher, RefreshTokenRepository, SessionCache, SessionRepository,
};
use crate::domain::value_object::{RevocationReason, SessionId, SessionStatus};
use crate::error::AuthError;

/// Revoke a single session. The gRPC layer resolves "my current session" to a
/// concrete `session_id` from the authenticated principal before dispatch.
#[derive(Debug, Clone)]
pub struct LogoutCommand {
    pub session_id: String,
    /// The authenticated account when dispatched from the client edge: the
    /// session must belong to it (a foreign session reads as not found, so the
    /// surface leaks no session ids). `None` over the mesh.
    pub actor: Option<String>,
}

impl Validate for LogoutCommand {
    fn validate(&self) -> Result<(), Vec<FieldViolation>> {
        if self.session_id.trim().is_empty() {
            return Err(vec![FieldViolation::new(
                "session_id",
                "AUT-VAL-020",
                "session_id must not be empty",
            )]);
        }
        Ok(())
    }
}

#[derive(Debug, Clone)]
pub struct LogoutOutcome {
    pub success: bool,
}

/// Revokes one session: marks it revoked, blacklists it for the access-token
/// window, and invalidates its refresh tokens. Idempotent — revoking an already
/// terminal session succeeds without re-emitting events.
pub struct LogoutHandler {
    sessions: Arc<dyn SessionRepository>,
    refresh_tokens: Arc<dyn RefreshTokenRepository>,
    cache: Arc<dyn SessionCache>,
    publisher: Arc<dyn EventPublisher>,
    policy: SessionPolicy,
}

impl LogoutHandler {
    pub fn new(
        sessions: Arc<dyn SessionRepository>,
        refresh_tokens: Arc<dyn RefreshTokenRepository>,
        cache: Arc<dyn SessionCache>,
        publisher: Arc<dyn EventPublisher>,
        policy: SessionPolicy,
    ) -> Self {
        Self { sessions, refresh_tokens, cache, publisher, policy }
    }

    pub async fn handle(
        &self,
        envelope: Envelope<LogoutCommand>,
        now: DateTime<Utc>,
    ) -> Result<LogoutOutcome, AuthError> {
        ensure_valid(&envelope.payload)?;
        let session_id = SessionId::try_from(envelope.payload.session_id.as_str())?;

        let mut session = self
            .sessions
            .find_by_id(&session_id)
            .await?
            .ok_or(AuthError::SessionNotFound { id: session_id.as_str() })?;

        if let Some(actor) = &envelope.payload.actor
            && session.account_id().as_str() != *actor
        {
            return Err(AuthError::SessionNotFound { id: session_id.as_str() });
        }

        // Already terminal ⇒ idempotent success, nothing to do.
        if session.status() != SessionStatus::Active {
            return Ok(LogoutOutcome { success: true });
        }

        session.revoke(now, RevocationReason::Logout, envelope.correlation_id)?;
        self.sessions.save(&session).await?;
        self.cache.blacklist_session(&session_id, self.policy.access_ttl).await?;
        self.refresh_tokens.revoke_all_for_session(&session_id).await?;
        for event in &session.drain_events() {
            self.publisher.publish(event).await?;
        }

        Ok(LogoutOutcome { success: true })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::application::command::{LoginCommand, LoginHandler};
    use crate::application::fakes::{t0, Fixture};
    use crate::application::port::AuthnGrant;
    use crate::domain::value_object::DeviceFingerprint;
    use uuid::Uuid;

    async fn login(fx: &Fixture) -> crate::application::command::IssuedSession {
        let h: LoginHandler = fx.login_handler();
        let env = Envelope::new(
            Uuid::now_v7(),
            LoginCommand {
                grant: AuthnGrant::Password { username: "u".into(), password: "p".into() },
                device: DeviceFingerprint::default(),
                guest_refresh_token: None,
            },
        );
        h.handle(env, t0()).await.unwrap()
    }

    fn logout_env(session_id: &str) -> Envelope<LogoutCommand> {
        Envelope::new(Uuid::now_v7(), LogoutCommand { session_id: session_id.to_owned(), actor: None })
    }

    #[tokio::test]
    async fn logout_revokes_blacklists_and_emits() {
        let fx = Fixture::new();
        let issued = login(&fx).await;
        let sid = issued.session_id;

        let out = fx.logout_handler().handle(logout_env(&sid.as_str()), t0()).await.unwrap();
        assert!(out.success);

        assert!(fx.sessions.list_active_by_account(&issued.account_id).await.unwrap().is_empty());
        assert!(fx.cache.is_blacklisted(&sid).await.unwrap());
        assert!(fx.publisher.event_types().contains(&"auth.session_revoked"));
    }

    #[tokio::test]
    async fn logout_is_idempotent() {
        let fx = Fixture::new();
        let issued = login(&fx).await;
        let sid = issued.session_id.as_str();

        assert!(fx.logout_handler().handle(logout_env(&sid), t0()).await.unwrap().success);
        // Second logout on the now-revoked session still succeeds, no new events.
        let before = fx.publisher.count();
        assert!(fx.logout_handler().handle(logout_env(&sid), t0()).await.unwrap().success);
        assert_eq!(fx.publisher.count(), before, "no duplicate revocation event");
    }

    #[tokio::test]
    async fn logout_unknown_session_is_not_found() {
        let fx = Fixture::new();
        let err = fx
            .logout_handler()
            .handle(logout_env(&crate::domain::value_object::SessionId::new().as_str()), t0())
            .await
            .unwrap_err();
        assert!(matches!(err, AuthError::SessionNotFound { .. }));
    }
}
