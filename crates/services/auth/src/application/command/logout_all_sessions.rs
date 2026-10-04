use std::sync::Arc;

use chrono::{DateTime, Utc};
use cqrs::Envelope;
use validate_core::{FieldViolation, Validate};

use crate::application::ensure_valid;
use crate::application::policy::SessionPolicy;
use crate::application::port::{
    EventPublisher, RefreshTokenRepository, SessionCache, SessionRepository,
};
use crate::domain::value_object::{AccountId, RevocationReason, SessionStatus};
use crate::error::AuthError;

/// Global sign-out for an account.
#[derive(Debug, Clone)]
pub struct LogoutAllSessionsCommand {
    pub account_id: String,
}

impl Validate for LogoutAllSessionsCommand {
    fn validate(&self) -> Result<(), Vec<FieldViolation>> {
        if self.account_id.trim().is_empty() {
            return Err(vec![FieldViolation::new(
                "account_id",
                "AUT-VAL-021",
                "account_id must not be empty",
            )]);
        }
        Ok(())
    }
}

#[derive(Debug, Clone)]
pub struct LogoutAllSessionsOutcome {
    /// The new generation after the bump; every token below it is dead.
    pub generation: i64,
    pub sessions_revoked: i32,
}

/// Bumps the account's generation — the instant, edge-enforced kill switch — then
/// durably revokes each active session and its refresh tokens.
///
/// The generation bump is what actually invalidates outstanding edge tokens on
/// the next request; the per-session revocation keeps the durable record and the
/// blacklist consistent.
pub struct LogoutAllSessionsHandler {
    sessions: Arc<dyn SessionRepository>,
    refresh_tokens: Arc<dyn RefreshTokenRepository>,
    cache: Arc<dyn SessionCache>,
    publisher: Arc<dyn EventPublisher>,
    policy: SessionPolicy,
}

impl LogoutAllSessionsHandler {
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
        envelope: Envelope<LogoutAllSessionsCommand>,
        now: DateTime<Utc>,
    ) -> Result<LogoutAllSessionsOutcome, AuthError> {
        ensure_valid(&envelope.payload)?;
        let account_id = AccountId::try_from(envelope.payload.account_id.as_str())?;

        // Instant global kill: every existing edge token now carries a stale gen.
        let generation = self.cache.bump_generation(&account_id).await?;

        // Durably revoke each active session for record-keeping + blacklist.
        let sessions = self.sessions.list_active_by_account(&account_id).await?;
        let mut revoked = 0;
        for mut session in sessions {
            if session.status() != SessionStatus::Active {
                continue;
            }
            let session_id = session.id();
            session.revoke(now, RevocationReason::GlobalLogout, envelope.correlation_id)?;
            self.sessions.save(&session).await?;
            self.cache.blacklist_session(&session_id, self.policy.access_ttl).await?;
            self.refresh_tokens.revoke_all_for_session(&session_id).await?;
            for event in &session.drain_events() {
                self.publisher.publish(event).await?;
            }
            revoked += 1;
        }

        Ok(LogoutAllSessionsOutcome { generation: generation.value(), sessions_revoked: revoked })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::application::command::LoginCommand;
    use crate::application::fakes::{t0, Fixture};
    use crate::application::port::AuthnGrant;
    use crate::domain::value_object::{DeviceFingerprint, Generation};
    use uuid::Uuid;

    async fn login(fx: &Fixture) -> crate::application::command::IssuedSession {
        let env = Envelope::new(
            Uuid::now_v7(),
            LoginCommand {
                grant: AuthnGrant::Password { username: "u".into(), password: "p".into() },
                device: DeviceFingerprint::default(),
                guest_refresh_token: None,
            },
        );
        fx.login_handler().handle(env, t0()).await.unwrap()
    }

    #[tokio::test]
    async fn logout_all_bumps_generation_and_revokes_every_session() {
        let fx = Fixture::new();
        // Two sessions for the same account (same subject ⇒ same account).
        let a = login(&fx).await;
        let _b = login(&fx).await;
        assert_eq!(fx.sessions.list_active_by_account(&a.account_id).await.unwrap().len(), 2);

        let out = fx
            .logout_all_handler()
            .handle(
                Envelope::new(
                    Uuid::now_v7(),
                    LogoutAllSessionsCommand { account_id: a.account_id.as_str() },
                ),
                t0(),
            )
            .await
            .unwrap();

        assert_eq!(out.sessions_revoked, 2);
        assert_eq!(out.generation, Generation::INITIAL.next().value());
        assert!(fx.sessions.list_active_by_account(&a.account_id).await.unwrap().is_empty());
        let revoked = fx.publisher.event_types().iter().filter(|t| **t == "auth.session_revoked").count();
        assert_eq!(revoked, 2);
    }
}
