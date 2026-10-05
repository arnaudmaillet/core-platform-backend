use std::sync::Arc;

use chrono::{DateTime, Utc};
use cqrs::{Envelope, Query, QueryHandler};

use crate::application::port::SessionRepository;
use crate::domain::value_object::{AccountId, SessionStatus};
use crate::error::AuthError;

/// List an account's active sessions for a device-management view.
#[derive(Debug, Clone)]
pub struct ListSessionsQuery {
    pub account_id: String,
    /// The caller's own session, flagged `current` in the result when present.
    pub current_session_id: Option<String>,
}

impl Query for ListSessionsQuery {
    type Response = Vec<SessionSummary>;
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SessionSummary {
    pub session_id: String,
    pub status: SessionStatus,
    pub generation: i64,
    pub issued_at: DateTime<Utc>,
    pub expires_at: DateTime<Utc>,
    pub absolute_expiry: DateTime<Utc>,
    pub current: bool,
}

pub struct ListSessionsHandler {
    sessions: Arc<dyn SessionRepository>,
}

impl ListSessionsHandler {
    pub fn new(sessions: Arc<dyn SessionRepository>) -> Self {
        Self { sessions }
    }
}

impl QueryHandler<ListSessionsQuery> for ListSessionsHandler {
    type Error = AuthError;

    async fn handle(
        &self,
        envelope: Envelope<ListSessionsQuery>,
    ) -> Result<Vec<SessionSummary>, Self::Error> {
        let query = &envelope.payload;
        let account_id = AccountId::try_from(query.account_id.as_str())?;
        let current = query.current_session_id.as_deref();

        let sessions = self.sessions.list_active_by_account(&account_id).await?;
        Ok(sessions
            .into_iter()
            .map(|s| {
                let session_id = s.id().as_str();
                let current = current == Some(session_id.as_str());
                SessionSummary {
                    session_id,
                    status: s.status(),
                    generation: s.generation().value(),
                    issued_at: s.issued_at(),
                    expires_at: s.expires_at(),
                    absolute_expiry: s.absolute_expiry(),
                    current,
                }
            })
            .collect())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::application::command::LoginCommand;
    use crate::application::fakes::{t0, Fixture};
    use crate::application::port::AuthnGrant;
    use crate::domain::value_object::DeviceFingerprint;
    use uuid::Uuid;

    async fn login(fx: &Fixture) -> crate::application::command::IssuedSession {
        let env = Envelope::new(
            Uuid::now_v7(),
            LoginCommand {
                grant: AuthnGrant::Password { username: "u".into(), password: "p".into() },
                device: DeviceFingerprint::default(),
                guest_refresh_token: None,
                client_ip: None,
            },
        );
        fx.login_handler().handle(env, t0()).await.unwrap()
    }

    #[tokio::test]
    async fn lists_active_sessions_and_flags_current() {
        let fx = Fixture::new();
        let a = login(&fx).await;
        let _b = login(&fx).await;

        let env = Envelope::new(
            Uuid::now_v7(),
            ListSessionsQuery {
                account_id: a.account_id.as_str(),
                current_session_id: Some(a.session_id.as_str()),
            },
        );
        let sessions = fx.list_sessions_handler().handle(env).await.unwrap();

        assert_eq!(sessions.len(), 2);
        assert_eq!(sessions.iter().filter(|s| s.current).count(), 1);
        assert!(sessions.iter().all(|s| s.status == SessionStatus::Active));
    }

    #[tokio::test]
    async fn rejects_invalid_account_id() {
        let fx = Fixture::new();
        let env = Envelope::new(
            Uuid::now_v7(),
            ListSessionsQuery { account_id: "not-a-uuid".into(), current_session_id: None },
        );
        let err = fx.list_sessions_handler().handle(env).await.unwrap_err();
        assert!(matches!(err, AuthError::InvalidAccountId(_)));
    }
}
