use std::sync::Arc;

use chrono::{DateTime, Utc};
use cqrs::{Envelope, Query, QueryHandler};

use crate::application::port::{SessionCache, TokenMinter};
use crate::error::AuthError;

/// Server-side token introspection.
#[derive(Debug, Clone)]
pub struct IntrospectQuery {
    pub access_token: String,
}

impl Query for IntrospectQuery {
    type Response = IntrospectionView;
}

/// The normalized principal an edge token resolves to — the same shape
/// `auth-context` produces on the inbound path. `active` is `false` (with empty
/// fields) for any token that fails a check, never an error.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IntrospectionView {
    pub active: bool,
    pub account_id: Option<String>,
    pub session_id: Option<String>,
    pub generation: i64,
    pub permissions: Vec<String>,
    pub expires_at: Option<DateTime<Utc>>,
}

impl IntrospectionView {
    fn inactive() -> Self {
        Self {
            active: false,
            account_id: None,
            session_id: None,
            generation: 0,
            permissions: Vec::new(),
            expires_at: None,
        }
    }
}

/// Verifies an edge token's signature, then applies the live revocation checks
/// (generation epoch + blacklist + expiry) the edge would apply.
pub struct IntrospectHandler {
    minter: Arc<dyn TokenMinter>,
    cache: Arc<dyn SessionCache>,
}

impl IntrospectHandler {
    pub fn new(minter: Arc<dyn TokenMinter>, cache: Arc<dyn SessionCache>) -> Self {
        Self { minter, cache }
    }

    /// Clock-injected core, for deterministic tests.
    pub async fn handle_at(
        &self,
        envelope: Envelope<IntrospectQuery>,
        now: DateTime<Utc>,
    ) -> Result<IntrospectionView, AuthError> {
        let claims = match self.minter.verify_access(&envelope.payload.access_token).await {
            Ok(claims) => claims,
            Err(_) => return Ok(IntrospectionView::inactive()),
        };

        if claims.expires_at <= now {
            return Ok(IntrospectionView::inactive());
        }
        let current_generation = self.cache.current_generation(&claims.account_id).await?;
        if claims.generation != current_generation {
            return Ok(IntrospectionView::inactive());
        }
        if self.cache.is_blacklisted(&claims.session_id).await? {
            return Ok(IntrospectionView::inactive());
        }

        Ok(IntrospectionView {
            active: true,
            account_id: Some(claims.account_id.as_str()),
            session_id: Some(claims.session_id.as_str()),
            generation: claims.generation.value(),
            permissions: claims.permissions.iter().map(|p| p.as_str().to_owned()).collect(),
            expires_at: Some(claims.expires_at),
        })
    }
}

impl QueryHandler<IntrospectQuery> for IntrospectHandler {
    type Error = AuthError;

    async fn handle(
        &self,
        envelope: Envelope<IntrospectQuery>,
    ) -> Result<IntrospectionView, Self::Error> {
        self.handle_at(envelope, Utc::now()).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::application::command::{LoginCommand, LogoutAllSessionsCommand, LogoutCommand};
    use crate::application::fakes::{t0, Fixture};
    use crate::application::port::AuthnGrant;
    use crate::domain::value_object::DeviceFingerprint;
    use chrono::Duration;
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

    fn introspect_env(token: &str) -> Envelope<IntrospectQuery> {
        Envelope::new(Uuid::now_v7(), IntrospectQuery { access_token: token.to_owned() })
    }

    #[tokio::test]
    async fn active_token_introspects_to_principal() {
        let fx = Fixture::new();
        let issued = login(&fx).await;

        let view = fx
            .introspect_handler()
            .handle_at(introspect_env(&issued.access_token), t0() + Duration::minutes(1))
            .await
            .unwrap();

        assert!(view.active);
        assert_eq!(view.account_id, Some(issued.account_id.as_str()));
        assert_eq!(view.session_id, Some(issued.session_id.as_str()));
    }

    #[tokio::test]
    async fn garbage_token_is_inactive() {
        let fx = Fixture::new();
        let view = fx.introspect_handler().handle_at(introspect_env("bogus"), t0()).await.unwrap();
        assert!(!view.active);
        assert_eq!(view, IntrospectionView::inactive());
    }

    #[tokio::test]
    async fn expired_token_is_inactive() {
        let fx = Fixture::new();
        let issued = login(&fx).await;
        // Access TTL is 10 minutes; introspect 11 minutes later.
        let view = fx
            .introspect_handler()
            .handle_at(introspect_env(&issued.access_token), t0() + Duration::minutes(11))
            .await
            .unwrap();
        assert!(!view.active);
    }

    #[tokio::test]
    async fn token_inactive_after_global_logout_bumps_generation() {
        let fx = Fixture::new();
        let issued = login(&fx).await;

        fx.logout_all_handler()
            .handle(
                Envelope::new(
                    Uuid::now_v7(),
                    LogoutAllSessionsCommand { account_id: issued.account_id.as_str() },
                ),
                t0(),
            )
            .await
            .unwrap();

        // The token's embedded generation (0) is now below the account's (1).
        let view = fx
            .introspect_handler()
            .handle_at(introspect_env(&issued.access_token), t0() + Duration::minutes(1))
            .await
            .unwrap();
        assert!(!view.active);
    }

    #[tokio::test]
    async fn token_inactive_after_single_logout_blacklist() {
        let fx = Fixture::new();
        let issued = login(&fx).await;

        fx.logout_handler()
            .handle(
                Envelope::new(
                    Uuid::now_v7(),
                    LogoutCommand { session_id: issued.session_id.as_str(), actor: None },
                ),
                t0(),
            )
            .await
            .unwrap();

        // Generation still matches (single logout doesn't bump it); the blacklist
        // is what makes the token inactive.
        let view = fx
            .introspect_handler()
            .handle_at(introspect_env(&issued.access_token), t0() + Duration::minutes(1))
            .await
            .unwrap();
        assert!(!view.active);
    }
}
