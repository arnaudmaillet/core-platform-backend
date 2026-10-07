//! Third-party app authorisations (#667): the apps an account let sign it in
//! or act for it, each with its scopes. The holder lists and revokes them
//! (Settings → Apps and Websites); granting and use come from the OAuth server
//! a partner integration will add (mesh only until then).
//!
//! Every grant and every withdrawal is published (`auth.v1.events`) and kept
//! by audit as a `consent` record (GDPR Art. 7: proof of consent; withdrawing
//! is as easy as giving). Both are idempotent: a retry repeats the same
//! instant, so audit keeps one record.

use std::sync::Arc;

use chrono::{DateTime, SubsecRound, Utc};
use cqrs::Envelope;

use super::credentials::caller_session;
use super::mfa_settings::MfaCaller;
use crate::application::port::{
    AppAuthorization, AppAuthorizationRepository, EventPublisher, SessionCache, SessionRepository,
};
use crate::domain::event::{AppAuthorizationRevoked, AppAuthorized, DomainEvent};
use crate::domain::value_object::{AccountId, SessionId};
use crate::error::AuthError;

/// Longest app id, display name, icon URL and scope.
const MAX_APP_ID: usize = 64;
const MAX_DISPLAY_NAME: usize = 100;
const MAX_ICON_URL: usize = 512;
const MAX_SCOPE: usize = 64;
/// Most scopes one grant carries.
const MAX_SCOPES: usize = 20;

/// A grant, as the OAuth server asks for it.
#[derive(Debug, Clone)]
pub struct AppGrant {
    pub app_id:       String,
    pub display_name: String,
    pub icon_url:     String,
    pub scopes:       Vec<String>,
}

fn violation(field: &str, message: &str) -> AuthError {
    AuthError::DomainViolation { field: field.into(), message: message.into() }
}

fn token_like(s: &str, max: usize) -> bool {
    !s.is_empty()
        && s.len() <= max
        && s.chars().all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || matches!(c, '.' | '_' | '-' | ':'))
}

/// An app id: lowercase ASCII letters, digits, `.`, `_`, `-`, `:`; ≤ 64.
pub fn valid_app_id(app_id: &str) -> Result<(), AuthError> {
    if token_like(app_id, MAX_APP_ID) { Ok(()) } else { Err(violation("app_id", "an app id is 1–64 of [a-z0-9._:-]")) }
}

impl AppGrant {
    /// The grant as stored: validated, scopes sorted and deduplicated.
    fn normalized(self, granted_at: DateTime<Utc>) -> Result<AppAuthorization, AuthError> {
        valid_app_id(&self.app_id)?;
        let display_name = self.display_name.split_whitespace().collect::<Vec<_>>().join(" ");
        if display_name.is_empty() || display_name.chars().count() > MAX_DISPLAY_NAME {
            return Err(violation("display_name", "a display name is 1–100 characters"));
        }
        let icon_url = self.icon_url.trim().to_owned();
        if !icon_url.is_empty() && (!icon_url.starts_with("https://") || icon_url.len() > MAX_ICON_URL) {
            return Err(violation("icon_url", "an icon is an https URL of at most 512 characters"));
        }
        let mut scopes = self.scopes;
        scopes.sort();
        scopes.dedup();
        if scopes.is_empty() || scopes.len() > MAX_SCOPES || !scopes.iter().all(|s| token_like(s, MAX_SCOPE)) {
            return Err(violation("scopes", "1–20 scopes of [a-z0-9._:-], at most 64 each"));
        }
        Ok(AppAuthorization {
            app_id: self.app_id,
            display_name,
            icon_url,
            scopes,
            // Postgres keeps microseconds: the event and every later read agree.
            granted_at: granted_at.trunc_subsecs(6),
            last_used_at: None,
        })
    }
}

pub struct AppAuthorizations {
    repository: Arc<dyn AppAuthorizationRepository>,
    sessions:   Arc<dyn SessionRepository>,
    cache:      Arc<dyn SessionCache>,
    publisher:  Arc<dyn EventPublisher>,
}

impl AppAuthorizations {
    pub fn new(
        repository: Arc<dyn AppAuthorizationRepository>,
        sessions: Arc<dyn SessionRepository>,
        cache: Arc<dyn SessionCache>,
        publisher: Arc<dyn EventPublisher>,
    ) -> Self {
        Self { repository, sessions, cache, publisher }
    }

    async fn caller(&self, caller: &MfaCaller, now: DateTime<Utc>) -> Result<AccountId, AuthError> {
        let account_id = AccountId::try_from(caller.account_id.as_str())?;
        let session_id = SessionId::try_from(caller.session_id.as_str())?;
        caller_session(&self.sessions, &self.cache, &account_id, &session_id, now).await?;
        Ok(account_id)
    }

    /// The caller's authorised apps, most recently granted first.
    pub async fn list(&self, envelope: Envelope<MfaCaller>, now: DateTime<Utc>) -> Result<Vec<AppAuthorization>, AuthError> {
        let account_id = self.caller(&envelope.payload, now).await?;
        self.repository.list(&account_id).await
    }

    /// Withdraws the caller's authorisation of an app (and so every token
    /// issued under it); returns the apps left. Idempotent; `AUT-5030` when
    /// the caller never authorised it.
    pub async fn revoke(
        &self,
        envelope: Envelope<(MfaCaller, String)>,
        now: DateTime<Utc>,
    ) -> Result<Vec<AppAuthorization>, AuthError> {
        let correlation_id = envelope.correlation_id;
        let (caller, app_id) = envelope.payload;
        let account_id = self.caller(&caller, now).await?;
        valid_app_id(&app_id).map_err(|_| AuthError::AppAuthorizationNotFound)?;
        let revoked_at = self
            .repository
            .revoke(&account_id, &app_id, now.trunc_subsecs(6))
            .await?
            .ok_or(AuthError::AppAuthorizationNotFound)?;
        let event = AppAuthorizationRevoked { account_id, app_id, occurred_at: revoked_at, correlation_id };
        self.publisher.publish(&DomainEvent::AppAuthorizationRevoked(event)).await?;
        self.repository.list(&account_id).await
    }

    /// Mesh only (the OAuth server): records a grant the holder consented to.
    pub async fn grant(&self, envelope: Envelope<(String, AppGrant)>, now: DateTime<Utc>) -> Result<AppAuthorization, AuthError> {
        let correlation_id = envelope.correlation_id;
        let (account_id, grant) = envelope.payload;
        let account_id = AccountId::try_from(account_id.as_str())?;
        let authorization = grant.normalized(now)?;
        let stored = self.repository.grant(&account_id, &authorization).await?;
        let event = AppAuthorized {
            account_id,
            app_id: stored.app_id.clone(),
            scopes: stored.scopes.clone(),
            occurred_at: stored.granted_at,
            correlation_id,
        };
        self.publisher.publish(&DomainEvent::AppAuthorized(event)).await?;
        Ok(stored)
    }

    /// Mesh only (the OAuth server): the app used its grant. `AUT-5030` when it
    /// has no active one — the token must then be refused.
    pub async fn record_use(&self, account_id: &str, app_id: &str, now: DateTime<Utc>) -> Result<(), AuthError> {
        let account_id = AccountId::try_from(account_id)?;
        if self.repository.record_use(&account_id, app_id, now.trunc_subsecs(6)).await? {
            Ok(())
        } else {
            Err(AuthError::AppAuthorizationNotFound)
        }
    }
}

#[cfg(test)]
mod tests {
    use chrono::Duration;
    use uuid::Uuid;

    use super::*;
    use crate::application::command::{IssuedSession, LoginCommand};
    use crate::application::fakes::{t0, Fixture, InMemoryAppAuthorizationRepository};
    use crate::application::port::AuthnGrant;
    use crate::domain::value_object::DeviceFingerprint;

    struct World {
        fx:      Fixture,
        handler: AppAuthorizations,
    }

    impl World {
        fn new() -> Self {
            let fx = Fixture::new();
            let handler = AppAuthorizations::new(
                Arc::new(InMemoryAppAuthorizationRepository::default()),
                Arc::clone(&fx.sessions) as _,
                Arc::clone(&fx.cache) as _,
                Arc::clone(&fx.publisher) as _,
            );
            Self { fx, handler }
        }

        async fn login(&self) -> IssuedSession {
            let cmd = LoginCommand {
                grant: AuthnGrant::Password { username: "user".into(), password: "pw".into() },
                device: DeviceFingerprint::default(),
                guest_refresh_token: None,
                client_ip: None,
            };
            self.fx.login_handler().handle(Envelope::new(Uuid::now_v7(), cmd), t0()).await.unwrap().issued().unwrap()
        }

        async fn grant(&self, session: &IssuedSession, app: &str, scopes: &[&str], at: DateTime<Utc>) -> AppAuthorization {
            let grant = AppGrant {
                app_id:       app.into(),
                display_name: "  Partner   App ".into(),
                icon_url:     "https://partner.example/icon.png".into(),
                scopes:       scopes.iter().map(|s| (*s).to_owned()).collect(),
            };
            self.handler.grant(Envelope::new(Uuid::now_v7(), (session.account_id.as_str(), grant)), at).await.unwrap()
        }

        async fn listed(&self, session: &IssuedSession) -> Vec<String> {
            self.handler.list(on(session), t0()).await.unwrap().into_iter().map(|a| a.app_id).collect()
        }

        fn consent_events(&self) -> Vec<&'static str> {
            self.fx.publisher.event_types().into_iter().filter(|t| t.starts_with("auth.app_")).collect()
        }
    }

    fn on(session: &IssuedSession) -> Envelope<MfaCaller> {
        Envelope::new(
            Uuid::now_v7(),
            MfaCaller { account_id: session.account_id.as_str(), session_id: session.session_id.as_str() },
        )
    }

    #[tokio::test]
    async fn a_grant_is_listed_and_a_revocation_withdraws_it_with_proof() {
        let w = World::new();
        let session = w.login().await;
        assert!(w.listed(&session).await.is_empty(), "nothing until a partner app is authorised");

        let first = w.grant(&session, "partner.app", &["profile:read", "profile:read", "email"], t0()).await;
        assert_eq!((first.display_name.as_str(), first.scopes.clone()), ("Partner App", vec!["email".into(), "profile:read".into()]));
        w.grant(&session, "other.app", &["profile:read"], t0() + Duration::seconds(5)).await;
        assert_eq!(w.listed(&session).await, vec!["other.app", "partner.app"], "most recent first");

        // Granting again the same scopes is the same consent.
        let again = w.grant(&session, "partner.app", &["email", "profile:read"], t0() + Duration::minutes(1)).await;
        assert_eq!(again.granted_at, first.granted_at);

        let left = w.handler.revoke(Envelope::new(Uuid::now_v7(), (on(&session).payload, "partner.app".into())), t0()).await.unwrap();
        assert_eq!(left.iter().map(|a| a.app_id.as_str()).collect::<Vec<_>>(), vec!["other.app"]);
        // Revoking again: same withdrawal; an unknown app: AUT-5030.
        w.handler.revoke(Envelope::new(Uuid::now_v7(), (on(&session).payload, "partner.app".into())), t0()).await.unwrap();
        assert!(matches!(
            w.handler.revoke(Envelope::new(Uuid::now_v7(), (on(&session).payload, "never.app".into())), t0()).await,
            Err(AuthError::AppAuthorizationNotFound)
        ));

        assert_eq!(
            w.consent_events(),
            vec![
                "auth.app_authorized",
                "auth.app_authorized",
                "auth.app_authorized",
                "auth.app_authorization_revoked",
                "auth.app_authorization_revoked",
            ],
            "every grant and withdrawal is on record (retries repeat their instant)"
        );
    }

    #[tokio::test]
    async fn a_revoked_app_can_no_longer_be_used_until_granted_again() {
        let w = World::new();
        let session = w.login().await;
        let account = session.account_id.as_str();
        w.grant(&session, "partner.app", &["profile:read"], t0()).await;
        w.handler.record_use(&account, "partner.app", t0()).await.unwrap();
        let used = w.handler.list(on(&session), t0()).await.unwrap();
        assert!(used[0].last_used_at.is_some());

        w.handler.revoke(Envelope::new(Uuid::now_v7(), (on(&session).payload, "partner.app".into())), t0()).await.unwrap();
        assert!(matches!(w.handler.record_use(&account, "partner.app", t0()).await, Err(AuthError::AppAuthorizationNotFound)));

        let again = w.grant(&session, "partner.app", &["profile:read"], t0() + Duration::hours(1)).await;
        assert_eq!(again.granted_at, (t0() + Duration::hours(1)).trunc_subsecs(6), "a new consent");
        w.handler.record_use(&account, "partner.app", t0()).await.unwrap();
    }

    #[test]
    fn a_grant_is_validated() {
        let grant = |app: &str, icon: &str, scopes: &[&str]| {
            AppGrant {
                app_id:       app.into(),
                display_name: "App".into(),
                icon_url:     icon.into(),
                scopes:       scopes.iter().map(|s| (*s).to_owned()).collect(),
            }
            .normalized(t0())
        };
        assert!(grant("partner.app", "", &["email"]).is_ok());
        assert!(grant("Partner App", "", &["email"]).is_err(), "app id");
        assert!(grant("partner.app", "http://x/icon.png", &["email"]).is_err(), "https icon");
        assert!(grant("partner.app", "", &[]).is_err(), "a scope");
        assert!(grant("partner.app", "", &["Email Address"]).is_err(), "scope syntax");
    }

    #[tokio::test]
    async fn listing_and_revoking_need_a_live_session() {
        let w = World::new();
        let stranger = MfaCaller { account_id: Uuid::now_v7().to_string(), session_id: Uuid::now_v7().to_string() };
        assert!(w.handler.list(Envelope::new(Uuid::now_v7(), stranger), t0()).await.is_err());
    }
}
