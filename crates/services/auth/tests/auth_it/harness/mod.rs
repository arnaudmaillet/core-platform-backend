//! Integration harness: boots ephemeral Postgres + Redis containers, applies the
//! `.sql` migrations, and wires a real auth graph against them through the
//! production composition root ([`auth::app::App::compose`]).
//!
//! Auth's own stores (Postgres + Redis) are real; its *external* dependencies —
//! the IdP and the `account` service — are stubbed, exactly as they would be
//! mocked at a service boundary. The token minter is the real ES256 one.
#![allow(dead_code)]

use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use async_trait::async_trait;
use chrono::Duration as ChronoDuration;
use sqlx::PgPool;
use tonic::{Request, Status};
use uuid::Uuid;

use auth::app::{App, AppDeps};
use auth::application::port::{
    AccountActivation, AccountDirectory, AccountSnapshot, AuthnGrant, CredentialAdmin,
    EventPublisher, IdentityProvider, NormalizedClaims, ProfileDirectory,
};
use auth::application::SessionPolicy;
use auth::domain::value_object::{AccountId, IdpSubject, Permission, ProfileId};
use auth::error::AuthError;
use auth::infrastructure::cache::RedisSessionCache;
use auth::infrastructure::event::LogEventPublisher;
use auth::infrastructure::grpc::handler::{proto, AuthServiceHandler};
use auth::infrastructure::persistence::{
    PgGuestRegistry, PgRefreshTokenRepository, PgSessionRepository, PgSubjectLinkRepository,
};
use auth::infrastructure::token::{Es256TokenMinter, EsKeyMaterial};

use postgres_storage::config::StatementLogLevel;
use postgres_storage::{PgPoolBuilder, PostgresConfig, TransactionManager};
use redis_storage::{RedisClientBuilder, RedisConfig};

const MIGRATIONS_DIR: &str = concat!(env!("CARGO_MANIFEST_DIR"), "/migrations");

/// Generates an ephemeral P-256 keypair (PKCS#8 private PEM, SPKI public PEM) for
/// the test minter. No key material is hardcoded.
fn ephemeral_es256_pem() -> (Vec<u8>, Vec<u8>) {
    use p256::ecdsa::SigningKey;
    use p256::pkcs8::{EncodePrivateKey, EncodePublicKey, LineEnding};
    let signing = SigningKey::random(&mut rand_core::OsRng);
    let private_pem = signing.to_pkcs8_pem(LineEnding::LF).unwrap().as_bytes().to_vec();
    let public_pem = signing.verifying_key().to_public_key_pem(LineEnding::LF).unwrap().into_bytes();
    (private_pem, public_pem)
}

// ── Stub external services ───────────────────────────────────────────────────

/// IdP stub: echoes the password-grant username back as the subject (fixed
/// issuer), so "log in as alice" deterministically maps to one subject/account.
struct StubIdp;

#[async_trait]
impl IdentityProvider for StubIdp {
    async fn authenticate(&self, grant: AuthnGrant) -> Result<NormalizedClaims, AuthError> {
        let subject = match grant {
            AuthnGrant::Password { username, .. } => username,
            AuthnGrant::AuthorizationCode { code, .. } => code,
        };
        Ok(NormalizedClaims { issuer: "https://idp.test".to_owned(), subject })
    }
}

/// IdP credential-management stub, consistent with [`StubIdp`]: a subject's
/// login name is the subject itself; each password set is recorded.
#[derive(Default)]
pub struct StubCredentials {
    pub set: Mutex<Vec<(String, String)>>,
}

#[async_trait]
impl CredentialAdmin for StubCredentials {
    async fn login_name(&self, subject: &IdpSubject) -> Result<String, AuthError> {
        Ok(subject.subject().to_owned())
    }

    async fn set_password(&self, subject: &IdpSubject, new_password: &str) -> Result<(), AuthError> {
        self.set.lock().unwrap().push((subject.subject().to_owned(), new_password.to_owned()));
        Ok(())
    }
}

/// `account` stub: provisions a stable account id per subject and reports every
/// account active with a fixed permission set.
struct StubDirectory {
    accounts: Mutex<HashMap<IdpSubject, AccountId>>,
}

#[async_trait]
impl AccountDirectory for StubDirectory {
    async fn resolve_or_provision(&self, subject: &IdpSubject) -> Result<AccountId, AuthError> {
        let mut accounts = self.accounts.lock().unwrap();
        Ok(*accounts
            .entry(subject.clone())
            .or_insert_with(|| AccountId::from_uuid(Uuid::now_v7())))
    }

    async fn lookup(&self, _account_id: &AccountId) -> Result<AccountSnapshot, AuthError> {
        Ok(AccountSnapshot {
            activation: AccountActivation::Active,
            permissions: vec![Permission::new("posts:write")],
        })
    }

    async fn resume_deactivated(&self, _account_id: &AccountId) -> Result<(), AuthError> {
        Ok(())
    }
}

/// `profile` stub: every account owns exactly one profile, derived
/// deterministically from the account id (UUIDv5), so a token's `pids` claim is
/// stable across login and refresh within a test.
struct StubProfiles;

#[async_trait]
impl ProfileDirectory for StubProfiles {
    async fn list_profile_ids(&self, account_id: &AccountId) -> Result<Vec<ProfileId>, AuthError> {
        let derived = Uuid::new_v5(&Uuid::NAMESPACE_OID, account_id.as_uuid().as_bytes());
        Ok(vec![ProfileId::from_uuid(derived)])
    }
}

// ── Harness ──────────────────────────────────────────────────────────────────

pub struct Harness {
    pub handler: AuthServiceHandler,
    pub pool: PgPool,
    pub credentials: Arc<StubCredentials>,
}

impl Harness {
    pub async fn start() -> Self {
        let pg_url = test_support::containers::postgres_ready(MIGRATIONS_DIR).await;
        let redis_endpoint = test_support::containers::redis_endpoint().await;

        let pg_config = PostgresConfig {
            database_url: pg_url,
            max_connections: 8,
            min_connections: 1,
            acquire_timeout: Duration::from_secs(5),
            idle_timeout: None,
            max_lifetime: None,
            statement_log_level: StatementLogLevel::Debug,
            slow_statement_threshold: Duration::from_millis(500),
        };
        let pool = PgPoolBuilder::build(pg_config).await.expect("it: postgres pool");
        let tx = TransactionManager::new(pool.clone());

        let redis = RedisClientBuilder::new(RedisConfig {
            hosts: vec![redis_endpoint],
            ..RedisConfig::default()
        })
        .build()
        .await
        .expect("it: redis client");

        let (private_pem, public_pem) = ephemeral_es256_pem();
        let minter = Es256TokenMinter::from_pem(EsKeyMaterial {
            private_pem,
            public_pem,
            key_id: "auth-es256-1".to_owned(),
            issuer: "https://auth.core-platform".to_owned(),
            audience: "core-platform".to_owned(),
        })
        .expect("it: minter");

        let credentials = Arc::new(StubCredentials::default());
        let deps = AppDeps {
            idp: Arc::new(StubIdp),
            credentials: credentials.clone(),
            directory: Arc::new(StubDirectory { accounts: Mutex::new(HashMap::new()) }),
            profiles: Arc::new(StubProfiles),
            links: Arc::new(PgSubjectLinkRepository::new(tx.clone())),
            sessions: Arc::new(PgSessionRepository::new(tx.clone())),
            refresh_tokens: Arc::new(PgRefreshTokenRepository::new(tx.clone())),
            cache: Arc::new(RedisSessionCache::new(redis.clone())),
            minter: Arc::new(minter),
            publisher: Arc::new(LogEventPublisher) as Arc<dyn EventPublisher>,
            guests: Arc::new(PgGuestRegistry::new(tx.clone())),
            guest_sessions_enabled: true,
            policy: SessionPolicy::new(
                ChronoDuration::minutes(10),
                ChronoDuration::minutes(30),
                ChronoDuration::hours(8),
                ChronoDuration::days(7),
            ),
        };

        Self { handler: App::compose(deps), pool, credentials }
    }

    // ── RPC helpers ──────────────────────────────────────────────────────────

    pub async fn login(&self, username: &str) -> Result<proto::LoginResponse, Status> {
        let request = Request::new(proto::LoginRequest {
            device: None,
            grant_type: proto::GrantType::Password as i32,
            credential: Some(proto::login_request::Credential::Password(proto::PasswordGrant {
                username: username.to_owned(),
                password: "pw".to_owned(),
            })),
        });
        self.handler.login(request).await.map(|r| r.into_inner())
    }

    pub async fn start_guest(&self, device_id: &str) -> Result<proto::StartGuestSessionResponse, Status> {
        let request = Request::new(proto::StartGuestSessionRequest {
            device: Some(proto::DeviceContext {
                user_agent: String::new(),
                ip_address: String::new(),
                device_id: device_id.to_owned(),
            }),
            attestation: "assertion".to_owned(),
            locale: "fr-FR".to_owned(),
            region_hint: "FR".to_owned(),
            current_country: String::new(),
        });
        self.handler.start_guest_session(request).await.map(|r| r.into_inner())
    }

    pub async fn refresh_on(
        &self,
        refresh_token: &str,
        device_id: &str,
    ) -> Result<proto::RefreshResponse, Status> {
        let request = Request::new(proto::RefreshRequest {
            refresh_token: refresh_token.to_owned(),
            device: Some(proto::DeviceContext {
                user_agent: String::new(),
                ip_address: String::new(),
                device_id: device_id.to_owned(),
            }),
        });
        self.handler.refresh(request).await.map(|r| r.into_inner())
    }

    pub async fn refresh(&self, refresh_token: &str) -> Result<proto::RefreshResponse, Status> {
        let request = Request::new(proto::RefreshRequest {
            refresh_token: refresh_token.to_owned(),
            device: None,
        });
        self.handler.refresh(request).await.map(|r| r.into_inner())
    }

    pub async fn logout(&self, session_id: &str) -> Result<proto::LogoutResponse, Status> {
        let request = Request::new(proto::LogoutRequest { session_id: session_id.to_owned() });
        self.handler.logout(request).await.map(|r| r.into_inner())
    }

    pub async fn logout_all(
        &self,
        account_id: &str,
    ) -> Result<proto::LogoutAllSessionsResponse, Status> {
        let request =
            Request::new(proto::LogoutAllSessionsRequest { account_id: account_id.to_owned() });
        self.handler.logout_all_sessions(request).await.map(|r| r.into_inner())
    }

    pub async fn introspect(&self, access_token: &str) -> Result<proto::IntrospectResponse, Status> {
        let request = Request::new(proto::IntrospectRequest { access_token: access_token.to_owned() });
        self.handler.introspect(request).await.map(|r| r.into_inner())
    }

    pub async fn list_sessions(
        &self,
        account_id: &str,
    ) -> Result<proto::ListSessionsResponse, Status> {
        let request = Request::new(proto::ListSessionsRequest { account_id: account_id.to_owned() });
        self.handler.list_sessions(request).await.map(|r| r.into_inner())
    }

    // ── Direct DB assertions ─────────────────────────────────────────────────

    /// The device recorded for `guest_id` in `guest_principals`, if any.
    pub async fn guest_device(&self, guest_id: &str) -> Option<String> {
        let id = Uuid::parse_str(guest_id).unwrap();
        sqlx::query_scalar("SELECT device_id FROM guest_principals WHERE guest_id = $1")
            .bind(id)
            .fetch_optional(&self.pool)
            .await
            .unwrap()
    }

    /// The `kind` of a session row.
    pub async fn session_kind(&self, session_id: &str) -> String {
        let id = Uuid::parse_str(session_id).unwrap();
        sqlx::query_scalar("SELECT kind FROM sessions WHERE id = $1")
            .bind(id)
            .fetch_one(&self.pool)
            .await
            .unwrap()
    }

    pub async fn count_active_sessions(&self, account_id: &str) -> i64 {
        let id = Uuid::parse_str(account_id).unwrap();
        sqlx::query_scalar("SELECT COUNT(*) FROM sessions WHERE account_id = $1 AND status = 'active'")
            .bind(id)
            .fetch_one(&self.pool)
            .await
            .expect("count active sessions")
    }

    pub async fn count_subject_links(&self, account_id: &str) -> i64 {
        let id = Uuid::parse_str(account_id).unwrap();
        sqlx::query_scalar("SELECT COUNT(*) FROM subject_links WHERE account_id = $1")
            .bind(id)
            .fetch_one(&self.pool)
            .await
            .expect("count subject links")
    }
}

/// A fresh random username (⇒ a fresh subject ⇒ a fresh account).
pub fn random_user() -> String {
    format!("user-{}", Uuid::now_v7())
}
