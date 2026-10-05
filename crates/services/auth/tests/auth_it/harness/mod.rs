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
            AuthnGrant::IdToken { .. } | AuthnGrant::Code { .. } => return Err(AuthError::IdpAuthenticationFailed),
        };
        Ok(NormalizedClaims { issuer: "https://idp.test".to_owned(), subject })
    }
}

/// IdP credential-management stub, consistent with [`StubIdp`]: a subject's
/// login name is the subject itself; each password set is recorded.
#[derive(Default)]
pub struct StubCredentials {
    pub set: Mutex<Vec<(String, String)>>,
    pub deleted: Mutex<Vec<String>>,
    /// (subject, email) of every IdP email set (#651).
    pub emails: Mutex<Vec<(String, String)>>,
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

    async fn delete_user(&self, subject: &IdpSubject) -> Result<(), AuthError> {
        self.deleted.lock().unwrap().push(subject.subject().to_owned());
        Ok(())
    }

    async fn set_email(&self, subject: &IdpSubject, email: &str) -> Result<(), AuthError> {
        self.emails.lock().unwrap().push((subject.subject().to_owned(), email.to_owned()));
        Ok(())
    }
}

/// `account` stub: provisions a stable account id per subject and reports every
/// account active with a fixed permission set; contacts are kept per account.
/// (sealed seed, unused backup-code hashes), as `account` keeps them.
pub type StoredMfa = (Vec<u8>, Vec<String>);

pub struct StubDirectory {
    accounts: Mutex<HashMap<IdpSubject, AccountId>>,
    pub contacts: Mutex<HashMap<AccountId, auth::application::port::ContactDetails>>,
    /// account → (sealed seed, unused backup-code hashes): two-step sign-in
    /// on (#649), kept as `account` would.
    pub mfa: Mutex<HashMap<AccountId, StoredMfa>>,
}

impl StubDirectory {
    pub fn new() -> Self {
        Self {
            accounts: Mutex::new(HashMap::new()),
            contacts: Mutex::new(HashMap::new()),
            mfa: Mutex::new(HashMap::new()),
        }
    }
}

#[async_trait]
impl AccountDirectory for StubDirectory {
    async fn resolve_or_provision(&self, subject: &IdpSubject) -> Result<AccountId, AuthError> {
        let mut accounts = self.accounts.lock().unwrap();
        Ok(*accounts
            .entry(subject.clone())
            .or_insert_with(|| AccountId::from_uuid(Uuid::now_v7())))
    }

    async fn lookup(&self, account_id: &AccountId) -> Result<AccountSnapshot, AuthError> {
        Ok(AccountSnapshot {
            activation: AccountActivation::Active,
            permissions: vec![Permission::new("posts:write")],
            age_bracket: None,
            mfa_enrolled: self.mfa.lock().unwrap().contains_key(account_id),
        })
    }

    async fn mfa_secret(&self, account_id: &AccountId) -> Result<auth::application::port::MfaSecret, AuthError> {
        Ok(match self.mfa.lock().unwrap().get(account_id) {
            Some((sealed, codes)) => auth::application::port::MfaSecret {
                enrolled: true,
                sealed_seed: sealed.clone(),
                recovery_codes_remaining: codes.len() as u32,
            },
            None => auth::application::port::MfaSecret::default(),
        })
    }

    async fn export_link(
        &self,
        _account_id: &AccountId,
    ) -> Result<Option<(String, chrono::DateTime<chrono::Utc>)>, AuthError> {
        Ok(None)
    }

    async fn enroll_mfa(&self, account_id: &AccountId, sealed_seed: &[u8], code_hashes: &[String]) -> Result<(), AuthError> {
        let mut mfa = self.mfa.lock().unwrap();
        if mfa.contains_key(account_id) {
            return Err(AuthError::MfaAlreadyEnabled);
        }
        mfa.insert(*account_id, (sealed_seed.to_vec(), code_hashes.to_vec()));
        Ok(())
    }

    async fn revoke_mfa(&self, account_id: &AccountId) -> Result<(), AuthError> {
        self.mfa.lock().unwrap().remove(account_id).map(|_| ()).ok_or(AuthError::MfaNotEnabled)
    }

    async fn replace_recovery_codes(&self, account_id: &AccountId, code_hashes: &[String]) -> Result<(), AuthError> {
        match self.mfa.lock().unwrap().get_mut(account_id) {
            Some((_, codes)) => {
                *codes = code_hashes.to_vec();
                Ok(())
            }
            None => Err(AuthError::MfaNotEnabled),
        }
    }

    async fn consume_recovery_code(&self, account_id: &AccountId, code_hash: &str) -> Result<bool, AuthError> {
        let mut mfa = self.mfa.lock().unwrap();
        let Some((_, codes)) = mfa.get_mut(account_id) else { return Ok(false) };
        Ok(match codes.iter().position(|c| c == code_hash) {
            Some(at) => {
                codes.remove(at);
                true
            }
            None => false,
        })
    }

    async fn resume_deactivated(&self, _account_id: &AccountId) -> Result<(), AuthError> {
        Ok(())
    }

    async fn provision(&self, account: &auth::application::port::NewAccount) -> Result<AccountId, AuthError> {
        self.resolve_or_provision(&account.subject).await
    }

    async fn find_by_email(&self, _email: &str) -> Result<Option<auth::application::port::EmailHolder>, AuthError> {
        Ok(None)
    }

    async fn find_by_phone(&self, _phone: &str) -> Result<Option<auth::application::port::EmailHolder>, AuthError> {
        Ok(None)
    }

    async fn contact(&self, account_id: &AccountId) -> Result<auth::application::port::ContactDetails, AuthError> {
        Ok(self.contacts.lock().unwrap().get(account_id).cloned().unwrap_or_default())
    }

    async fn change_contact(
        &self,
        account_id: &AccountId,
        channel: auth::application::port::VerificationChannel,
        destination: &str,
    ) -> Result<(), AuthError> {
        let mut contacts = self.contacts.lock().unwrap();
        let contact = contacts.entry(*account_id).or_default();
        match channel {
            auth::application::port::VerificationChannel::Email => contact.email = Some(destination.to_owned()),
            auth::application::port::VerificationChannel::Sms => contact.phone = Some(destination.to_owned()),
        }
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
    /// The `account` stub (contacts, #651).
    pub directory: Arc<StubDirectory>,
    /// The live Redis, for adapter-level scenarios (one-time codes).
    pub redis: redis_storage::RedisClient,
    /// The seed key the handler uses (#649), to enroll accounts in scenarios.
    pub seed_cipher: Arc<auth::infrastructure::mfa::AesSeedCipher>,
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
        let directory = Arc::new(StubDirectory::new());
        // Two-step sign-in (#649) over the real cipher and the live Redis.
        let seed_cipher = Arc::new(
            auth::infrastructure::mfa::AesSeedCipher::new("it", &[7u8; 32], &[]).expect("it: seed key"),
        );
        let mfa = Arc::new(auth::application::command::MfaVerifier::new(
            directory.clone(),
            seed_cipher.clone(),
            Arc::new(auth::infrastructure::cache::RedisMfaStore::new(redis.clone())),
            auth::application::command::MfaPolicy::default(),
        ));
        let deps = AppDeps {
            idp: Arc::new(StubIdp),
            credentials: credentials.clone(),
            directory: directory.clone(),
            profiles: Arc::new(StubProfiles),
            links: Arc::new(PgSubjectLinkRepository::new(tx.clone())),
            sessions: Arc::new(PgSessionRepository::new(tx.clone())),
            refresh_tokens: Arc::new(PgRefreshTokenRepository::new(tx.clone())),
            cache: Arc::new(RedisSessionCache::new(redis.clone())),
            minter: Arc::new(minter),
            publisher: Arc::new(LogEventPublisher) as Arc<dyn EventPublisher>,
            guests: Arc::new(PgGuestRegistry::new(tx.clone())),
            guest_sessions_enabled: true,
            federated: std::sync::Arc::new(auth::infrastructure::idp::JwksFederatedTokenVerifier::new()),
            codes: std::sync::Arc::new(auth::application::command::VerificationCodes::new(
                std::sync::Arc::new(auth::infrastructure::cache::RedisVerificationStore::new(redis.clone())),
                std::sync::Arc::new(auth::infrastructure::notify::LogCodeSender),
                auth::application::command::VerificationPolicy::default(),
            )),
            attestation: None,
            nonces: std::sync::Arc::new(auth::application::command::FederatedNonces::new(
                std::sync::Arc::new(auth::infrastructure::cache::RedisNonceStore::new(redis.clone())),
                true,
            )),
            mfa,
            mfa_issuer: "Core Platform".into(),
            policy: SessionPolicy::new(
                ChronoDuration::minutes(10),
                ChronoDuration::minutes(30),
                ChronoDuration::hours(8),
                ChronoDuration::days(7),
            ),
        };

        Self { handler: App::compose(deps), pool, credentials, directory, redis, seed_cipher }
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
            guest_refresh_token: String::new(),
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
            ..Default::default()
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
