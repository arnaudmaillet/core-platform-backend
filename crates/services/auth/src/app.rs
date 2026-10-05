//! The auth service's composition root.
//!
//! [`App::compose`] is *pure* wiring: eight port handles in, a fully-assembled
//! gRPC handler out — it binds no socket and reads no environment, so the live
//! integration harness and the binary entrypoint build the exact same graph.
//! [`App::build`] is the I/O variant that constructs the concrete adapters from
//! config + backend connections, then defers to `compose`.

use std::sync::Arc;

use postgres_storage::{PgPoolBuilder, PostgresConfig, TransactionManager};
use redis_storage::{RedisClient, RedisClientBuilder, RedisConfig};
use sqlx::PgPool;
use tonic::transport::Channel;
use transport::kafka::config::client::KafkaClientConfig;
use transport::kafka::config::producer::ProducerConfig;
use transport::kafka::producer::KafkaProducerBuilder;

use crate::application::command::{
    AccountErasure, ChangePasswordHandler, FederatedNonces, GuestRetention, LoginHandler, LogoutAllSessionsHandler,
    LogoutHandler, MemberSessions, NonceBoundVerifier, RefreshHandler, SignUpHandler, StartGuestSessionHandler,
    VerificationCodes, VerifyCredentialsHandler,
};
use crate::application::port::{
    AccountDirectory, CredentialAdmin, EventPublisher, FederatedTokenVerifier, GuestRegistry,
    IdentityProvider,
    ProfileDirectory,
    RefreshTokenRepository,
    SessionCache, SessionRepository, SubjectLinkRepository, TokenMinter,
};
use crate::application::query::{IntrospectHandler, ListSessionsHandler};
use crate::application::SessionPolicy;
use crate::config::AuthConfig;
use crate::infrastructure::cache::{RedisNonceStore, RedisSessionCache, RedisVerificationStore};
use crate::infrastructure::notify::{ChannelCodeSender, LogCodeSender, SmtpCodeSender, SnsCodeSender};
use crate::application::port::CodeSender;
use crate::infrastructure::directory::{GrpcAccountDirectory, GrpcProfileDirectory};
use crate::infrastructure::event::outbox_relay::OutboxRelay;
use crate::infrastructure::event::pg_outbox_publisher::PgOutboxPublisher;
use crate::infrastructure::event::{KafkaEventPublisher, LogEventPublisher};
use crate::infrastructure::grpc::handler::AuthServiceHandler;
use crate::infrastructure::idp::{
    JwksFederatedTokenVerifier, KeycloakCredentialAdmin, KeycloakIdentityProvider,
    UnconfiguredCredentialAdmin,
};
use crate::infrastructure::persistence::{
    PgAccountEraser, PgGuestRegistry, PgRefreshTokenRepository, PgSessionRepository, PgSubjectLinkRepository,
};
use crate::infrastructure::token::Es256TokenMinter;

/// The nine ports the application layer depends on, plus the token policy.
pub struct AppDeps {
    pub idp: Arc<dyn IdentityProvider>,
    /// The IdP's credential management (`ChangePassword`).
    pub credentials: Arc<dyn CredentialAdmin>,
    pub directory: Arc<dyn AccountDirectory>,
    pub profiles: Arc<dyn ProfileDirectory>,
    pub links: Arc<dyn SubjectLinkRepository>,
    pub sessions: Arc<dyn SessionRepository>,
    pub refresh_tokens: Arc<dyn RefreshTokenRepository>,
    pub cache: Arc<dyn SessionCache>,
    pub minter: Arc<dyn TokenMinter>,
    pub publisher: Arc<dyn EventPublisher>,
    /// Guest records (`StartGuestSession`).
    pub guests: Arc<dyn GuestRegistry>,
    /// `StartGuestSession` kill switch (`AUTH_GUEST_SESSIONS_ENABLED`).
    pub guest_sessions_enabled: bool,
    /// Native Sign in with Apple / Google id_tokens (SignUp, id_token Login).
    pub federated: Arc<dyn FederatedTokenVerifier>,
    /// Email one-time codes (StartVerification, code SignUp / Login).
    pub codes: Arc<VerificationCodes>,
    /// Server-issued sign-in nonces (StartFederatedSignIn), redeemed by the
    /// id_token SignUp / Login.
    pub nonces: Arc<FederatedNonces>,
    pub policy: SessionPolicy,
}

/// Backend connection configs. `kafka` is optional: absent ⇒ the log publisher.
pub struct Backends {
    pub postgres: PostgresConfig,
    pub redis: RedisConfig,
    pub kafka: Option<KafkaClientConfig>,
}

/// A fully-wired auth service. Retains the Postgres pool and Redis client so the
/// runtime can build liveness probes over the same connections.
pub struct App {
    pub handler: AuthServiceHandler,
    pub pool: PgPool,
    pub redis: RedisClient,
    /// The key ring's JWKS, serialized once at build (the ring is fixed for the
    /// process lifetime — rotation is a redeploy). Served over HTTP by the
    /// runtime host (see `infrastructure::http::jwks`) for downstream
    /// verifiers (realtime, audit) that fetch `AUTH_JWKS_URL`.
    pub jwks_json: String,
    /// Drains the auth_outbox table to the broker; spawned by the runtime
    /// adapter. Handlers never touch the broker directly anymore.
    pub relay: OutboxRelay,
    /// Deletes guest data past `AUTH_GUEST_RETENTION_DAYS`; spawned by the
    /// runtime adapter.
    pub guest_retention: GuestRetention,
    /// Erases an account's auth data on `account_deleted` (GDPR Art. 17); fed
    /// by the account-event consumer the runtime adapter spawns.
    pub erasure: Arc<AccountErasure>,
}

impl App {
    /// Pure composition: assemble the six application handlers from the ports and
    /// wrap them in the gRPC handler. No I/O — drives the unit/integration graph.
    pub fn compose(deps: AppDeps) -> AuthServiceHandler {
        // Every id_token sign-in also redeems its server-issued nonce.
        let federated: Arc<dyn FederatedTokenVerifier> =
            Arc::new(NonceBoundVerifier::new(Arc::clone(&deps.federated), Arc::clone(&deps.nonces)));
        let login = Arc::new(LoginHandler::new(
            Arc::clone(&deps.idp),
            Arc::clone(&deps.directory),
            Arc::clone(&deps.profiles),
            Arc::clone(&deps.links),
            Arc::clone(&deps.sessions),
            Arc::clone(&deps.refresh_tokens),
            Arc::clone(&deps.cache),
            Arc::clone(&deps.minter),
            Arc::clone(&deps.publisher),
            deps.policy.clone(),
        )
        .with_federated(Arc::clone(&federated), Arc::clone(&deps.guests))
        .with_codes(Arc::clone(&deps.codes)));
        let sign_up = Arc::new(SignUpHandler::new(
            Arc::clone(&federated),
            Arc::clone(&deps.directory),
            Arc::clone(&deps.links),
            Arc::clone(&deps.guests),
            MemberSessions {
                profiles: Arc::clone(&deps.profiles),
                sessions: Arc::clone(&deps.sessions),
                refresh_tokens: Arc::clone(&deps.refresh_tokens),
                cache: Arc::clone(&deps.cache),
                minter: Arc::clone(&deps.minter),
                publisher: Arc::clone(&deps.publisher),
                policy: deps.policy.clone(),
            },
        )
        .with_codes(Arc::clone(&deps.codes)));
        let refresh = Arc::new(RefreshHandler::new(
            Arc::clone(&deps.directory),
            Arc::clone(&deps.profiles),
            Arc::clone(&deps.sessions),
            Arc::clone(&deps.refresh_tokens),
            Arc::clone(&deps.cache),
            Arc::clone(&deps.minter),
            Arc::clone(&deps.publisher),
            deps.policy.clone(),
        ));
        let logout = Arc::new(LogoutHandler::new(
            Arc::clone(&deps.sessions),
            Arc::clone(&deps.refresh_tokens),
            Arc::clone(&deps.cache),
            Arc::clone(&deps.publisher),
            deps.policy.clone(),
        ));
        let logout_all = Arc::new(LogoutAllSessionsHandler::new(
            Arc::clone(&deps.sessions),
            Arc::clone(&deps.refresh_tokens),
            Arc::clone(&deps.cache),
            Arc::clone(&deps.publisher),
            deps.policy.clone(),
        ));
        let introspect =
            Arc::new(IntrospectHandler::new(Arc::clone(&deps.minter), Arc::clone(&deps.cache)));
        let list_sessions = Arc::new(ListSessionsHandler::new(Arc::clone(&deps.sessions)));
        let change_password = Arc::new(ChangePasswordHandler::new(
            Arc::clone(&deps.idp),
            Arc::clone(&deps.credentials),
            Arc::clone(&deps.sessions),
            Arc::clone(&deps.refresh_tokens),
            Arc::clone(&deps.cache),
            Arc::clone(&deps.publisher),
            deps.policy.clone(),
        ));
        let verify_credentials = Arc::new(VerifyCredentialsHandler::new(
            Arc::clone(&deps.idp),
            Arc::clone(&deps.credentials),
            Arc::clone(&deps.directory),
            Arc::clone(&deps.profiles),
            Arc::clone(&deps.sessions),
            Arc::clone(&deps.cache),
            Arc::clone(&deps.minter),
            deps.policy.clone(),
        ));
        let start_guest = Arc::new(StartGuestSessionHandler::new(
            Arc::clone(&deps.sessions),
            Arc::clone(&deps.refresh_tokens),
            Arc::clone(&deps.cache),
            Arc::clone(&deps.minter),
            Arc::clone(&deps.guests),
            deps.policy.clone(),
            deps.guest_sessions_enabled,
        ));

        AuthServiceHandler::new(
            login,
            refresh,
            logout,
            logout_all,
            introspect,
            list_sessions,
            start_guest,
            change_password,
            verify_credentials,
        )
        .with_sign_up(sign_up)
        .with_codes(deps.codes)
        .with_federated_nonces(deps.nonces)
    }

    /// Builds the concrete adapter graph from config + backend connections.
    pub async fn build(
        config: AuthConfig,
        backends: Backends,
    ) -> Result<App, Box<dyn std::error::Error>> {
        let pool = PgPoolBuilder::build(backends.postgres).await?;
        let tx = TransactionManager::new(pool.clone());
        let redis = RedisClientBuilder::new(backends.redis).build().await?;

        // The broker publisher is now the RELAY's sink, not the handlers':
        // handlers enqueue to the Postgres outbox (same fault domain as their
        // session writes) and the relay drains it in the background — TIER-0
        // login no longer hangs or fails on broker trouble, and a committed
        // session can't lose its compliance event (audit consumes these).
        let sink: Arc<dyn EventPublisher> = match backends.kafka {
            Some(cfg) => {
                let producer = KafkaProducerBuilder::new(ProducerConfig::new(cfg)).build()?;
                Arc::new(KafkaEventPublisher::new(producer))
            }
            None => Arc::new(LogEventPublisher),
        };
        let relay = OutboxRelay::new(pool.clone(), sink);
        let publisher: Arc<dyn EventPublisher> = Arc::new(PgOutboxPublisher::new(pool.clone()));

        // Lazy connect: the channel dials `account` on first use, so a cold start
        // does not require the dependency to be up at boot. Both deadlines are
        // mandatory — tonic has no default request timeout, and this channel sits
        // on the login hot path.
        let channel = Channel::from_shared(config.account_endpoint)?
            .timeout(config.account_rpc_timeout)
            .connect_timeout(config.account_connect_timeout)
            .connect_lazy();
        // Same shape for `profile` (the `pids` claim). Failures here are
        // fail-safe in the handlers, but a hung call must still bound the mint.
        let profile_channel = Channel::from_shared(config.profile_endpoint)?
            .timeout(config.profile_rpc_timeout)
            .connect_timeout(config.profile_connect_timeout)
            .connect_lazy();

        // reqwest's default client has no request timeout; the token exchange
        // must fail fast when the IdP hangs.
        let idp_client = reqwest::Client::builder()
            .timeout(config.idp_http_timeout)
            .connect_timeout(config.idp_connect_timeout)
            .build()?;

        // Build the concrete minter first: the JWKS is published from the same
        // ring, and only the concrete type can serialize it (the port stays
        // JWKS-agnostic — a PASETO minter would distribute keys differently).
        let minter = Es256TokenMinter::from_key_ring(config.signing, config.retiring_keys)?;
        let jwks_json = minter.jwks_json()?;

        let credentials: Arc<dyn CredentialAdmin> = if config.keycloak_admin.is_configured() {
            Arc::new(KeycloakCredentialAdmin::new(idp_client.clone(), config.keycloak_admin))
        } else {
            tracing::warn!(
                "AUTH_KEYCLOAK_ADMIN_URL / _CLIENT_ID / _CLIENT_SECRET not set — \
                 ChangePassword and VerifyCredentials answer UNAVAILABLE (AUT-5005)"
            );
            Arc::new(UnconfiguredCredentialAdmin)
        };

        // One-time codes: SMTP (Amazon SES) when configured, a log line for local
        // runs, otherwise off (StartVerification fails FAILED_PRECONDITION).
        let email_sender: Option<Arc<dyn CodeSender>> = match (config.verification_sender.as_str(), &config.smtp) {
            ("smtp", Some(smtp)) => Some(Arc::new(SmtpCodeSender::new(smtp.clone()).map_err(|e| e.to_string())?)),
            ("smtp", None) => return Err("AUTH_VERIFICATION_SENDER=smtp needs AUTH_SMTP_HOST".into()),
            ("log", _) => {
                tracing::warn!("one-time codes are only logged (AUTH_VERIFICATION_SENDER=log): local runs only");
                Some(Arc::new(LogCodeSender))
            }
            _ => None,
        };
        // SMS codes: SNS when configured; the local `log` sender covers SMS too.
        let sms_sender: Option<Arc<dyn CodeSender>> = match (config.sms_sender.as_str(), &config.sns) {
            ("sns", Some(sns)) => Some(Arc::new(SnsCodeSender::new(idp_client.clone(), sns.clone()))),
            ("sns", None) => return Err("AUTH_SMS_SENDER=sns needs AUTH_SNS_REGION".into()),
            _ if config.verification_sender == "log" => Some(Arc::new(LogCodeSender)),
            _ => None,
        };
        let code_sender: Arc<dyn CodeSender> =
            Arc::new(ChannelCodeSender { email: email_sender, sms: sms_sender });

        // Native Sign in with Apple / Google: a provider with no client id is off.
        let mut federated = JwksFederatedTokenVerifier::new();
        if let Some(apple) = JwksFederatedTokenVerifier::apple(config.apple_audiences.clone(), config.federated_jwks_timeout) {
            federated = federated.with(crate::domain::value_object::FederatedProvider::Apple, apple);
        }
        if let Some(google) = JwksFederatedTokenVerifier::google(config.google_audiences.clone(), config.federated_jwks_timeout) {
            federated = federated.with(crate::domain::value_object::FederatedProvider::Google, google);
        }
        // Keys fetched now and every AUTH_FEDERATED_JWKS_REFRESH_SECS: the first
        // sign-in pays no fetch, and a withdrawn key stops verifying.
        let federated = Arc::new(federated);
        if !config.apple_audiences.is_empty() || !config.google_audiences.is_empty() {
            federated.spawn_refresh(config.federated_jwks_refresh);
        }

        let erasure = Arc::new(AccountErasure::new(
            Arc::new(PgAccountEraser::new(tx.clone())),
            Arc::new(RedisSessionCache::new(redis.clone())),
        ));
        let guest_retention = GuestRetention::new(
            Arc::new(PgGuestRegistry::new(tx.clone())),
            chrono::Duration::days(config.guest_retention_days),
        );
        let deps = AppDeps {
            idp: Arc::new(KeycloakIdentityProvider::new(idp_client, config.keycloak)),
            credentials,
            directory: Arc::new(GrpcAccountDirectory::new(channel)),
            profiles: Arc::new(GrpcProfileDirectory::new(profile_channel)),
            links: Arc::new(PgSubjectLinkRepository::new(tx.clone())),
            sessions: Arc::new(PgSessionRepository::new(tx.clone())),
            refresh_tokens: Arc::new(PgRefreshTokenRepository::new(tx.clone())),
            cache: Arc::new(RedisSessionCache::new(redis.clone())),
            minter: Arc::new(minter),
            publisher,
            guests: Arc::new(PgGuestRegistry::new(tx.clone())),
            guest_sessions_enabled: config.guest_sessions_enabled,
            federated,
            codes: Arc::new(VerificationCodes::new(
                Arc::new(RedisVerificationStore::new(redis.clone())),
                code_sender,
                config.verification.clone(),
            )),
            nonces: Arc::new(FederatedNonces::new(
                Arc::new(RedisNonceStore::new(redis.clone())),
                config.federated_nonce_required,
            )),
            policy: config.policy,
        };

        Ok(App {
            handler: App::compose(deps).with_trusted_proxy_hops(config.trusted_proxy_hops),
            pool,
            redis,
            jwks_json,
            relay,
            guest_retention,
            erasure,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::application::fakes::Fixture;
    use crate::infrastructure::grpc::handler::proto;
    use tonic::{Code, Request};

    /// Composes the gRPC handler over the in-memory fakes — the exact graph
    /// `App::build` produces, minus the real backends.
    fn handler_from_fakes(fx: &Fixture) -> AuthServiceHandler {
        App::compose(AppDeps {
            idp: fx.idp.clone(),
            credentials: fx.credentials.clone(),
            directory: fx.directory.clone(),
            profiles: fx.profiles.clone(),
            links: fx.links.clone(),
            sessions: fx.sessions.clone(),
            refresh_tokens: fx.refresh_tokens.clone(),
            cache: fx.cache.clone(),
            minter: fx.minter.clone(),
            publisher: fx.publisher.clone(),
            guests: fx.guests.clone(),
            guest_sessions_enabled: true,
            federated: Arc::new(JwksFederatedTokenVerifier::new()),
            codes: Arc::new(VerificationCodes::new(
                Arc::new(crate::application::fakes::InMemoryVerificationStore::default()),
                Arc::new(crate::application::fakes::RecordingCodeSender::default()),
                crate::application::command::VerificationPolicy::default(),
            )),
            nonces: Arc::new(FederatedNonces::new(
                Arc::new(crate::application::fakes::InMemoryNonceStore::default()),
                true,
            )),
            policy: fx.policy.clone(),
        })
    }

    #[tokio::test]
    async fn login_rpc_maps_request_and_response() {
        let fx = Fixture::new();
        let handler = handler_from_fakes(&fx);

        let request = Request::new(proto::LoginRequest {
            device: Some(proto::DeviceContext {
                user_agent: "agent".into(),
                ip_address: String::new(),
                device_id: "dev-1".into(),
            }),
            grant_type: proto::GrantType::Password as i32,
            credential: Some(proto::login_request::Credential::Password(proto::PasswordGrant {
                username: "user".into(),
                password: "secret".into(),
            })),
            guest_refresh_token: String::new(),
        });

        let response = handler.login(request).await.unwrap().into_inner();
        assert!(!response.account_id.is_empty());
        assert!(response.first_link);
        let tokens = response.tokens.expect("token pair present");
        assert_eq!(tokens.token_type, "Bearer");
        assert!(!tokens.access_token.is_empty());
        assert!(!tokens.refresh_token.is_empty());
        assert_eq!(tokens.expires_in, 600);
    }

    #[tokio::test]
    async fn login_without_credential_is_invalid_argument() {
        let fx = Fixture::new();
        let handler = handler_from_fakes(&fx);
        let request = Request::new(proto::LoginRequest {
            device: None,
            grant_type: proto::GrantType::Unspecified as i32,
            credential: None,
            guest_refresh_token: String::new(),
        });
        let status = handler.login(request).await.unwrap_err();
        assert_eq!(status.code(), Code::InvalidArgument);
    }

    #[tokio::test]
    async fn logout_unknown_session_maps_to_not_found() {
        let fx = Fixture::new();
        let handler = handler_from_fakes(&fx);
        let request = Request::new(proto::LogoutRequest {
            session_id: crate::domain::value_object::SessionId::new().as_str(),
        });
        let status = handler.logout(request).await.unwrap_err();
        assert_eq!(status.code(), Code::NotFound);
    }
}
