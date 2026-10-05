//! Adapts the auth composition root to the fleet [`service_runtime::Service`]
//! contract. Maps env → config, defers to [`App::build`], registers the concrete
//! tonic service, and reports Postgres + Redis liveness via the storage crates'
//! ready-made probes over the connections `App` retains.

use std::sync::Arc;

use async_trait::async_trait;
use postgres_storage::PostgresConfig;
use redis_storage::RedisConfig;
use service_runtime::{HealthProbe, InfraRegistry, Service};
use service_runtime::edge::{authenticated, public};
use service_runtime::EdgePolicy;
use tonic::service::RoutesBuilder;
use tonic_reflection::server::Builder as ReflectionBuilder;
use transport::kafka::config::client::KafkaClientConfig;

use crate::app::{App, Backends};
use crate::config::AuthConfig;
use crate::infrastructure::grpc::handler::{AuthServiceHandler, AuthServiceServer};
use crate::infrastructure::grpc::server::FILE_DESCRIPTOR_SET;

/// The concrete tonic server type, named once so the health key and the
/// reflection registration agree.
type AuthServer = AuthServiceServer<AuthServiceHandler>;

/// The auth service as hosted by [`service_runtime`].
pub struct AuthService {
    app: App,
}

#[async_trait]
impl Service for AuthService {
    const NAME: &'static str = "auth";
    const VERSION: &'static str = env!("CARGO_PKG_VERSION");
    const GRPC_SERVICE_NAME: &'static str = <AuthServer as tonic::server::NamedService>::NAME;

    /// The RPCs exposed on the client edge listener (`GRPC_EDGE_ADDR`); anything
    /// else on this service is mesh-only. See `transport::grpc::edge`.
    // Login/Refresh/StartGuestSession are the only anonymous RPCs in the fleet
    // (they *produce* the session). Introspect is INTERNAL and stays mesh-only.
    const EDGE_POLICY: EdgePolicy = &[
        public("/auth.v1.AuthService/Login"),
        public("/auth.v1.AuthService/Refresh"),
        // Guest mode: an installation starts an anonymous, read-only session.
        // Abuse controls (App Attest, per-IP / per-device limits) come with B5.
        public("/auth.v1.AuthService/StartGuestSession"),
        // Sign-up with a native Apple / Google id_token (proof of the identity
        // is the verified token; the edge has no session yet).
        public("/auth.v1.AuthService/SignUp"),
        // One-time codes to an email address (passwordless sign-up / sign-in);
        // budgeted per address here and per IP at the edge.
        public("/auth.v1.AuthService/StartVerification"),
        // A single-use nonce for native Sign in with Apple / Google.
        public("/auth.v1.AuthService/StartFederatedSignIn"),
        authenticated("/auth.v1.AuthService/Logout"),
        authenticated("/auth.v1.AuthService/LogoutAllSessions"),
        authenticated("/auth.v1.AuthService/ListSessions"),
        // The signed-in holder's credentials (never a guest's: there is none).
        authenticated("/auth.v1.AuthService/ChangePassword"),
        authenticated("/auth.v1.AuthService/VerifyCredentials"),
    ];

    async fn build(_infra: Arc<InfraRegistry>) -> anyhow::Result<Self> {
        let config = AuthConfig::from_env()?;
        let backends = Backends {
            postgres: PostgresConfig::from_env(),
            redis: RedisConfig::from_env(),
            kafka: Some(KafkaClientConfig::from_env()),
        };

        // `App::build` errors are `Box<dyn Error>` (not `Send + Sync`); flatten to
        // a message rather than propagating the box into `anyhow`.
        let app = App::build(config, backends)
            .await
            .map_err(|e| anyhow::anyhow!("auth app build: {e}"))?;

        // JWKS over plain HTTP, next to the gRPC server: downstream verifiers
        // (realtime, audit via auth-context) fetch AUTH_JWKS_URL with ordinary
        // JWKS clients. Bind failures abort boot — a mesh whose verifiers
        // can't fetch keys is fail-closed everywhere downstream, so
        // crash-looping visibly here beats serving logins nobody can verify.
        let jwks_addr: std::net::SocketAddr = std::env::var("AUTH_JWKS_HTTP_ADDR")
            .unwrap_or_else(|_| "0.0.0.0:8081".to_owned())
            .parse()?;
        let jwks_json = app.jwks_json.clone();
        let listener = tokio::net::TcpListener::bind(jwks_addr).await?;
        tracing::info!(%jwks_addr, "auth.jwks http listener bound");
        tokio::spawn(async move {
            if let Err(error) =
                axum::serve(listener, crate::infrastructure::http::jwks::router(jwks_json)).await
            {
                tracing::error!(%error, "auth.jwks http listener exited");
            }
        });

        // Outbox relay: the only path from committed auth events to the
        // broker. Interval is env-tunable; 1s keeps worst-case event latency
        // well under the audit plane's freshness expectations.
        let relay_interval = std::time::Duration::from_millis(
            std::env::var("AUTH_OUTBOX_RELAY_INTERVAL_MS")
                .ok()
                .and_then(|v| v.parse().ok())
                .unwrap_or(1_000),
        );
        tokio::spawn(app.relay.clone().run(relay_interval));

        Ok(Self { app })
    }

    fn health_probes(&self) -> Vec<Arc<dyn HealthProbe>> {
        vec![
            postgres_storage::health::probe(self.app.pool.clone()),
            redis_storage::health::probe(self.app.redis.clone()),
        ]
    }

    fn register(self, routes: &mut RoutesBuilder) -> anyhow::Result<()> {
        let reflection = ReflectionBuilder::configure()
            .register_encoded_file_descriptor_set(FILE_DESCRIPTOR_SET)
            .build_v1()?;

        routes.add_service(reflection);
        routes.add_service(AuthServiceServer::new(self.app.handler));
        Ok(())
    }
}
