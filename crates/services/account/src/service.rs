//! Adapts the account composition root to the fleet [`service_runtime::Service`]
//! contract. Account is PostgreSQL-backed; the pool is built here, shared into
//! [`App::build`], and reused (it is `Clone`/`Arc`-backed) for the readiness probe.

use std::sync::Arc;

use async_trait::async_trait;
use cqrs::command::InMemoryCommandBus;
use cqrs::query::InMemoryQueryBus;
use postgres_storage::{PgPoolBuilder, PostgresConfig};
use service_runtime::{HealthProbe, InfraRegistry, Service};
use service_runtime::edge::authenticated;
use service_runtime::EdgePolicy;
use sqlx::PgPool;
use tonic::service::RoutesBuilder;
use tonic_reflection::server::Builder as ReflectionBuilder;

use crate::app::App;
use crate::application::port::EventPublisher;
use crate::infrastructure::event::{KafkaEventPublisher, LogEventPublisher};
use crate::infrastructure::grpc::handler::account_service_handler::AccountServiceServer;
use crate::infrastructure::grpc::handler::AccountServiceHandler;
use crate::infrastructure::grpc::server::FILE_DESCRIPTOR_SET;
use transport::kafka::config::{KafkaClientConfig, ProducerConfig};
use transport::kafka::producer::KafkaProducerBuilder;

type AccountServer =
    AccountServiceServer<AccountServiceHandler<Arc<InMemoryCommandBus>, Arc<InMemoryQueryBus>>>;

/// The account service as hosted by [`service_runtime`].
pub struct AccountService {
    app: App,
    pool: PgPool,
}

#[async_trait]
impl Service for AccountService {
    const NAME: &'static str = "account";
    const VERSION: &'static str = env!("CARGO_PKG_VERSION");
    const GRPC_SERVICE_NAME: &'static str = <AccountServer as tonic::server::NamedService>::NAME;

    /// The RPCs exposed on the client edge listener (`GRPC_EDGE_ADDR`); anything
    /// else on this service is mesh-only. See `transport::grpc::edge`.
    // Self-service only: every listed RPC binds `account_id` to the token subject.
    // Admin/compliance RPCs (KYC, suspend, roles, listing) and the
    // auth-plane writes (RecordLogin, Anonymize, CreateAccount) stay mesh-only
    // until a staff permission catalogue exists.
    const EDGE_POLICY: EdgePolicy = &[
        authenticated("/account.v1.AccountService/VerifyEmail"),
        authenticated("/account.v1.AccountService/VerifyPhone"),
        authenticated("/account.v1.AccountService/ChangePassword"),
        authenticated("/account.v1.AccountService/EnrollMfa"),
        authenticated("/account.v1.AccountService/RevokeMfa"),
        authenticated("/account.v1.AccountService/DeactivateAccount"),
        authenticated("/account.v1.AccountService/RequestGdprDeletion"),
        authenticated("/account.v1.AccountService/RequestDataExport"),
        // The holder's own GDPR record and consents (GDPR Art. 7, 15).
        authenticated("/account.v1.AccountService/GetGdprRecord"),
        authenticated("/account.v1.AccountService/UpdateConsents"),
        authenticated("/account.v1.AccountService/GetAccountById"),
        authenticated("/account.v1.AccountService/GetAccountStatus"),
    ];

    async fn build(_infra: Arc<InfraRegistry>) -> anyhow::Result<Self> {
        let pool = PgPoolBuilder::build(PostgresConfig::from_env())
            .await
            .map_err(|e| anyhow::anyhow!("account postgres pool: {e}"))?;

        // Publish account.v1.events to Kafka when a broker is configured; otherwise
        // a no-op log publisher keeps local/dev runs broker-free.
        let publisher = build_publisher()?;

        // `PgPool` is `Arc`-backed: one clone serves the app graph, one the probe.
        let app = App::build(pool.clone(), publisher)
            .await
            .map_err(|e| anyhow::anyhow!("account app build: {e}"))?;

        Ok(Self { app, pool })
    }

    fn health_probes(&self) -> Vec<Arc<dyn HealthProbe>> {
        vec![postgres_storage::health::probe(self.pool.clone())]
    }

    fn register(self, routes: &mut RoutesBuilder) -> anyhow::Result<()> {
        // Step-up on account deletion / deactivation: off until clients call
        // auth.v1.VerifyCredentials first (then flip it per environment).
        let require_step_up = std::env::var("ACCOUNT_REQUIRE_STEP_UP")
            .is_ok_and(|v| matches!(v.trim(), "1" | "true" | "TRUE" | "yes"));
        let handler = AccountServiceHandler::new(
            Arc::clone(&self.app.command_bus),
            Arc::clone(&self.app.query_bus),
        )
        .with_step_up(require_step_up);
        let reflection = ReflectionBuilder::configure()
            .register_encoded_file_descriptor_set(FILE_DESCRIPTOR_SET)
            .build_v1()?;

        routes.add_service(reflection);
        routes.add_service(AccountServiceServer::new(handler));
        Ok(())
    }
}

/// Builds the account event publisher: Kafka when `KAFKA_BROKERS` is set,
/// otherwise a no-op log publisher (broker-free local/dev).
fn build_publisher() -> anyhow::Result<Arc<dyn EventPublisher>> {
    if std::env::var("KAFKA_BROKERS").is_ok() {
        let producer =
            KafkaProducerBuilder::new(ProducerConfig::new(KafkaClientConfig::from_env()))
                .build()
                .map_err(|e| anyhow::anyhow!("account kafka producer: {e}"))?;
        Ok(Arc::new(KafkaEventPublisher::new(producer)))
    } else {
        Ok(Arc::new(LogEventPublisher))
    }
}
