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
use crate::application::command::{AnonymizeDueAccounts, ExportDueData, PeerExportSources};
use crate::infrastructure::directory::MeshProfileDirectory;
use crate::infrastructure::export::{ExportStoreConfig, MeshEndpoints, MeshExportPeers, S3ExportStore};
use crate::infrastructure::worker::export_pass::run_export_pass;
use crate::infrastructure::worker::gdpr_janitor::run_gdpr_janitor;
use crate::infrastructure::worker::supervision_sweep::run_supervision_sweep;
use crate::application::port::{EventPublisher, ExportStore};
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
    // auth-plane writes (RecordLogin, Anonymize, CreateAccount) and every MFA
    // RPC (auth's, #649: it holds the seed key and the step-up) stay mesh-only
    // until a staff permission catalogue exists. VerifyEmail / VerifyPhone are
    // auth-plane writes too: they carry no proof, so only auth calls them, once
    // the holder proved the address (a verified id_token or a one-time code).
    const EDGE_POLICY: EdgePolicy = &[
        authenticated("/account.v1.AccountService/ChangePassword"),
        authenticated("/account.v1.AccountService/DeactivateAccount"),
        authenticated("/account.v1.AccountService/RequestGdprDeletion"),
        authenticated("/account.v1.AccountService/CancelGdprDeletion"),
        authenticated("/account.v1.AccountService/RequestDataExport"),
        // The holder's own GDPR record and consents (GDPR Art. 7, 15).
        authenticated("/account.v1.AccountService/GetGdprRecord"),
        authenticated("/account.v1.AccountService/FindProfilesByContacts"),
        authenticated("/account.v1.AccountService/UpdateConsents"),
        authenticated("/account.v1.AccountService/GetAccountById"),
        authenticated("/account.v1.AccountService/GetAccountStatus"),
        // The holder's own date of birth, once (accounts created without one).
        authenticated("/account.v1.AccountService/SetDateOfBirth"),
        // #670: family supervision, the caller's own account.
        authenticated("/account.v1.AccountService/CreateSupervisionInvite"),
        authenticated("/account.v1.AccountService/AcceptSupervisionInvite"),
        authenticated("/account.v1.AccountService/ListSupervisions"),
        authenticated("/account.v1.AccountService/EndSupervision"),
    ];

    async fn build(_infra: Arc<InfraRegistry>) -> anyhow::Result<Self> {
        let pool = PgPoolBuilder::build(PostgresConfig::from_env())
            .await
            .map_err(|e| anyhow::anyhow!("account postgres pool: {e}"))?;

        // Publish account.v1.events to Kafka when a broker is configured; otherwise
        // a no-op log publisher keeps local/dev runs broker-free.
        let publisher = build_publisher()?;

        // The GDPR data export (#653): on only with a store (ACCOUNT_EXPORT_*);
        // without it an export request stays pending.
        let exports = match ExportStoreConfig::from_env() {
            Some(config) => Some(Arc::new(
                S3ExportStore::new(config).map_err(|e| anyhow::anyhow!("account export store: {e}"))?,
            )),
            None => {
                tracing::warn!("ACCOUNT_EXPORT_BUCKET unset: GDPR data exports stay pending");
                None
            }
        };

        // `PgPool` is `Arc`-backed: one clone serves the app graph, one the probe.
        // Contact matching (#661) reads profile and social-graph over the mesh
        // (the export's endpoints).
        let endpoints = MeshEndpoints::from_env();
        let directory = MeshProfileDirectory::new(&endpoints.profile, &endpoints.social_graph)
            .map_err(|e| anyhow::anyhow!("account profile directory: {e}"))?;
        let app = App::build_with_exports(
            pool.clone(),
            publisher,
            exports.clone().map(|store| store as Arc<dyn ExportStore>),
            Some(Arc::new(directory)),
        )
        .await
        .map_err(|e| anyhow::anyhow!("account app build: {e}"))?;

        // The export pass: gathers each pending account's data over the mesh
        // (ACCOUNT_EXPORT_INTERVAL_SECS, default 300; 0 = off).
        let export_secs = std::env::var("ACCOUNT_EXPORT_INTERVAL_SECS")
            .ok()
            .and_then(|v| v.trim().parse::<u64>().ok())
            .unwrap_or(300);
        if let (Some(store), true) = (exports, export_secs > 0) {
            let peers = MeshExportPeers::new(&MeshEndpoints::from_env())
                .map_err(|e| anyhow::anyhow!("account export peers: {e}"))?;
            let pass = Arc::new(ExportDueData::new(
                Arc::clone(&app.repository),
                Arc::new(PeerExportSources::new(Arc::new(peers))),
                store,
            ));
            tokio::spawn(run_export_pass(pass, std::time::Duration::from_secs(export_secs)));
        }

        // The GDPR janitor: anonymizes accounts whose erasure grace period has
        // ended (ACCOUNT_GDPR_JANITOR_INTERVAL_SECS, default hourly; 0 = off).
        let interval_secs = std::env::var("ACCOUNT_GDPR_JANITOR_INTERVAL_SECS")
            .ok()
            .and_then(|v| v.trim().parse::<u64>().ok())
            .unwrap_or(3_600);
        if interval_secs > 0 {
            let janitor = AnonymizeDueAccounts::new(Arc::clone(&app.repository));
            // An erased account's supervisions end first (#670).
            let janitor = Arc::new(match &app.supervisions {
                Some(supervisions) => janitor.with_supervisions(Arc::clone(supervisions)),
                None => janitor,
            });
            tokio::spawn(run_gdpr_janitor(janitor, std::time::Duration::from_secs(interval_secs)));
        }

        // Family supervision's daily pass (#670): supervisions whose teen
        // turned 18 end, expired invites go
        // (ACCOUNT_SUPERVISION_SWEEP_INTERVAL_SECS, default hourly; 0 = off).
        let sweep_secs = std::env::var("ACCOUNT_SUPERVISION_SWEEP_INTERVAL_SECS")
            .ok()
            .and_then(|v| v.trim().parse::<u64>().ok())
            .unwrap_or(3_600);
        if let (Some(supervisions), true) = (&app.supervisions, sweep_secs > 0) {
            tokio::spawn(run_supervision_sweep(Arc::clone(supervisions), std::time::Duration::from_secs(sweep_secs)));
        }

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
        .with_step_up(require_step_up)
        .with_supervisions(self.app.supervisions.clone());
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

#[cfg(test)]
mod tests {
    use super::*;

    /// Marking an address verified takes a proof only auth checks: these RPCs
    /// must never be reachable on the client edge.
    #[test]
    fn verification_writes_are_mesh_only() {
        for method in [
            "/account.v1.AccountService/VerifyEmail",
            "/account.v1.AccountService/VerifyPhone",
            // MFA material and its writes are auth's (#649).
            "/account.v1.AccountService/EnrollMfa",
            "/account.v1.AccountService/RevokeMfa",
            "/account.v1.AccountService/GetMfaSecret",
            "/account.v1.AccountService/ConsumeRecoveryCode",
            "/account.v1.AccountService/ReplaceRecoveryCodes",
        ] {
            assert!(
                AccountService::EDGE_POLICY.iter().all(|rule| rule.method != method),
                "{method} is on the edge"
            );
        }
    }
}
