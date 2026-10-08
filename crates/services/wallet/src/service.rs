//! Adapts the wallet composition root to the fleet [`service_runtime::Service`]
//! contract: the Postgres pool, the account-erasure consumer, and the
//! `wallet.v1` routes.

use std::sync::Arc;
use std::time::Duration;

use anyhow::Context;
use async_trait::async_trait;
use postgres_storage::{PgPoolBuilder, PostgresConfig};
use service_runtime::edge::authenticated;
use service_runtime::{EdgePolicy, HealthProbe, InfraRegistry, Service};
use sqlx::PgPool;
use tonic::service::RoutesBuilder;
use tonic_reflection::server::Builder as ReflectionBuilder;
use transport::kafka::config::{ConsumerConfig, KafkaClientConfig, ProducerConfig};
use transport::kafka::consumer::{KafkaConsumerBuilder, KafkaConsumerHandle};
use transport::kafka::producer::{KafkaProducerBuilder, KafkaProducerHandle};

use crate::application::port::EventPublisher;
use crate::infrastructure::client::GrpcTargetDirectory;
use crate::infrastructure::event::{KafkaEventPublisher, LogEventPublisher};

use crate::app::App;
use crate::application::Wallets;
use crate::config::WalletConfig;
use crate::infrastructure::consumer::run_account_consumer;
use crate::infrastructure::grpc::{WalletServiceHandler, WalletServiceServer, FILE_DESCRIPTOR_SET};

const ACCOUNT_TOPIC: &str = "account.v1.events";
const ACCOUNT_GROUP: &str = "wallet-account-events";
/// Backoff before respawning a consumer after the runner returns.
const CONSUMER_RESPAWN_BACKOFF: Duration = Duration::from_secs(5);

type WalletServer = WalletServiceServer<WalletServiceHandler>;

/// The wallet service as hosted by [`service_runtime`].
pub struct WalletService {
    app:  App,
    pool: PgPool,
}

#[async_trait]
impl Service for WalletService {
    const NAME: &'static str = "wallet";
    const VERSION: &'static str = env!("CARGO_PKG_VERSION");
    const GRPC_SERVICE_NAME: &'static str = <WalletServer as tonic::server::NamedService>::NAME;

    /// The caller's own wallet only (`account_id` bound to the token).
    /// `SpendGems` is mesh only: geo-discovery's country unlocks.
    const EDGE_POLICY: EdgePolicy = &[
        authenticated("/wallet.v1.WalletService/GetWallet"),
        authenticated("/wallet.v1.WalletService/ClaimReward"),
        authenticated("/wallet.v1.WalletService/ListWalletTransactions"),
        authenticated("/wallet.v1.WalletService/BuyStakePack"),
        // Likes (#665): the caller's account and profile.
        authenticated("/wallet.v1.WalletService/Stake"),
    ];

    async fn build(_infra: Arc<InfraRegistry>) -> anyhow::Result<Self> {
        let pool = PgPoolBuilder::build(PostgresConfig::from_env())
            .await
            .map_err(|e| anyhow::anyhow!("wallet postgres pool: {e}"))?;
        // Likes (#665): post and comment over the mesh; stakes announced on
        // wallet.v1.events.
        let targets = GrpcTargetDirectory::new(
            mesh_channel("WALLET_POST_GRPC_ENDPOINT", "http://localhost:50056")?,
            mesh_channel("WALLET_COMMENT_GRPC_ENDPOINT", "http://localhost:50057")?,
        );
        let publisher: Arc<dyn EventPublisher> = if std::env::var("KAFKA_BROKERS").is_ok() {
            let producer = KafkaProducerBuilder::new(ProducerConfig::new(KafkaClientConfig::from_env()))
                .build()
                .map_err(|e| anyhow::anyhow!("wallet kafka producer: {e}"))?;
            Arc::new(KafkaEventPublisher::new(producer))
        } else {
            Arc::new(LogEventPublisher)
        };
        let app = App::build(pool.clone(), WalletConfig::from_env(), Some((Arc::new(targets), publisher)));
        // A deleted account's wallet goes with it.
        spawn_account_consumer(Arc::clone(&app.wallets));
        Ok(Self { app, pool })
    }

    fn health_probes(&self) -> Vec<Arc<dyn HealthProbe>> {
        vec![postgres_storage::health::probe(self.pool.clone())]
    }

    fn register(self, routes: &mut RoutesBuilder) -> anyhow::Result<()> {
        let reflection = ReflectionBuilder::configure()
            .register_encoded_file_descriptor_set(FILE_DESCRIPTOR_SET)
            .build_v1()?;
        routes.add_service(reflection);
        routes.add_service(WalletServiceServer::new(self.app.handler));
        Ok(())
    }
}

/// A lazily-connected mesh channel to `env_key` (default `default`), 1 s
/// request and 500 ms connect deadlines.
fn mesh_channel(env_key: &str, default: &str) -> anyhow::Result<tonic::transport::Channel> {
    let endpoint = std::env::var(env_key).ok().filter(|v| !v.trim().is_empty()).unwrap_or_else(|| default.to_owned());
    Ok(tonic::transport::Channel::from_shared(endpoint)
        .map_err(|e| anyhow::anyhow!("invalid {env_key}: {e}"))?
        .timeout(Duration::from_millis(1000))
        .connect_timeout(Duration::from_millis(500))
        .connect_lazy())
}

/// Spawns the supervised account consumer.
fn spawn_account_consumer(wallets: Arc<Wallets>) {
    tokio::spawn(async move {
        loop {
            match build_consumer(ACCOUNT_TOPIC, ACCOUNT_GROUP) {
                Ok((consumer, producer)) => {
                    run_account_consumer(consumer, Arc::clone(&wallets), producer).await;
                    tracing::warn!("wallet account consumer exited; respawning after backoff");
                }
                Err(error) => tracing::error!(%error, "failed to build the account consumer; retrying"),
            }
            tokio::time::sleep(CONSUMER_RESPAWN_BACKOFF).await;
        }
    });
}

/// A manual-commit consumer (subscribed to `topic`) and the dead-letter
/// producer the runner needs.
fn build_consumer(topic: &str, group: &str) -> anyhow::Result<(KafkaConsumerHandle, KafkaProducerHandle)> {
    let kafka = KafkaClientConfig::from_env();
    let consumer = KafkaConsumerBuilder::new(ConsumerConfig::new(kafka.clone(), group))
        .subscribe(topic)
        .build()
        .with_context(|| format!("build consumer for {topic}"))?;
    let producer = KafkaProducerBuilder::new(ProducerConfig::new(kafka))
        .build()
        .with_context(|| format!("build dead-letter producer for {topic}"))?;
    Ok((consumer, producer))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_edge_exposes_the_callers_own_wallet_only() {
        assert_eq!(WalletService::EDGE_POLICY.len(), 5);
        assert!(service_runtime::edge::validate_policy(WalletService::EDGE_POLICY).is_ok());
        // Another service's gem spend never reaches the app.
        let spend = "/wallet.v1.WalletService/SpendGems";
        assert!(WalletService::EDGE_POLICY.iter().all(|rule| rule.method != spend));
    }
}
