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
use crate::infrastructure::client::{GrpcAudienceCheck, GrpcLikePositions, GrpcTargetDirectory};
use crate::infrastructure::event::{KafkaEventPublisher, LogEventPublisher};

use crate::app::{App, StakeDeps};
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
    /// `SpendGems` is mesh only: geo-discovery's country unlocks;
    /// `ExportWallet` and `ListStakePositions` too: account's GDPR export.
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
        // Likes (#665): post and comment over the mesh, the reader's access
        // from social-graph; stakes announced on wallet.v1.events through
        // the outbox.
        let targets = GrpcTargetDirectory::new(
            mesh_channel("WALLET_POST_GRPC_ENDPOINT", "http://localhost:50056")?,
            mesh_channel("WALLET_COMMENT_GRPC_ENDPOINT", "http://localhost:50057")?,
        );
        let audience = GrpcAudienceCheck::new(mesh_channel("WALLET_SOCIAL_GRAPH_GRPC_ENDPOINT", "http://localhost:50053")?);
        let positions = like_positions()?;
        let settles = positions.is_some();
        let deps = StakeDeps {
            targets:   Arc::new(targets),
            audience:  Arc::new(audience),
            publisher: build_publisher()?,
            positions,
        };
        let mut app = App::build(pool.clone(), WalletConfig::from_env(), Some(deps));
        // Its mesh-only RPCs check who calls them, and SpendGems who spends (#852).
        app.handler = app.handler.with_mesh_gate(service_runtime::mesh_gate_from_env(), service_runtime::staff_gate_from_env());
        spawn_outbox_drainer(Arc::clone(&app.wallets));
        if settles {
            spawn_settler(Arc::clone(&app.wallets));
            spawn_envelope(Arc::clone(&app.wallets));
        }
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

/// The outbox's publisher: Kafka, fail-closed — without `KAFKA_BROKERS` the
/// service does not start, unless `WALLET_ALLOW_LOG_PUBLISHER=true` (local
/// runs: events are logged, never lost silently in a deployed env).
fn build_publisher() -> anyhow::Result<Arc<dyn EventPublisher>> {
    if std::env::var("KAFKA_BROKERS").is_ok_and(|v| !v.trim().is_empty()) {
        let producer = KafkaProducerBuilder::new(ProducerConfig::new(KafkaClientConfig::from_env()))
            .build()
            .map_err(|e| anyhow::anyhow!("wallet kafka producer: {e}"))?;
        return Ok(Arc::new(KafkaEventPublisher::new(producer)));
    }
    if std::env::var("WALLET_ALLOW_LOG_PUBLISHER").is_ok_and(|v| v.trim() == "true") {
        tracing::warn!("KAFKA_BROKERS unset: wallet events are logged, not published (local only)");
        return Ok(Arc::new(LogEventPublisher));
    }
    anyhow::bail!("KAFKA_BROKERS is required (wallet.v1.events); set WALLET_ALLOW_LOG_PUBLISHER=true for local runs")
}

/// Publishes what the outbox holds, every `WALLET_OUTBOX_DRAIN_SECS` (5).
fn spawn_outbox_drainer(wallets: Arc<Wallets>) {
    let every = std::env::var("WALLET_OUTBOX_DRAIN_SECS").ok().and_then(|v| v.trim().parse().ok()).unwrap_or(5);
    tokio::spawn(async move {
        let mut tick = tokio::time::interval(Duration::from_secs(every.max(1)));
        loop {
            tick.tick().await;
            if let Err(error) = wallets.drain_outbox(500, chrono::Utc::now()).await {
                tracing::warn!(%error, "wallet outbox drain failed; retrying at the next tick");
            }
        }
    });
}

/// Stake settlement (#665, shadow mode) asks engagement at
/// `WALLET_ENGAGEMENT_GRPC_ENDPOINT` what each position came to. Unset: no
/// position settles (until the mesh route exists).
fn like_positions() -> anyhow::Result<Option<Arc<dyn crate::application::port::LikePositions>>> {
    if !std::env::var("WALLET_ENGAGEMENT_GRPC_ENDPOINT").is_ok_and(|v| !v.trim().is_empty()) {
        tracing::warn!("WALLET_ENGAGEMENT_GRPC_ENDPOINT unset: stake settlement is off");
        return Ok(None);
    }
    let channel = mesh_channel("WALLET_ENGAGEMENT_GRPC_ENDPOINT", "")?;
    Ok(Some(Arc::new(GrpcLikePositions::new(channel))))
}

/// Settles the positions due, every `WALLET_SETTLEMENT_SECS` (60), up to
/// `WALLET_SETTLEMENT_BATCH` (500) a pass. Shadow mode: no gems minted.
fn spawn_settler(wallets: Arc<Wallets>) {
    let env = |name: &str, default: u64| std::env::var(name).ok().and_then(|v| v.trim().parse().ok()).unwrap_or(default);
    let (every, batch) = (env("WALLET_SETTLEMENT_SECS", 60).max(1), env("WALLET_SETTLEMENT_BATCH", 500).max(1));
    tokio::spawn(async move {
        let mut tick = tokio::time::interval(Duration::from_secs(every));
        loop {
            tick.tick().await;
            match wallets.settle_due(batch as i64, chrono::Utc::now()).await {
                Ok(0) => {}
                Ok(settled) => tracing::info!(settled, "stake positions settled (shadow mode)"),
                Err(error) => tracing::warn!(%error, "stake settlement failed; retrying at the next tick"),
            }
        }
    });
}

/// Computes each finished day's curator envelope, checking every
/// `WALLET_ENVELOPE_SECS` (600). Shadow mode: provisional gems only.
fn spawn_envelope(wallets: Arc<Wallets>) {
    let every = std::env::var("WALLET_ENVELOPE_SECS").ok().and_then(|v| v.trim().parse().ok()).unwrap_or(600_u64);
    tokio::spawn(async move {
        let mut tick = tokio::time::interval(Duration::from_secs(every.max(1)));
        loop {
            tick.tick().await;
            if let Err(error) = wallets.run_envelopes(chrono::Utc::now()).await {
                tracing::warn!(%error, "curator envelope failed; retrying at the next tick");
            }
        }
    });
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
        // Another service's gem spend, and an account's export, never reach
        // the app.
        for method in [
            "/wallet.v1.WalletService/SpendGems",
            "/wallet.v1.WalletService/ExportWallet",
            "/wallet.v1.WalletService/ListStakePositions",
        ] {
            assert!(WalletService::EDGE_POLICY.iter().all(|rule| rule.method != method), "{method}");
        }
    }
}
