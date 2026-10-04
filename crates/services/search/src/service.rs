//! Adapts the search composition root to the fleet [`service_runtime::Service`]
//! contract. Maps env → config, defers to [`App::build`], self-spawns the
//! ingestion consumers (post + moderation), registers the concrete tonic service,
//! and reports OpenSearch liveness via a ping probe.

use std::sync::Arc;
use std::time::Duration;

use anyhow::Context;
use async_trait::async_trait;
use service_runtime::{FnProbe, HealthProbe, InfraRegistry, Service};
use service_runtime::edge::public_read;
use service_runtime::EdgePolicy;
use tonic::service::RoutesBuilder;
use tonic_reflection::server::Builder as ReflectionBuilder;
use transport::kafka::config::{ConsumerConfig, KafkaClientConfig, ProducerConfig};
use transport::kafka::consumer::{KafkaConsumerBuilder, KafkaConsumerHandle};
use transport::kafka::producer::{KafkaProducerBuilder, KafkaProducerHandle};

use crate::app::App;
use crate::application::command::ProjectionHandler;
use crate::config::SearchConfig;
use crate::infrastructure::consumer::{
    run_moderation_consumer, run_post_consumer, run_profile_consumer,
};
use crate::infrastructure::grpc::{FILE_DESCRIPTOR_SET, SearchServiceHandler, SearchServiceServer};
use crate::infrastructure::hydrate::SourceHydrator;

const POST_TOPIC: &str = "post.v1.events";
const POST_GROUP: &str = "search-post-indexer";
const PROFILE_TOPIC: &str = "profile.v1.events";
const PROFILE_GROUP: &str = "search-profile-indexer";
const MODERATION_TOPIC: &str = "moderation.v1.events";
const MODERATION_GROUP: &str = "search-moderation-indexer";
/// Backoff before respawning a consumer after the runner returns.
const CONSUMER_RESPAWN_BACKOFF: Duration = Duration::from_secs(5);

type SearchServer = SearchServiceServer<SearchServiceHandler>;

/// The search service as hosted by [`service_runtime`].
pub struct SearchService {
    app: App,
}

#[async_trait]
impl Service for SearchService {
    const NAME: &'static str = "search";
    const VERSION: &'static str = env!("CARGO_PKG_VERSION");
    const GRPC_SERVICE_NAME: &'static str = <SearchServer as tonic::server::NamedService>::NAME;

    /// The RPCs exposed on the client edge listener (`GRPC_EDGE_ADDR`); anything
    /// else on this service is mesh-only. See `transport::grpc::edge`.
    const EDGE_POLICY: EdgePolicy = &[
        public_read("/search.v1.SearchService/Search"),
        public_read("/search.v1.SearchService/Suggest"),
        public_read("/search.v1.SearchService/MultiSearch"),
    ];

    async fn build(_infra: Arc<InfraRegistry>) -> anyhow::Result<Self> {
        let config = SearchConfig::from_env();
        let app = App::build(config)
            .await
            .map_err(|e| anyhow::anyhow!("search app build: {e}"))?;

        // Ingestion: post + profile content (hydrated) + moderation visibility.
        spawn_post_consumer(Arc::clone(&app.projection), Arc::clone(&app.hydrator));
        spawn_profile_consumer(Arc::clone(&app.projection), Arc::clone(&app.hydrator));
        spawn_moderation_consumer(Arc::clone(&app.projection));

        Ok(Self { app })
    }

    fn health_probes(&self) -> Vec<Arc<dyn HealthProbe>> {
        let index = Arc::clone(&self.app.index);
        vec![Arc::new(FnProbe::new("opensearch", move || {
            let index = Arc::clone(&index);
            async move { index.ping().await.map_err(anyhow::Error::from) }
        }))]
    }

    fn register(self, routes: &mut RoutesBuilder) -> anyhow::Result<()> {
        let reflection = ReflectionBuilder::configure()
            .register_encoded_file_descriptor_set(FILE_DESCRIPTOR_SET)
            .build_v1()?;

        routes.add_service(reflection);
        routes.add_service(SearchServiceServer::new(self.app.handler));
        Ok(())
    }
}

/// Spawns the supervised post-indexing consumer, rebuilding its Kafka handles and
/// restarting after a backoff whenever the runner returns.
fn spawn_post_consumer(projection: Arc<ProjectionHandler>, hydrator: Arc<dyn SourceHydrator>) {
    tokio::spawn(async move {
        loop {
            match build_consumer(POST_TOPIC, POST_GROUP) {
                Ok((consumer, producer)) => {
                    run_post_consumer(
                        consumer,
                        Arc::clone(&projection),
                        Arc::clone(&hydrator),
                        producer,
                    )
                    .await;
                    tracing::warn!("post consumer exited; respawning after backoff");
                }
                Err(error) => tracing::error!(%error, "failed to build post consumer; retrying"),
            }
            tokio::time::sleep(CONSUMER_RESPAWN_BACKOFF).await;
        }
    });
}

/// Spawns the supervised profile-indexing consumer.
fn spawn_profile_consumer(projection: Arc<ProjectionHandler>, hydrator: Arc<dyn SourceHydrator>) {
    tokio::spawn(async move {
        loop {
            match build_consumer(PROFILE_TOPIC, PROFILE_GROUP) {
                Ok((consumer, producer)) => {
                    run_profile_consumer(
                        consumer,
                        Arc::clone(&projection),
                        Arc::clone(&hydrator),
                        producer,
                    )
                    .await;
                    tracing::warn!("profile consumer exited; respawning after backoff");
                }
                Err(error) => {
                    tracing::error!(%error, "failed to build profile consumer; retrying")
                }
            }
            tokio::time::sleep(CONSUMER_RESPAWN_BACKOFF).await;
        }
    });
}

/// Spawns the supervised moderation-visibility consumer.
fn spawn_moderation_consumer(projection: Arc<ProjectionHandler>) {
    tokio::spawn(async move {
        loop {
            match build_consumer(MODERATION_TOPIC, MODERATION_GROUP) {
                Ok((consumer, producer)) => {
                    run_moderation_consumer(consumer, Arc::clone(&projection), producer).await;
                    tracing::warn!("moderation consumer exited; respawning after backoff");
                }
                Err(error) => {
                    tracing::error!(%error, "failed to build moderation consumer; retrying")
                }
            }
            tokio::time::sleep(CONSUMER_RESPAWN_BACKOFF).await;
        }
    });
}

/// Builds a manual-commit consumer (subscribed to `topic`) and the dead-letter
/// producer the runner needs.
fn build_consumer(
    topic: &str,
    group: &str,
) -> anyhow::Result<(KafkaConsumerHandle, KafkaProducerHandle)> {
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
