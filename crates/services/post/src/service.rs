//! Adapts the post composition root to the fleet [`service_runtime::Service`]
//! contract. Post is ScyllaDB-only and always publishes domain events through the
//! durable Kafka publisher.

use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use cqrs::command::InMemoryCommandBus;
use cqrs::query::InMemoryQueryBus;
use scylla_storage::ScyllaConfig;
use service_runtime::{HealthProbe, InfraRegistry, Service};
use service_runtime::edge::authenticated;
use service_runtime::EdgePolicy;
use tonic::service::RoutesBuilder;
use tonic_reflection::server::Builder as ReflectionBuilder;
use transport::kafka::config::{ConsumerConfig, KafkaClientConfig, ProducerConfig};
use transport::kafka::consumer::{KafkaConsumerBuilder, KafkaConsumerHandle};
use transport::kafka::producer::{KafkaProducerBuilder, KafkaProducerHandle};

use crate::app::{App, Backends};
use crate::application::port::AuthorTierStore;
use crate::infrastructure::consumer::{run_author_tier_consumer, run_moderation_consumer};
use crate::infrastructure::grpc::handler::post_service_handler::PostServiceServer;
use crate::infrastructure::grpc::handler::PostServiceHandler;
use crate::infrastructure::grpc::server::FILE_DESCRIPTOR_SET;
use crate::infrastructure::publisher::KafkaEventPublisher;

/// The profile event stream post denormalizes author tier from.
const PROFILE_EVENTS_TOPIC: &str = "profile.v1.events";
/// Consumer group for post's author-tier projection consumer.
const AUTHOR_TIER_GROUP: &str = "post-author-tier";
/// Moderation's decision stream; post holds the outcome for its reads.
const MODERATION_EVENTS_TOPIC: &str = "moderation.v1.events";
/// Consumer group for post's moderation-outcome consumer.
const MODERATION_GROUP: &str = "post-moderation";
/// Backoff before respawning the consumer after the runner returns.
const CONSUMER_RESPAWN_BACKOFF: Duration = Duration::from_secs(5);

type PostServer =
    PostServiceServer<PostServiceHandler<Arc<InMemoryCommandBus>, Arc<InMemoryQueryBus>>>;

/// The post service as hosted by [`service_runtime`].
pub struct PostService {
    app: App,
}

#[async_trait]
impl Service for PostService {
    const NAME: &'static str = "post";
    const VERSION: &'static str = env!("CARGO_PKG_VERSION");
    const GRPC_SERVICE_NAME: &'static str = <PostServer as tonic::server::NamedService>::NAME;

    /// The RPCs exposed on the client edge listener (`GRPC_EDGE_ADDR`); anything
    /// else on this service is mesh-only. See `transport::grpc::edge`.
    const EDGE_POLICY: EdgePolicy = &[
        authenticated("/post.v1.PostService/CreatePost"),
        authenticated("/post.v1.PostService/PublishPost"),
        authenticated("/post.v1.PostService/UpdatePost"),
        authenticated("/post.v1.PostService/DeletePost"),
        authenticated("/post.v1.PostService/GetPost"),
        authenticated("/post.v1.PostService/ListPostsByProfile"),
    ];

    async fn build(_infra: Arc<InfraRegistry>) -> anyhow::Result<Self> {
        let backends = Backends {
            scylla: ScyllaConfig::from_env(),
        };

        let producer = KafkaProducerBuilder::new(ProducerConfig::new(KafkaClientConfig::from_env()))
            .build()?;
        let publisher = Arc::new(KafkaEventPublisher::new(producer));

        let app = App::build(backends, publisher)
            .await
            .map_err(|e| anyhow::anyhow!("post app build: {e}"))?;

        // Inbound integration: profile tier signal → denormalized author-tier
        // projection (read on the publish path to stamp posts).
        spawn_author_tier_consumer(Arc::clone(&app.author_tier_store));
        // Inbound integration: moderation outcomes (takedowns, reversals) → the
        // restriction post's reads apply.
        spawn_moderation_consumer(Arc::clone(&app.command_bus));

        Ok(Self { app })
    }

    fn health_probes(&self) -> Vec<Arc<dyn HealthProbe>> {
        vec![scylla_storage::health::probe(Arc::clone(&self.app.scylla))]
    }

    fn register(self, routes: &mut RoutesBuilder) -> anyhow::Result<()> {
        let handler = PostServiceHandler::new(
            Arc::clone(&self.app.command_bus),
            Arc::clone(&self.app.query_bus),
        );
        let reflection = ReflectionBuilder::configure()
            .register_encoded_file_descriptor_set(FILE_DESCRIPTOR_SET)
            .build_v1()?;

        routes.add_service(reflection);
        routes.add_service(PostServiceServer::new(handler));
        Ok(())
    }
}

/// Spawns the supervised author-tier projection consumer (profile.v1.events →
/// `post.author_tiers`), respawning after a backoff whenever the runner returns.
fn spawn_author_tier_consumer(store: Arc<dyn AuthorTierStore>) {
    tokio::spawn(async move {
        loop {
            match build_author_tier_consumer() {
                Ok((consumer, producer)) => {
                    run_author_tier_consumer(consumer, Arc::clone(&store), producer).await;
                    tracing::warn!("author-tier consumer exited; respawning after backoff");
                }
                Err(error) => {
                    tracing::error!(%error, "failed to build author-tier consumer; retrying");
                }
            }
            tokio::time::sleep(CONSUMER_RESPAWN_BACKOFF).await;
        }
    });
}

/// Builds the manual-commit consumer (subscribed to `profile.v1.events`) and the
/// dead-letter producer the runner needs.
fn build_author_tier_consumer() -> anyhow::Result<(KafkaConsumerHandle, KafkaProducerHandle)> {
    build_consumer(AUTHOR_TIER_GROUP, PROFILE_EVENTS_TOPIC, "author-tier")
}

/// Spawns the supervised moderation-outcome consumer (moderation.v1.events →
/// the post's moderation restriction), respawning after a backoff whenever the
/// runner returns.
fn spawn_moderation_consumer(command_bus: Arc<InMemoryCommandBus>) {
    tokio::spawn(async move {
        loop {
            match build_consumer(MODERATION_GROUP, MODERATION_EVENTS_TOPIC, "moderation") {
                Ok((consumer, producer)) => {
                    run_moderation_consumer(consumer, Arc::clone(&command_bus), producer).await;
                    tracing::warn!("moderation consumer exited; respawning after backoff");
                }
                Err(error) => {
                    tracing::error!(%error, "failed to build moderation consumer; retrying");
                }
            }
            tokio::time::sleep(CONSUMER_RESPAWN_BACKOFF).await;
        }
    });
}

/// Builds a manual-commit consumer for `group` on `topic`, and the dead-letter
/// producer the runner needs.
fn build_consumer(
    group: &str,
    topic: &str,
    label: &str,
) -> anyhow::Result<(KafkaConsumerHandle, KafkaProducerHandle)> {
    let kafka = KafkaClientConfig::from_env();
    let consumer = KafkaConsumerBuilder::new(ConsumerConfig::new(kafka.clone(), group))
        .subscribe(topic)
        .build()
        .map_err(|e| anyhow::anyhow!("build {label} consumer: {e}"))?;
    let producer = KafkaProducerBuilder::new(ProducerConfig::new(kafka))
        .build()
        .map_err(|e| anyhow::anyhow!("build {label} dead-letter producer: {e}"))?;
    Ok((consumer, producer))
}
