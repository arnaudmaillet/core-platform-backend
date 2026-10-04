//! Adapts the profile composition root to the fleet [`service_runtime::Service`]
//! contract so the shared runtime can host it.
//!
//! Unlike chat, profile *consumes* the externalized infra registry: its cache
//! TTLs come from the `[cache]` section via [`InfraRegistry::cache`], so a TTL
//! push hot-reloads the live cache. Profile also has an inbound integration —
//! the account-event consumer — which is self-spawned in [`build`](ProfileService::build)
//! (mirroring how chat spawns its workers in `App::build`), so the single runtime
//! entrypoint needs no service-specific hooks.

use std::sync::Arc;
use std::time::Duration;

use anyhow::Context;
use async_trait::async_trait;
use cqrs::command::InMemoryCommandBus;
use cqrs::query::InMemoryQueryBus;
use infra_config::InfraRegistry;
use redis_storage::RedisConfig;
use scylla_storage::ScyllaConfig;
use service_runtime::{HealthProbe, Service};
use service_runtime::edge::{authenticated, public_read};
use service_runtime::EdgePolicy;
use tonic::service::RoutesBuilder;
use tonic_reflection::server::Builder as ReflectionBuilder;
use transport::kafka::config::{ConsumerConfig, KafkaClientConfig, ProducerConfig};
use transport::kafka::consumer::{KafkaConsumerBuilder, KafkaConsumerHandle};
use transport::kafka::producer::{KafkaProducerBuilder, KafkaProducerHandle};

use crate::app::{App, Backends};
use crate::application::port::EventPublisher;
use crate::infrastructure::consumer::{run_account_event_consumer, run_author_tier_consumer};
use crate::infrastructure::grpc::server::FILE_DESCRIPTOR_SET;
use crate::infrastructure::grpc::{ProfileServiceHandler, ProfileServiceServer};
use crate::infrastructure::publisher::KafkaProfileEventPublisher;

/// Kafka topic carrying the account lifecycle events profile reacts to.
const ACCOUNT_EVENTS_TOPIC: &str = "account.v1.events";
/// Consumer group for profile's account-event consumer.
const ACCOUNT_EVENTS_GROUP: &str = "profile-account-events";
/// Kafka topic carrying the author-tier signal profile denormalizes.
const AUTHOR_TIER_TOPIC: &str = "social-graph.author_tier_changed";
/// Consumer group for profile's author-tier consumer.
const AUTHOR_TIER_GROUP: &str = "profile-author-tier";
/// Backoff before respawning the consumer after the runner returns.
const CONSUMER_RESPAWN_BACKOFF: Duration = Duration::from_secs(5);

/// The concrete tonic server type for profile (the buses are shared as `Arc`,
/// which implement the bus traits), named once so the health key and reflection
/// registration agree.
type ProfileServer =
    ProfileServiceServer<ProfileServiceHandler<Arc<InMemoryCommandBus>, Arc<InMemoryQueryBus>>>;

/// The profile service as hosted by [`service_runtime`].
pub struct ProfileService {
    app: App,
}

#[async_trait]
impl Service for ProfileService {
    const NAME: &'static str = "profile";
    const VERSION: &'static str = env!("CARGO_PKG_VERSION");
    const GRPC_SERVICE_NAME: &'static str = <ProfileServer as tonic::server::NamedService>::NAME;

    /// The RPCs exposed on the client edge listener (`GRPC_EDGE_ADDR`); anything
    /// else on this service is mesh-only. See `transport::grpc::edge`.
    // VerifyProfile (admin) and Hide/RestoreProfile (moderation axis, event-driven)
    // stay mesh-only.
    const EDGE_POLICY: EdgePolicy = &[
        authenticated("/profile.v1.ProfileService/CreateProfile"),
        authenticated("/profile.v1.ProfileService/UpdateProfile"),
        authenticated("/profile.v1.ProfileService/ChangeHandle"),
        authenticated("/profile.v1.ProfileService/UpdateAvatar"),
        authenticated("/profile.v1.ProfileService/UpdateBanner"),
        authenticated("/profile.v1.ProfileService/SetVisibility"),
        authenticated("/profile.v1.ProfileService/SetInteractionSettings"),
        authenticated("/profile.v1.ProfileService/SetLocationSettings"),
        authenticated("/profile.v1.ProfileService/SetInteractionLimit"),
        authenticated("/profile.v1.ProfileService/ClearInteractionLimit"),
        authenticated("/profile.v1.ProfileService/SetDiscoverySettings"),
        authenticated("/profile.v1.ProfileService/SetCommentFilters"),
        authenticated("/profile.v1.ProfileService/SetTabSettings"),
        authenticated("/profile.v1.ProfileService/SetFeedSettings"),
        // #668: the owner's account type and verification request (staff's
        // ListPending… / Decide… stay mesh-only).
        authenticated("/profile.v1.ProfileService/SetAccountType"),
        authenticated("/profile.v1.ProfileService/RequestVerification"),
        authenticated("/profile.v1.ProfileService/GetVerificationRequest"),
        authenticated("/profile.v1.ProfileService/DeleteProfile"),
        public_read("/profile.v1.ProfileService/GetProfileById"),
        public_read("/profile.v1.ProfileService/GetProfileByHandle"),
        // Sign-up's live handle check: guests (who are signing up) and members.
        public_read("/profile.v1.ProfileService/CheckHandleAvailability"),
        authenticated("/profile.v1.ProfileService/ListProfilesByAccount"),
    ];

    async fn build(infra: Arc<InfraRegistry>) -> anyhow::Result<Self> {
        let backends = Backends {
            scylla: ScyllaConfig::from_env(),
            redis:  RedisConfig::from_env(),
        };

        // Profile's cache TTLs are externalized: the `[cache]` section is required.
        let cache_registry = infra
            .cache()
            .context("profile requires a [cache] section in infrastructure.toml")?;

        // Outbound: profile lifecycle events → `profile.v1.events`.
        let producer = KafkaProducerBuilder::new(ProducerConfig::new(KafkaClientConfig::from_env()))
            .build()
            .context("build profile event producer")?;
        let publisher: Arc<dyn EventPublisher> = Arc::new(KafkaProfileEventPublisher::new(producer));

        let app = App::build(backends, cache_registry, publisher)
            .await
            .map_err(|e| anyhow::anyhow!("profile app build: {e}"))?;

        // Inbound integration: account lifecycle → profile masking/restoration.
        spawn_account_event_consumer(Arc::clone(&app.command_bus));
        // Inbound integration: author-tier signal → denormalized profile tier.
        spawn_author_tier_consumer(Arc::clone(&app.command_bus));

        Ok(Self { app })
    }

    fn health_probes(&self) -> Vec<Arc<dyn HealthProbe>> {
        vec![
            scylla_storage::health::probe(Arc::clone(&self.app.scylla)),
            redis_storage::health::probe((*self.app.redis).clone()),
        ]
    }

    fn register(self, routes: &mut RoutesBuilder) -> anyhow::Result<()> {
        // The buses are shared behind `Arc`, which implements the bus traits.
        let handler = ProfileServiceHandler::new(
            Arc::clone(&self.app.command_bus),
            Arc::clone(&self.app.query_bus),
        );

        let reflection = ReflectionBuilder::configure()
            .register_encoded_file_descriptor_set(FILE_DESCRIPTOR_SET)
            .build_v1()?;

        routes.add_service(reflection);
        routes.add_service(ProfileServiceServer::new(handler));
        Ok(())
    }
}

/// Spawns the supervised account-event consumer. It rebuilds its Kafka handles
/// and restarts after [`CONSUMER_RESPAWN_BACKOFF`] whenever the runner returns
/// (stream end or unrecoverable broker/DLQ error), per the runner's contract.
fn spawn_account_event_consumer(command_bus: Arc<InMemoryCommandBus>) {
    tokio::spawn(async move {
        loop {
            match build_account_event_consumer() {
                Ok((consumer, producer)) => {
                    run_account_event_consumer(consumer, Arc::clone(&command_bus), producer).await;
                    tracing::warn!("account event consumer exited; respawning after backoff");
                }
                Err(error) => {
                    tracing::error!(%error, "failed to build account event consumer; retrying");
                }
            }
            tokio::time::sleep(CONSUMER_RESPAWN_BACKOFF).await;
        }
    });
}

/// Builds the manual-commit consumer (subscribed to the account topic) and the
/// dead-letter producer the runner needs.
fn build_account_event_consumer(
) -> anyhow::Result<(KafkaConsumerHandle, KafkaProducerHandle)> {
    let kafka = KafkaClientConfig::from_env();
    let consumer =
        KafkaConsumerBuilder::new(ConsumerConfig::new(kafka.clone(), ACCOUNT_EVENTS_GROUP))
            .subscribe(ACCOUNT_EVENTS_TOPIC)
            .build()
            .context("build account event consumer")?;
    let producer = KafkaProducerBuilder::new(ProducerConfig::new(kafka))
        .build()
        .context("build account event dead-letter producer")?;
    Ok((consumer, producer))
}

/// Spawns the supervised author-tier consumer (social-graph → denormalized tier),
/// respawning after a backoff whenever the runner returns.
fn spawn_author_tier_consumer(command_bus: Arc<InMemoryCommandBus>) {
    tokio::spawn(async move {
        loop {
            match build_author_tier_consumer() {
                Ok((consumer, producer)) => {
                    run_author_tier_consumer(consumer, Arc::clone(&command_bus), producer).await;
                    tracing::warn!("author tier consumer exited; respawning after backoff");
                }
                Err(error) => {
                    tracing::error!(%error, "failed to build author tier consumer; retrying");
                }
            }
            tokio::time::sleep(CONSUMER_RESPAWN_BACKOFF).await;
        }
    });
}

/// Builds the manual-commit consumer (subscribed to the author-tier topic) and the
/// dead-letter producer the runner needs.
fn build_author_tier_consumer(
) -> anyhow::Result<(KafkaConsumerHandle, KafkaProducerHandle)> {
    let kafka = KafkaClientConfig::from_env();
    let consumer = KafkaConsumerBuilder::new(ConsumerConfig::new(kafka.clone(), AUTHOR_TIER_GROUP))
        .subscribe(AUTHOR_TIER_TOPIC)
        .build()
        .context("build author tier consumer")?;
    let producer = KafkaProducerBuilder::new(ProducerConfig::new(kafka))
        .build()
        .context("build author tier dead-letter producer")?;
    Ok((consumer, producer))
}
