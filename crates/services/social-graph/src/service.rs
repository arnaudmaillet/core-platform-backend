//! Adapts the social-graph composition root to the fleet
//! [`service_runtime::Service`] contract so the shared runtime can host it.
//!
//! Domain wiring stays in [`crate::app`]; this module maps env → config, builds
//! the concrete Kafka event publisher, defers to [`App::build`], registers the
//! gRPC services, and exposes the backend health probes.

use std::sync::Arc;

use async_trait::async_trait;
use cqrs::command::InMemoryCommandBus;
use cqrs::query::InMemoryQueryBus;
use redis_storage::RedisConfig;
use scylla_storage::ScyllaConfig;
use service_runtime::{HealthProbe, InfraRegistry, Service};
use service_runtime::edge::{authenticated, public_read};
use service_runtime::EdgePolicy;
use tonic::service::RoutesBuilder;
use tonic_reflection::server::Builder as ReflectionBuilder;
use transport::kafka::config::{ConsumerConfig, KafkaClientConfig, ProducerConfig};
use transport::kafka::consumer::{KafkaConsumerBuilder, KafkaConsumerHandle};
use transport::kafka::producer::{KafkaProducerBuilder, KafkaProducerHandle};

use crate::domain::value_object::TierThresholds;

/// Default follower-count floors for tier classification (a product default; tune
/// via `SOCIAL_GRAPH_PREMIUM_FOLLOWER_THRESHOLD` / `..._VIP_FOLLOWER_THRESHOLD`).
const DEFAULT_PREMIUM_FOLLOWERS: i64 = 10_000;
const DEFAULT_VIP_FOLLOWERS: i64 = 1_000_000;

fn tier_thresholds_from_env() -> TierThresholds {
    TierThresholds::new(
        env_i64("SOCIAL_GRAPH_PREMIUM_FOLLOWER_THRESHOLD", DEFAULT_PREMIUM_FOLLOWERS),
        env_i64("SOCIAL_GRAPH_VIP_FOLLOWER_THRESHOLD", DEFAULT_VIP_FOLLOWERS),
    )
}

fn env_i64(key: &str, default: i64) -> i64 {
    std::env::var(key).ok().and_then(|v| v.parse().ok()).unwrap_or(default)
}

use crate::app::{App, Backends};
use crate::application::port::EventPublisher;
use crate::infrastructure::grpc::handler::social_graph_service_handler::SocialGraphServiceServer;
use crate::infrastructure::grpc::handler::SocialGraphServiceHandler;
use crate::infrastructure::grpc::server::FILE_DESCRIPTOR_SET;
use crate::infrastructure::consumer::run_profile_audience_consumer;
use crate::infrastructure::publisher::KafkaEventPublisher;

/// Profile's event stream; social-graph projects its audience facts.
const PROFILE_EVENTS_TOPIC: &str = "profile.v1.events";
/// Consumer group for the profile-audience projection.
const PROFILE_AUDIENCE_GROUP: &str = "social-graph-profile-audience";
/// Backoff before respawning the consumer after the runner returns.
const CONSUMER_RESPAWN_BACKOFF: std::time::Duration = std::time::Duration::from_secs(5);

type SocialGraphServer =
    SocialGraphServiceServer<SocialGraphServiceHandler<Arc<InMemoryCommandBus>, Arc<InMemoryQueryBus>>>;

/// The social-graph service as hosted by [`service_runtime`].
pub struct SocialGraphService {
    app: App,
}

#[async_trait]
impl Service for SocialGraphService {
    const NAME: &'static str = "social-graph";
    const VERSION: &'static str = env!("CARGO_PKG_VERSION");
    const GRPC_SERVICE_NAME: &'static str =
        <SocialGraphServer as tonic::server::NamedService>::NAME;

    /// The RPCs exposed on the client edge listener (`GRPC_EDGE_ADDR`); anything
    /// else on this service is mesh-only. See `transport::grpc::edge`.
    const EDGE_POLICY: EdgePolicy = &[
        authenticated("/social_graph.v1.SocialGraphService/Follow"),
        authenticated("/social_graph.v1.SocialGraphService/Unfollow"),
        authenticated("/social_graph.v1.SocialGraphService/Block"),
        authenticated("/social_graph.v1.SocialGraphService/Unblock"),
        authenticated("/social_graph.v1.SocialGraphService/GetRelationStatus"),
        public_read("/social_graph.v1.SocialGraphService/ListFollowers"),
        public_read("/social_graph.v1.SocialGraphService/ListFollowing"),
        authenticated("/social_graph.v1.SocialGraphService/ListBlocks"),
        // The owner's list tools (bound to profile_id): remove a follower, and
        // who may see the follower / following lists.
        authenticated("/social_graph.v1.SocialGraphService/RemoveFollower"),
        authenticated("/social_graph.v1.SocialGraphService/SetListPrivacy"),
        authenticated("/social_graph.v1.SocialGraphService/GetListPrivacy"),
        // Mutes: the muter's own (bound to actor_id / profile_id).
        authenticated("/social_graph.v1.SocialGraphService/Mute"),
        authenticated("/social_graph.v1.SocialGraphService/Unmute"),
        authenticated("/social_graph.v1.SocialGraphService/ListMutes"),
        // Follow requests to private profiles: the owner's inbox and answers
        // (bound to owner_id), the requester's cancel (bound to actor_id).
        authenticated("/social_graph.v1.SocialGraphService/ListFollowRequests"),
        authenticated("/social_graph.v1.SocialGraphService/ApproveFollowRequest"),
        authenticated("/social_graph.v1.SocialGraphService/DeclineFollowRequest"),
        authenticated("/social_graph.v1.SocialGraphService/CancelFollowRequest"),
    ];

    async fn build(_infra: Arc<InfraRegistry>) -> anyhow::Result<Self> {
        let backends = Backends {
            scylla: ScyllaConfig::from_env(),
            redis:  RedisConfig::from_env(),
        };

        // Social-graph always publishes downstream events; build the durable
        // Kafka publisher from env.
        let producer = KafkaProducerBuilder::new(ProducerConfig::new(KafkaClientConfig::from_env()))
            .build()?;
        let publisher: Arc<dyn EventPublisher> = Arc::new(KafkaEventPublisher::new(producer));

        let app = App::build(backends, publisher, tier_thresholds_from_env())
            .await
            .map_err(|e| anyhow::anyhow!("social-graph app build: {e}"))?;

        // Inbound integration: profile visibility / hidden facts → the audience
        // projection the access check reads.
        spawn_profile_audience_consumer(Arc::clone(&app.command_bus));

        Ok(Self { app })
    }

    fn health_probes(&self) -> Vec<Arc<dyn HealthProbe>> {
        vec![
            scylla_storage::health::probe(Arc::clone(&self.app.scylla)),
            redis_storage::health::probe((*self.app.redis).clone()),
        ]
    }

    fn register(self, routes: &mut RoutesBuilder) -> anyhow::Result<()> {
        let handler = SocialGraphServiceHandler::new(
            Arc::clone(&self.app.command_bus),
            Arc::clone(&self.app.query_bus),
        );
        let reflection = ReflectionBuilder::configure()
            .register_encoded_file_descriptor_set(FILE_DESCRIPTOR_SET)
            .build_v1()?;

        routes.add_service(reflection);
        routes.add_service(SocialGraphServiceServer::new(handler));
        Ok(())
    }
}

/// Spawns the supervised profile-audience consumer, respawning after a backoff
/// whenever the runner returns.
fn spawn_profile_audience_consumer(command_bus: Arc<InMemoryCommandBus>) {
    tokio::spawn(async move {
        loop {
            match build_profile_audience_consumer() {
                Ok((consumer, producer)) => {
                    run_profile_audience_consumer(consumer, Arc::clone(&command_bus), producer).await;
                    tracing::warn!("profile-audience consumer exited; respawning after backoff");
                }
                Err(error) => {
                    tracing::error!(%error, "failed to build profile-audience consumer; retrying");
                }
            }
            tokio::time::sleep(CONSUMER_RESPAWN_BACKOFF).await;
        }
    });
}

/// Builds the manual-commit consumer (subscribed to `profile.v1.events`) and the
/// dead-letter producer the runner needs.
fn build_profile_audience_consumer() -> anyhow::Result<(KafkaConsumerHandle, KafkaProducerHandle)> {
    let kafka = KafkaClientConfig::from_env();
    let consumer = KafkaConsumerBuilder::new(ConsumerConfig::new(kafka.clone(), PROFILE_AUDIENCE_GROUP))
        .subscribe(PROFILE_EVENTS_TOPIC)
        .build()
        .map_err(|e| anyhow::anyhow!("build profile-audience consumer: {e}"))?;
    let producer = KafkaProducerBuilder::new(ProducerConfig::new(kafka))
        .build()
        .map_err(|e| anyhow::anyhow!("build profile-audience dead-letter producer: {e}"))?;
    Ok((consumer, producer))
}
