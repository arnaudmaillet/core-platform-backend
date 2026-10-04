//! Adapts the comment composition root to the fleet [`service_runtime::Service`]
//! contract. Comment is ScyllaDB-only and publishes domain events through the
//! durable Kafka publisher.

use std::sync::Arc;

use async_trait::async_trait;
use cqrs::command::InMemoryCommandBus;
use cqrs::query::InMemoryQueryBus;
use scylla_storage::ScyllaConfig;
use service_runtime::{HealthProbe, InfraRegistry, Service};
use service_runtime::edge::{authenticated, public_read};
use service_runtime::EdgePolicy;
use tonic::service::RoutesBuilder;
use tonic_reflection::server::Builder as ReflectionBuilder;
use transport::kafka::config::{KafkaClientConfig, ProducerConfig};
use transport::kafka::producer::KafkaProducerBuilder;

use crate::app::{App, Backends};
use crate::application::port::ReadGate;
use crate::infrastructure::client::GrpcReadGate;
use crate::infrastructure::grpc::handler::comment_service_handler::{
    CommentServiceHandler, CommentServiceServer,
};
use crate::infrastructure::grpc::server::FILE_DESCRIPTOR_SET;
use crate::domain::comment_filter::TermList;
use crate::infrastructure::consumer::CommentFiltersWorker;
use crate::infrastructure::publisher::KafkaCommentEventPublisher;

type CommentServer =
    CommentServiceServer<CommentServiceHandler<Arc<InMemoryCommandBus>, Arc<InMemoryQueryBus>>>;

/// The comment service as hosted by [`service_runtime`].
pub struct CommentService {
    app: App,
}

#[async_trait]
impl Service for CommentService {
    const NAME: &'static str = "comment";
    const VERSION: &'static str = env!("CARGO_PKG_VERSION");
    const GRPC_SERVICE_NAME: &'static str = <CommentServer as tonic::server::NamedService>::NAME;

    /// The RPCs exposed on the client edge listener (`GRPC_EDGE_ADDR`); anything
    /// else on this service is mesh-only. See `transport::grpc::edge`.
    const EDGE_POLICY: EdgePolicy = &[
        authenticated("/comment.v1.CommentService/CreateComment"),
        authenticated("/comment.v1.CommentService/DeleteComment"),
        authenticated("/comment.v1.CommentService/ReviewHeldComment"),
        public_read("/comment.v1.CommentService/GetComment"),
        public_read("/comment.v1.CommentService/ListTopLevel"),
        public_read("/comment.v1.CommentService/ListReplies"),
    ];

    async fn build(_infra: Arc<InfraRegistry>) -> anyhow::Result<Self> {
        let backends = Backends {
            scylla: ScyllaConfig::from_env(),
        };

        let producer = KafkaProducerBuilder::new(ProducerConfig::new(KafkaClientConfig::from_env()))
            .build()?;
        let publisher = Arc::new(KafkaCommentEventPublisher::new(producer));

        let app = App::build(backends, publisher, read_gate_from_env()?, offensive_terms_from_env())
            .await
            .map_err(|e| anyhow::anyhow!("comment app build: {e}"))?;

        // Inbound integration: the post owners' hidden words / offensive
        // filter (profile.v1.events) → the filter the reads apply.
        tokio::spawn(
            CommentFiltersWorker::new(
                KafkaClientConfig::from_env(),
                Arc::clone(&app.filter_store),
                COMMENT_FILTERS_GROUP,
            )
            .run(),
        );

        Ok(Self { app })
    }

    fn health_probes(&self) -> Vec<Arc<dyn HealthProbe>> {
        vec![scylla_storage::health::probe(Arc::clone(&self.app.scylla))]
    }

    fn register(self, routes: &mut RoutesBuilder) -> anyhow::Result<()> {
        let handler = CommentServiceHandler::new(
            Arc::clone(&self.app.command_bus),
            Arc::clone(&self.app.query_bus),
        );
        let reflection = ReflectionBuilder::configure()
            .register_encoded_file_descriptor_set(FILE_DESCRIPTOR_SET)
            .build_v1()?;

        routes.add_service(reflection);
        routes.add_service(CommentServiceServer::new(handler));
        Ok(())
    }
}

/// Consumer group of the comment-filters projection.
const COMMENT_FILTERS_GROUP: &str = "comment-filters";

/// The offensive-term list (#660): one term per line (`#` comments) in the file
/// at `COMMENT_OFFENSIVE_TERMS_FILE`. Trust & safety owns its content. Without
/// it the offensive filter hides nothing (hidden words still apply).
pub(crate) fn offensive_terms_from_env() -> Arc<TermList> {
    let Ok(path) = std::env::var("COMMENT_OFFENSIVE_TERMS_FILE") else {
        tracing::warn!("COMMENT_OFFENSIVE_TERMS_FILE unset: the offensive-comment filter hides nothing");
        return Arc::new(TermList::default());
    };
    match std::fs::read_to_string(&path) {
        Ok(text) => {
            let terms = TermList::new(text.lines().map(str::to_owned));
            tracing::info!(path, terms = terms.len(), "offensive-comment terms loaded");
            Arc::new(terms)
        }
        Err(error) => {
            tracing::error!(path, %error, "cannot read the offensive-comment terms; the filter hides nothing");
            Arc::new(TermList::default())
        }
    }
}

/// The read gate: post GetPost + social-graph CheckAccess. Lazy connects (a
/// cold start needs neither up); request + connect deadlines are mandatory
/// (tonic has none). Non-mesh reads fail closed without them.
pub(crate) fn read_gate_from_env() -> anyhow::Result<Arc<dyn ReadGate>> {
    Ok(Arc::new(GrpcReadGate::new(
        lazy_channel("COMMENT_POST_GRPC_ENDPOINT", "http://localhost:50056")?,
        lazy_channel("COMMENT_SOCIAL_GRAPH_GRPC_ENDPOINT", "http://localhost:50053")?,
    )))
}

/// A lazily-connected channel to the endpoint in `endpoint_env` (or `default`),
/// with the request and connect deadlines from COMMENT_GATE_RPC_TIMEOUT_MS /
/// COMMENT_GATE_CONNECT_TIMEOUT_MS (1 s each by default).
fn lazy_channel(endpoint_env: &str, default: &str) -> anyhow::Result<tonic::transport::Channel> {
    let ms = |key: &str| {
        std::time::Duration::from_millis(
            std::env::var(key).ok().and_then(|v| v.parse().ok()).unwrap_or(1_000),
        )
    };
    let endpoint = std::env::var(endpoint_env).unwrap_or_else(|_| default.to_owned());
    Ok(tonic::transport::Channel::from_shared(endpoint)
        .map_err(|e| anyhow::anyhow!("invalid {endpoint_env}: {e}"))?
        .timeout(ms("COMMENT_GATE_RPC_TIMEOUT_MS"))
        .connect_timeout(ms("COMMENT_GATE_CONNECT_TIMEOUT_MS"))
        .connect_lazy())
}
