//! The media service's composition root.
//!
//! [`App::compose`] is *pure* wiring: the ports in, the assembled gRPC handler out —
//! no I/O, so the unit graph and the binary build the exact same handler.
//! [`App::build`] is the I/O variant that constructs the concrete adapters (S3 /
//! Postgres / Redis / CDN / moderation gRPC / Kafka) from config + backend
//! connections, then defers to `compose`. It retains the process + moderation
//! handlers so [`crate::service`] can self-spawn the consumers, and the storage
//! clients so the runtime builds liveness probes.

use std::sync::Arc;

use postgres_storage::{PgPoolBuilder, PostgresConfig, TransactionManager};
use redis_storage::{RedisClient, RedisClientBuilder, RedisConfig};
use sqlx::PgPool;
use tonic::transport::Channel;
use transport::kafka::config::client::KafkaClientConfig;
use transport::kafka::config::producer::ProducerConfig;
use transport::kafka::producer::KafkaProducerBuilder;

use crate::application::command::{
    ApplyModerationHandler, CommitUploadHandler, DeleteAssetHandler, DocumentRetention, IssueUploadTicketHandler,
    OwnerErasure,
    ProcessAssetHandler, TranscodeAssetHandler,
};
use crate::application::port::{
    AccessLog, AssetRepository, CdnGateway, DeliveryCache, EventPublisher, ImageProcessor, MalwareScanner,
    MediaProbe, ModerationScreen, ObjectStore, VideoTranscoder,
};
use crate::application::query::{
    GetAssetHandler, ListAssetsByOwnerHandler, PrivateDocumentUrlHandler, ResolveDeliveryHandler,
};
use crate::application::MediaPolicy;
use crate::config::MediaConfig;
use crate::infrastructure::cache::RedisDeliveryCache;
use crate::infrastructure::cdn::{CloudFrontCdnGateway, CloudFrontInvalidator, CloudFrontInvalidatorConfig};
use crate::infrastructure::event::{KafkaAccessLog, KafkaEventPublisher, LogAccessLog, LogEventPublisher};
use crate::infrastructure::grpc::MediaServiceHandler;
use crate::infrastructure::persistence::PgAssetRepository;
use crate::infrastructure::probe::{DispatchingMediaProbe, DocumentMediaProbe, ImageMediaProbe, VideoMediaProbe};
use crate::infrastructure::processor::{FfmpegVideoTranscoder, ImageRenditionProcessor};
use crate::infrastructure::scanner::LogMalwareScanner;
use crate::infrastructure::screen::GrpcModerationScreen;
use crate::infrastructure::store::{S3Client, S3ObjectStore};

/// The nine ports the application layer depends on, plus the policy.
pub struct AppDeps {
    pub assets: Arc<dyn AssetRepository>,
    pub cache: Arc<dyn DeliveryCache>,
    pub store: Arc<dyn ObjectStore>,
    pub cdn: Arc<dyn CdnGateway>,
    pub probe: Arc<dyn MediaProbe>,
    pub processor: Arc<dyn ImageProcessor>,
    pub transcoder: Arc<dyn VideoTranscoder>,
    pub scanner: Arc<dyn MalwareScanner>,
    pub screen: Arc<dyn ModerationScreen>,
    pub publisher: Arc<dyn EventPublisher>,
    /// Staff views of private documents, on the audit plane (#837).
    pub access_log: Arc<dyn AccessLog>,
    pub policy: MediaPolicy,
}

/// Backend connection configs. `kafka` is optional: absent ⇒ the log publisher.
pub struct Backends {
    pub postgres: PostgresConfig,
    pub redis: RedisConfig,
    pub kafka: Option<KafkaClientConfig>,
}

/// A fully-wired media service. Retains the storage clients (for liveness probes)
/// and the process + moderation handlers (for the self-spawned consumers).
pub struct App {
    pub handler: MediaServiceHandler,
    pub process: Arc<ProcessAssetHandler>,
    pub transcode: Arc<TranscodeAssetHandler>,
    pub apply_moderation: Arc<ApplyModerationHandler>,
    /// Erases a deleted account's media (#777), fed by the account consumer.
    pub erasure: Arc<OwnerErasure>,
    /// Private-document retention (#777): the decision consumer + the sweeper.
    pub retention: Arc<DocumentRetention>,
    pub document_sweep_interval: std::time::Duration,
    pub pool: PgPool,
    pub redis: RedisClient,
    pub store: Arc<S3Client>,
}

impl App {
    /// Pure composition: assemble the gRPC handler from the ports + the shared
    /// process handler. No I/O — drives the unit graph.
    pub fn compose(deps: AppDeps, process: Arc<ProcessAssetHandler>) -> MediaServiceHandler {
        let issue = Arc::new(IssueUploadTicketHandler::new(
            Arc::clone(&deps.assets),
            Arc::clone(&deps.store),
            deps.policy.clone(),
        ));
        let commit = Arc::new(CommitUploadHandler::new(
            Arc::clone(&deps.assets),
            Arc::clone(&deps.store),
            Arc::clone(&deps.probe),
            Arc::clone(&deps.publisher),
        ));
        let delete = Arc::new(DeleteAssetHandler::new(
            Arc::clone(&deps.assets),
            Arc::clone(&deps.store),
            Arc::clone(&deps.cdn),
            Arc::clone(&deps.cache),
            Arc::clone(&deps.publisher),
        ));
        let get = Arc::new(GetAssetHandler::new(Arc::clone(&deps.assets)));
        let resolve = Arc::new(ResolveDeliveryHandler::new(
            Arc::clone(&deps.assets),
            Arc::clone(&deps.cache),
            Arc::clone(&deps.cdn),
        ));
        let by_owner = Arc::new(ListAssetsByOwnerHandler::new(Arc::clone(&deps.assets), Arc::clone(&deps.cdn)));
        let private_documents = Arc::new(PrivateDocumentUrlHandler::new(
            Arc::clone(&deps.assets),
            Arc::clone(&deps.cdn),
            Arc::clone(&deps.access_log),
            deps.policy.signed_url_ttl,
        ));
        MediaServiceHandler::new(issue, commit, delete, process, get, resolve)
            .with_list_by_owner(by_owner)
            .with_private_documents(private_documents)
    }

    /// Builds the process + moderation handlers shared between the gRPC handler and
    /// the consumers (a single instance of each, over the same ports).
    #[allow(clippy::type_complexity)]
    fn build_workers(
        deps: &AppDeps,
    ) -> (Arc<ProcessAssetHandler>, Arc<TranscodeAssetHandler>, Arc<ApplyModerationHandler>) {
        let process = Arc::new(ProcessAssetHandler::new(
            Arc::clone(&deps.assets),
            Arc::clone(&deps.scanner),
            Arc::clone(&deps.screen),
            Arc::clone(&deps.processor),
            Arc::clone(&deps.store),
            Arc::clone(&deps.cache),
            Arc::clone(&deps.publisher),
            deps.policy.clone(),
        ));
        let transcode = Arc::new(TranscodeAssetHandler::new(
            Arc::clone(&deps.assets),
            Arc::clone(&deps.scanner),
            Arc::clone(&deps.screen),
            Arc::clone(&deps.transcoder),
            Arc::clone(&deps.cache),
            Arc::clone(&deps.publisher),
            deps.policy.clone(),
        ));
        let apply_moderation = Arc::new(ApplyModerationHandler::new(
            Arc::clone(&deps.assets),
            Arc::clone(&deps.store),
            Arc::clone(&deps.cdn),
            Arc::clone(&deps.cache),
            Arc::clone(&deps.publisher),
        ));
        (process, transcode, apply_moderation)
    }

    /// Builds the concrete adapter graph from config + backend connections.
    pub async fn build(
        config: MediaConfig,
        backends: Backends,
    ) -> Result<App, Box<dyn std::error::Error>> {
        let (document_retention, document_sweep_interval) = (config.document_retention, config.document_sweep_interval);
        let pool = PgPoolBuilder::build(backends.postgres).await?;
        let tx = TransactionManager::new(pool.clone());
        let redis = RedisClientBuilder::new(backends.redis).build().await?;

        // Shared S3 client: presigns URLs + does the worker's server-side byte I/O.
        // Its HTTP client carries the object-store hard timeout.
        let cdn_keys = (config.s3.access_key.clone(), config.s3.secret_key.clone());
        let store = Arc::new(S3Client::new(config.s3)?);

        let (publisher, access_log): (Arc<dyn EventPublisher>, Arc<dyn AccessLog>) = match backends.kafka {
            Some(cfg) => {
                let producer = KafkaProducerBuilder::new(ProducerConfig::new(cfg)).build()?;
                (Arc::new(KafkaEventPublisher::new(producer.clone())), Arc::new(KafkaAccessLog::new(producer)))
            }
            None => (Arc::new(LogEventPublisher), Arc::new(LogAccessLog)),
        };

        // Lazy connect: dials `moderation` on first use, so a cold start does not
        // require the gate to be up at boot.
        let screen_channel = Channel::from_shared(config.screen_endpoint)?.connect_lazy();

        let mut cdn = CloudFrontCdnGateway::new(config.cdn_base_url, Arc::clone(&store), config.policy.signed_url_ttl);
        match config.cloudfront_distribution_id {
            Some(distribution_id) => {
                cdn = cdn.with_invalidator(CloudFrontInvalidator::new(
                    reqwest::Client::builder().timeout(std::time::Duration::from_secs(10)).build()?,
                    CloudFrontInvalidatorConfig {
                        distribution_id,
                        access_key_id: cdn_keys.0,
                        secret_access_key: cdn_keys.1,
                        endpoint: "https://cloudfront.amazonaws.com".into(),
                    },
                ));
            }
            None => tracing::warn!("MEDIA_CLOUDFRONT_DISTRIBUTION_ID not set: takedowns are not purged from a CDN"),
        }

        let deps = AppDeps {
            assets: Arc::new(PgAssetRepository::new(tx)),
            cache: Arc::new(RedisDeliveryCache::new(redis.clone())),
            store: Arc::new(S3ObjectStore::new(Arc::clone(&store))),
            cdn: Arc::new(cdn),
            probe: Arc::new(DispatchingMediaProbe::new(
                Arc::new(ImageMediaProbe::new(Arc::clone(&store))),
                Arc::new(VideoMediaProbe::new(Arc::clone(&store))),
            )
            .with_documents(Arc::new(DocumentMediaProbe::new(Arc::clone(&store))))),
            processor: Arc::new(ImageRenditionProcessor::new(Arc::clone(&store))),
            transcoder: Arc::new(FfmpegVideoTranscoder::new(Arc::clone(&store))),
            scanner: Arc::new(LogMalwareScanner),
            screen: Arc::new(GrpcModerationScreen::new(screen_channel)),
            publisher,
            access_log,
            policy: config.policy,
        };

        let (process, transcode, apply_moderation) = App::build_workers(&deps);
        let delete = Arc::new(DeleteAssetHandler::new(
            Arc::clone(&deps.assets),
            Arc::clone(&deps.store),
            Arc::clone(&deps.cdn),
            Arc::clone(&deps.cache),
            Arc::clone(&deps.publisher),
        ));
        let erasure = Arc::new(OwnerErasure::new(Arc::clone(&deps.assets), Arc::clone(&delete)));
        let retention = Arc::new(DocumentRetention::new(Arc::clone(&deps.assets), delete, document_retention));
        let handler = App::compose(deps, Arc::clone(&process));
        Ok(App {
            handler,
            process,
            transcode,
            apply_moderation,
            erasure,
            retention,
            document_sweep_interval,
            pool,
            redis,
            store,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::application::fakes::Fixture;
    use crate::infrastructure::grpc::proto;
    use tonic::{Code, Request};

    /// Composes the gRPC handler over the in-memory fakes — the exact graph
    /// `App::build` produces, minus the real backends.
    fn handler_from_fakes(fx: &Fixture) -> MediaServiceHandler {
        let deps = AppDeps {
            assets: fx.assets.clone(),
            cache: fx.cache.clone(),
            store: fx.store.clone(),
            cdn: fx.cdn.clone(),
            probe: fx.probe.clone(),
            transcoder: fx.transcoder.clone(),
            processor: fx.processor.clone(),
            scanner: fx.scanner.clone(),
            screen: fx.screen.clone(),
            publisher: fx.publisher.clone(),
            access_log: fx.access_log.clone(),
            policy: fx.policy.clone(),
        };
        let (process, _transcode, _apply) = App::build_workers(&deps);
        App::compose(deps, process)
    }

    #[tokio::test]
    async fn issue_ticket_rpc_returns_a_presigned_upload() {
        let fx = Fixture::new();
        let handler = handler_from_fakes(&fx);
        let request = Request::new(proto::IssueUploadTicketRequest {
            owner_id: uuid::Uuid::from_u128(7).to_string(),
            kind: proto::MediaKind::PostImage as i32,
            declared_mime_type: "image/jpeg".into(),
            declared_size_bytes: 2_000_000,
            content_sha256: String::new(),
            idempotency_key: String::new(),
        });
        let resp = handler.issue_upload_ticket(request).await.unwrap().into_inner();
        assert!(!resp.deduplicated);
        let ticket = resp.ticket.expect("a presigned ticket");
        assert_eq!(ticket.method, "PUT");
        assert!(!resp.asset_id.is_empty());
    }

    #[tokio::test]
    async fn issue_ticket_rpc_rejects_oversize() {
        let fx = Fixture::new();
        let handler = handler_from_fakes(&fx);
        let request = Request::new(proto::IssueUploadTicketRequest {
            owner_id: uuid::Uuid::from_u128(7).to_string(),
            kind: proto::MediaKind::Avatar as i32,
            declared_mime_type: "image/png".into(),
            declared_size_bytes: 999_999_999,
            content_sha256: String::new(),
            idempotency_key: String::new(),
        });
        let status = handler.issue_upload_ticket(request).await.unwrap_err();
        assert_eq!(status.code(), Code::InvalidArgument);
    }

    #[tokio::test]
    async fn resolve_delivery_rpc_returns_public_urls_for_a_ready_asset() {
        let fx = Fixture::new();
        let (asset_id, _owner) = fx.ready_asset(crate::domain::value_object::MediaKind::PostImage).await;
        let handler = handler_from_fakes(&fx);
        let request = Request::new(proto::ResolveDeliveryRequest {
            asset_id: asset_id.as_str(),
            preferred: 0,
            visibility: 0,
        });
        let resp = handler.resolve_delivery(request).await.unwrap().into_inner();
        let media = resp.media.expect("delivered media");
        assert!(!media.degraded);
        assert!(!media.renditions.is_empty());
        assert_eq!(media.state, proto::AssetState::MediaAssetStateReady as i32);
    }

    #[tokio::test]
    async fn get_unknown_asset_is_not_found() {
        let fx = Fixture::new();
        let handler = handler_from_fakes(&fx);
        let request = Request::new(proto::GetAssetRequest {
            asset_id: uuid::Uuid::now_v7().to_string(),
        });
        let status = handler.get_asset(request).await.unwrap_err();
        assert_eq!(status.code(), Code::NotFound);
    }

    /// Bearer tokens as staff tooling sends them on the mesh (#837).
    struct Tokens;

    impl transport::grpc::edge::StaffVerifier for Tokens {
        fn verify<'a>(&'a self, token: &'a str) -> transport::grpc::edge::StaffVerification<'a> {
            let permissions: Option<&[&str]> = match token {
                "reviewer" => Some(&[transport::grpc::edge::VERIFICATION_REVIEW]),
                "member" => Some(&[]),
                _ => None,
            };
            let principal = permissions.map(|permissions| {
                let raw: auth_context::OidcClaims =
                    serde_json::from_value(serde_json::json!({ "sub": "staff-1", "exp": 4_102_444_800_i64 })).unwrap();
                auth_context::CurrentPrincipal {
                    user_id: auth_context::PrincipalId::new("staff-1"),
                    tenant_id: None,
                    permissions: permissions.iter().map(|p| auth_context::Permission::new(*p)).collect(),
                    raw_claims: raw,
                }
            });
            Box::pin(async move { principal })
        }
    }

    fn with_token<T>(message: T, token: Option<&str>) -> Request<T> {
        let mut request = Request::new(message);
        if let Some(token) = token {
            request.metadata_mut().insert("authorization", format!("Bearer {token}").parse().unwrap());
        }
        request
    }

    /// #837: a private document's link is a mesh call carrying a staff token
    /// that holds `verification:review`; no token, a member's token or a
    /// forged one is refused, and the link handed out is on the audit plane.
    #[tokio::test]
    async fn a_private_document_link_is_for_reviewers_only_and_recorded() {
        let fx = Fixture::new();
        let handler = handler_from_fakes(&fx)
            .with_staff_gate(transport::grpc::edge::StaffGate::new(Arc::new(Tokens)));
        let (doc, _) = fx.ready_asset(crate::domain::value_object::MediaKind::PrivateDocument).await;
        let ask = || proto::GetPrivateDocumentUrlRequest { asset_id: doc.as_str() };

        for (token, code) in
            [(None, Code::Unauthenticated), (Some("forged"), Code::Unauthenticated), (Some("member"), Code::PermissionDenied)]
        {
            let denied = handler.get_private_document_url(with_token(ask(), token)).await.unwrap_err();
            assert_eq!(denied.code(), code, "{token:?}");
        }
        assert!(fx.access_log.views().is_empty(), "nothing handed out, nothing recorded");

        let link = handler.get_private_document_url(with_token(ask(), Some("reviewer"))).await.unwrap().into_inner();
        assert!(!link.url.is_empty());
        let views = fx.access_log.views();
        assert_eq!((views.len(), views[0].viewer.as_str(), views[0].asset_id), (1, "staff-1", doc));

        // The default gate (no verifier configured) denies everyone.
        let closed = handler_from_fakes(&fx).get_private_document_url(with_token(ask(), Some("reviewer"))).await;
        assert_eq!(closed.unwrap_err().code(), Code::Unauthenticated);
    }
}
