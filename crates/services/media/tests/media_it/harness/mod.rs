//! Integration harness: boots ephemeral MinIO + Postgres + Redis, creates the
//! bucket + applies the `.sql` migration, and wires the real media handlers against
//! them. The moderation Screen + malware scanner are stubbed at their ports.
#![allow(dead_code)]

use std::io::Cursor;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use async_trait::async_trait;
use chrono::Utc;
use cqrs::{Envelope, QueryHandler};
use image::{DynamicImage, ImageFormat, Rgb, RgbImage};
use sha2::{Digest, Sha256};
use uuid::Uuid;

use media::application::command::{
    ApplyModerationCommand, ApplyModerationHandler, CommitUploadCommand, CommitUploadHandler,
    DeleteAssetCommand, DeleteAssetHandler, DeleteOutcome, IssueUploadTicketCommand,
    IssueUploadTicketHandler, IssueUploadTicketOutcome, ModerationAction, ProcessAssetCommand,
    ProcessAssetHandler, ProcessOutcome,
};
use media::application::port::{
    EventPublisher, MalwareScanner, ModerationScreen, ScanVerdict, ScreenDecision,
};
use media::application::query::{
    DeliveredMediaView, GetAssetHandler, GetAssetQuery, ResolveDeliveryHandler, ResolveDeliveryQuery,
};
use media::application::MediaPolicy;
use media::domain::aggregate::Asset;
use media::domain::value_object::{
    AssetId, ContentHash, DeliveryVisibility, MediaKind, MimeType, OwnerId, StorageKey,
};
use media::error::MediaError;
use media::infrastructure::cache::RedisDeliveryCache;
use media::infrastructure::cdn::CloudFrontCdnGateway;
use media::infrastructure::event::LogEventPublisher;
use media::infrastructure::persistence::PgAssetRepository;
use media::infrastructure::probe::ImageMediaProbe;
use media::infrastructure::processor::ImageRenditionProcessor;
use media::infrastructure::store::{S3Client, S3Config, S3ObjectStore};

use postgres_storage::config::StatementLogLevel;
use postgres_storage::{PgPoolBuilder, PostgresConfig, TransactionManager};
use redis_storage::{RedisClientBuilder, RedisConfig};

const MIGRATIONS_DIR: &str = concat!(env!("CARGO_MANIFEST_DIR"), "/migrations");

// ── Stub ports (no live moderation / scanner) ────────────────────────────────

/// A screen whose verdict the scenario controls.
pub struct ConfigurableScreen {
    decision: Mutex<ScreenDecision>,
}

impl ConfigurableScreen {
    fn new() -> Self {
        Self { decision: Mutex::new(ScreenDecision::allow()) }
    }

    pub fn set_block(&self, csam: bool) {
        *self.decision.lock().unwrap() =
            ScreenDecision { blocked: true, csam, reference: Some("ncmec:test".into()) };
    }
}

#[async_trait]
impl ModerationScreen for ConfigurableScreen {
    async fn screen(
        &self,
        _asset_id: &AssetId,
        _owner_id: &OwnerId,
        _content_hash: &ContentHash,
        _kind: MediaKind,
    ) -> Result<ScreenDecision, MediaError> {
        Ok(self.decision.lock().unwrap().clone())
    }
}

struct PassScanner;

#[async_trait]
impl MalwareScanner for PassScanner {
    async fn scan(&self, _key: &StorageKey) -> Result<ScanVerdict, MediaError> {
        Ok(ScanVerdict::Clean)
    }
}

// ── Harness ──────────────────────────────────────────────────────────────────

pub struct Harness {
    issue: IssueUploadTicketHandler,
    issue_dedup: IssueUploadTicketHandler,
    commit: CommitUploadHandler,
    process: ProcessAssetHandler,
    delete: DeleteAssetHandler,
    moderation: ApplyModerationHandler,
    get: GetAssetHandler,
    resolve: ResolveDeliveryHandler,
    by_owner: media::application::query::ListAssetsByOwnerHandler,
    pub store: Arc<S3Client>,
    pub screen: Arc<ConfigurableScreen>,
    /// Private documents (#777): their retention and the staff link.
    pub retention: media::application::command::DocumentRetention,
    pub staff: media::application::query::PrivateDocumentUrlHandler,
    http: reqwest::Client,
    owner: OwnerId,
}

impl Harness {
    pub async fn start() -> Self {
        let pg_url = test_support::containers::postgres_ready(MIGRATIONS_DIR).await;
        let redis_endpoint = test_support::containers::redis_endpoint().await;
        let minio_endpoint = test_support::containers::minio_ready().await;

        let pg_config = PostgresConfig {
            database_url: pg_url,
            max_connections: 8,
            min_connections: 1,
            acquire_timeout: Duration::from_secs(5),
            idle_timeout: None,
            max_lifetime: None,
            statement_log_level: StatementLogLevel::Debug,
            slow_statement_threshold: Duration::from_millis(500),
        };
        let pool = PgPoolBuilder::build(pg_config).await.expect("it: postgres pool");
        let tx = TransactionManager::new(pool.clone());

        let redis = RedisClientBuilder::new(RedisConfig {
            hosts: vec![redis_endpoint],
            ..RedisConfig::default()
        })
        .build()
        .await
        .expect("it: redis client");

        let store = Arc::new(
            S3Client::new(S3Config {
                public_endpoint: minio_endpoint.clone(),
                endpoint: minio_endpoint,
                region: "us-east-1".into(),
                bucket: "media".into(),
                access_key: "minioadmin".into(),
                secret_key: "minioadmin".into(),
                presign_ttl: Duration::from_secs(900),
                request_timeout: Duration::from_secs(10),
            })
            .expect("it: s3 client"),
        );
        store.ensure_bucket().await.expect("it: create bucket");

        // Private documents (#777) on: the MinIO bucket here has no CDN.
        let policy = MediaPolicy { private_documents_enabled: true, ..MediaPolicy::standard() };
        let mut dedup_policy = MediaPolicy::standard();
        dedup_policy.dedup_enabled = true;

        let assets: Arc<PgAssetRepository> = Arc::new(PgAssetRepository::new(tx));
        let cache = Arc::new(RedisDeliveryCache::new(redis));
        let object_store = Arc::new(S3ObjectStore::new(Arc::clone(&store)));
        let cdn = Arc::new(CloudFrontCdnGateway::new(
            "https://cdn.test".into(),
            Arc::clone(&store),
            policy.signed_url_ttl,
        ));
        // Images and, for private documents (#777), PDF / HEIC by magic bytes.
        let image_probe: Arc<dyn media::application::port::MediaProbe> = Arc::new(ImageMediaProbe::new(Arc::clone(&store)));
        let probe = Arc::new(
            media::infrastructure::probe::DispatchingMediaProbe::new(Arc::clone(&image_probe), image_probe)
                .with_documents(Arc::new(media::infrastructure::probe::DocumentMediaProbe::new(Arc::clone(&store)))),
        );
        let processor = Arc::new(ImageRenditionProcessor::new(Arc::clone(&store)));
        let scanner = Arc::new(PassScanner);
        let screen = Arc::new(ConfigurableScreen::new());
        let publisher: Arc<dyn EventPublisher> = Arc::new(LogEventPublisher);

        let issue = IssueUploadTicketHandler::new(
            assets.clone(),
            object_store.clone(),
            policy.clone(),
        );
        let issue_dedup =
            IssueUploadTicketHandler::new(assets.clone(), object_store.clone(), dedup_policy);
        let commit = CommitUploadHandler::new(
            assets.clone(),
            object_store.clone(),
            probe.clone(),
            publisher.clone(),
        );
        let process = ProcessAssetHandler::new(
            assets.clone(),
            scanner,
            Arc::clone(&screen) as Arc<dyn ModerationScreen>,
            processor,
            object_store.clone(),
            cache.clone(),
            publisher.clone(),
            policy,
        );
        let delete = DeleteAssetHandler::new(
            assets.clone(),
            object_store.clone(),
            cdn.clone(),
            cache.clone(),
            publisher.clone(),
        );
        let retention = media::application::command::DocumentRetention::new(
            assets.clone(),
            Arc::new(DeleteAssetHandler::new(
                assets.clone(),
                object_store.clone(),
                cdn.clone(),
                cache.clone(),
                publisher.clone(),
            )),
            chrono::Duration::days(30),
        );
        let staff = media::application::query::PrivateDocumentUrlHandler::new(
            assets.clone(),
            cdn.clone(),
            Arc::new(media::infrastructure::event::LogAccessLog),
            chrono::Duration::minutes(5),
        );
        let moderation =
            ApplyModerationHandler::new(assets.clone(), object_store, cdn.clone(), cache.clone(), publisher);
        let get = GetAssetHandler::new(assets.clone());
        let by_owner = media::application::query::ListAssetsByOwnerHandler::new(assets.clone(), cdn.clone());
        let resolve = ResolveDeliveryHandler::new(assets, cache, cdn);

        Self {
            retention,
            staff,
            by_owner,
            issue,
            issue_dedup,
            commit,
            process,
            delete,
            moderation,
            get,
            resolve,
            store,
            screen,
            http: reqwest::Client::new(),
            owner: OwnerId::from_uuid(Uuid::from_u128(7)),
        }
    }

    pub fn owner(&self) -> OwnerId {
        self.owner
    }

    /// A small valid JPEG of the given dimensions (a soft gradient).
    pub fn sample_jpeg(width: u32, height: u32) -> Vec<u8> {
        let img = RgbImage::from_fn(width, height, |x, y| {
            Rgb([(x % 256) as u8, (y % 256) as u8, 128])
        });
        let mut buf = Vec::new();
        DynamicImage::ImageRgb8(img)
            .write_to(&mut Cursor::new(&mut buf), ImageFormat::Jpeg)
            .expect("encode sample jpeg");
        buf
    }

    pub fn sha256_hex(bytes: &[u8]) -> String {
        let mut h = Sha256::new();
        h.update(bytes);
        h.finalize().iter().map(|b| format!("{b:02x}")).collect()
    }

    // ── Flow helpers ─────────────────────────────────────────────────────────

    pub async fn issue_ticket(
        &self,
        size: u64,
        sha: Option<String>,
        dedup: bool,
    ) -> Result<IssueUploadTicketOutcome, MediaError> {
        let cmd = IssueUploadTicketCommand {
            owner_id: self.owner,
            kind: MediaKind::PostImage,
            declared_mime: MimeType::new("image/jpeg").unwrap(),
            declared_size: size,
            content_sha256: sha,
            idempotency_key: None,
        };
        let handler = if dedup { &self.issue_dedup } else { &self.issue };
        handler.handle(Envelope::new(Uuid::now_v7(), cmd), Utc::now()).await
    }

    /// Uploads bytes directly to the pre-signed URL (the client's role).
    pub async fn put_to_url(&self, url: &str, bytes: Vec<u8>) -> bool {
        self.http
            .put(url)
            .header(reqwest::header::CONTENT_TYPE, "image/jpeg")
            .body(bytes)
            .send()
            .await
            .map(|r| r.status().is_success())
            .unwrap_or(false)
    }

    pub async fn commit(&self, asset_id: AssetId) -> Result<Asset, MediaError> {
        let cmd = CommitUploadCommand { asset_id, etag: None, content_sha256: None };
        self.commit.handle(Envelope::new(Uuid::now_v7(), cmd), Utc::now()).await
    }

    pub async fn process(&self, asset_id: AssetId) -> Result<ProcessOutcome, MediaError> {
        self.process
            .handle(Envelope::new(Uuid::now_v7(), ProcessAssetCommand { asset_id }), Utc::now())
            .await
    }

    /// The harness owner's assets, by id, with downloads valid `ttl` (#653).
    pub async fn list_by_owner(
        &self,
        limit: i64,
        after: Option<AssetId>,
        ttl: chrono::Duration,
    ) -> Result<Vec<media::application::query::OwnedAsset>, MediaError> {
        self.by_owner
            .handle_at(
                media::application::query::ListAssetsByOwnerQuery { owner_id: self.owner, limit, after, url_ttl: ttl },
                chrono::Utc::now(),
            )
            .await
    }

    pub async fn get(&self, asset_id: AssetId) -> Result<Asset, MediaError> {
        self.get.handle(Envelope::new(Uuid::now_v7(), GetAssetQuery { asset_id, caller: None })).await
    }

    pub async fn resolve(
        &self,
        asset_id: AssetId,
        visibility: Option<DeliveryVisibility>,
    ) -> DeliveredMediaView {
        self.resolve
            .handle_at(
                Envelope::new(
                    Uuid::now_v7(),
                    ResolveDeliveryQuery { asset_id, preferred: None, visibility },
                ),
                Utc::now(),
            )
            .await
            .expect("resolve")
    }

    pub async fn delete(&self, asset_id: AssetId) -> Result<DeleteOutcome, MediaError> {
        let cmd = DeleteAssetCommand { asset_id, owner_id: self.owner };
        self.delete.handle(Envelope::new(Uuid::now_v7(), cmd), Utc::now()).await
    }

    pub async fn apply_moderation(
        &self,
        asset_id: AssetId,
        action: ModerationAction,
    ) {
        self.moderation
            .handle(
                Envelope::new(Uuid::now_v7(), ApplyModerationCommand { asset_id, action, enforcement_id: None }),
                Utc::now(),
            )
            .await
            .expect("apply moderation");
    }

    /// Uploads `bytes` as a private document and commits it (the probe's verdict).
    pub async fn upload_document_raw(&self, bytes: Vec<u8>, mime: &str) -> Result<Asset, MediaError> {
        let cmd = IssueUploadTicketCommand {
            owner_id: self.owner,
            kind: MediaKind::PrivateDocument,
            declared_mime: MimeType::new(mime).unwrap(),
            declared_size: bytes.len() as u64,
            content_sha256: None,
            idempotency_key: None,
        };
        let out = self.issue.handle(Envelope::new(Uuid::now_v7(), cmd), Utc::now()).await.expect("ticket");
        let url = out.upload.expect("a fresh upload").presigned.url;
        let put = self.http.put(&url).header(reqwest::header::CONTENT_TYPE, mime).body(bytes).send().await.unwrap();
        assert!(put.status().is_success(), "direct upload to MinIO failed: {}", put.status());
        self.commit(out.asset_id).await
    }

    /// `resolve` without the expectation.
    pub async fn resolve_result(&self, asset_id: AssetId) -> Result<DeliveredMediaView, MediaError> {
        self.resolve
            .handle_at(
                Envelope::new(Uuid::now_v7(), ResolveDeliveryQuery { asset_id, preferred: None, visibility: None }),
                Utc::now(),
            )
            .await
    }

    /// Uploads `bytes` as a private document (#777) and runs it to READY.
    pub async fn upload_document(&self, bytes: Vec<u8>, mime: &str) -> AssetId {
        let cmd = IssueUploadTicketCommand {
            owner_id: self.owner,
            kind: MediaKind::PrivateDocument,
            declared_mime: MimeType::new(mime).unwrap(),
            declared_size: bytes.len() as u64,
            content_sha256: None,
            idempotency_key: None,
        };
        let out = self.issue.handle(Envelope::new(Uuid::now_v7(), cmd), Utc::now()).await.expect("ticket");
        let url = out.upload.expect("a fresh upload").presigned.url;
        let put = self.http.put(&url).header(reqwest::header::CONTENT_TYPE, mime).body(bytes).send().await.unwrap();
        assert!(put.status().is_success(), "direct upload to MinIO failed: {}", put.status());
        self.commit(out.asset_id).await.expect("commit");
        assert_eq!(self.process(out.asset_id).await.expect("process"), ProcessOutcome::Ready);
        out.asset_id
    }

    /// True if an object exists at `key` in the store.
    pub async fn object_exists(&self, key: &StorageKey) -> bool {
        self.store.object_size(key.as_str()).await.expect("object_size").is_some()
    }

    /// Drives a fresh asset all the way to READY, returning its id.
    /// Uploads and processes an image with bytes of its own: content-addressed
    /// keys are shared by identical bytes, so concurrent scenarios must not share.
    pub async fn upload_and_process(&self) -> AssetId {
        let seed = Uuid::now_v7().as_bytes()[15];
        let bytes = Self::sample_jpeg(1200 + u32::from(seed), 800);
        let out = self.issue_ticket(bytes.len() as u64, None, false).await.unwrap();
        let url = out.upload.unwrap().presigned.url;
        assert!(self.put_to_url(&url, bytes).await, "direct upload to MinIO failed");
        self.commit(out.asset_id).await.unwrap();
        let outcome = self.process(out.asset_id).await.unwrap();
        assert_eq!(outcome, ProcessOutcome::Ready);
        out.asset_id
    }
}
