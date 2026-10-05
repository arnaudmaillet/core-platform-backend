//! In-memory fakes for every outbound port, plus a [`Fixture`] that wires them into
//! the handlers. Test-only (`#[cfg(test)]`) — they prove the application layer works
//! against the port contracts with no real backend, which is exactly what makes the
//! abstraction credible before the Phase 4 adapters exist.

// Fakes are constructed explicitly via `new()` / `Fixture::new()`; a `Default`
// impl would add noise without a caller.
#![allow(clippy::new_without_default)]

use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::Duration as StdDuration;

use async_trait::async_trait;
use chrono::{DateTime, Duration, Utc};
use cqrs::Envelope;
use uuid::Uuid;

use super::command::{
    CommitUploadCommand, IssueUploadTicketCommand, ProcessAssetCommand,
};
use super::policy::MediaPolicy;
use super::port::{
    AssetRepository, CdnGateway, DeliveryCache, DerivedRenditions, EventPublisher, ImageProcessor,
    MalwareScanner, MediaProbe, MediaProbeReport, ModerationScreen, ObjectHead, ObjectStore,
    PresignedUpload, ResolvedUrl, ScanVerdict, ScreenDecision, TranscodeOutput, VideoTranscoder,
};
use crate::domain::aggregate::{Asset, AssetSnapshot, Rendition};
use crate::domain::event::DomainEvent;
use crate::domain::value_object::{
    AssetId, AssetState, Blurhash, ContentHash, DeliveryVisibility, Dimensions, MediaKind, MimeType,
    OwnerId, RenditionKind, StorageKey,
};
use crate::error::MediaError;

/// A fixed reference instant for deterministic tests.
pub fn t0() -> DateTime<Utc> {
    DateTime::parse_from_rfc3339("2026-06-26T12:00:00Z")
        .unwrap()
        .with_timezone(&Utc)
}

/// The canonical SHA-256 the [`StubMediaProbe`] reports, so the whole pipeline
/// shares one content hash.
pub const TEST_HASH: &str = "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855";

fn owner() -> OwnerId {
    OwnerId::from_uuid(Uuid::from_u128(7))
}

fn jpeg() -> MimeType {
    MimeType::new("image/jpeg").unwrap()
}

/// A declared MIME accepted by the kind's allowlist (video kinds reject images).
fn mime_for_kind(kind: MediaKind) -> MimeType {
    match kind {
        MediaKind::Video => MimeType::new("video/mp4").unwrap(),
        _ => jpeg(),
    }
}

// ─── AssetRepository ─────────────────────────────────────────────────────────────

pub struct InMemoryAssetRepository {
    assets: Mutex<HashMap<AssetId, Asset>>,
}

impl InMemoryAssetRepository {
    pub fn new() -> Self {
        Self { assets: Mutex::new(HashMap::new()) }
    }
}

#[async_trait]
impl AssetRepository for InMemoryAssetRepository {
    async fn save(&self, asset: &Asset) -> Result<(), MediaError> {
        let mut stored = asset.clone();
        let _ = stored.drain_events(); // events do not survive persistence
        self.assets.lock().unwrap().insert(stored.id(), stored);
        Ok(())
    }

    async fn find_by_id(&self, id: &AssetId) -> Result<Option<Asset>, MediaError> {
        Ok(self.assets.lock().unwrap().get(id).cloned())
    }

    async fn find_ready_by_content_hash(
        &self,
        hash: &ContentHash,
    ) -> Result<Option<Asset>, MediaError> {
        Ok(self
            .assets
            .lock()
            .unwrap()
            .values()
            .find(|a| a.state() == AssetState::Ready && a.content_hash() == Some(hash))
            .cloned())
    }
}

// ─── DeliveryCache ───────────────────────────────────────────────────────────────

pub struct InMemoryDeliveryCache {
    entries: Mutex<HashMap<AssetId, Asset>>,
}

impl InMemoryDeliveryCache {
    pub fn new() -> Self {
        Self { entries: Mutex::new(HashMap::new()) }
    }
}

#[async_trait]
impl DeliveryCache for InMemoryDeliveryCache {
    async fn get(&self, id: &AssetId) -> Result<Option<Asset>, MediaError> {
        Ok(self.entries.lock().unwrap().get(id).cloned())
    }

    async fn put(&self, asset: &Asset) -> Result<(), MediaError> {
        let mut stored = asset.clone();
        let _ = stored.drain_events();
        self.entries.lock().unwrap().insert(stored.id(), stored);
        Ok(())
    }

    async fn invalidate(&self, id: &AssetId) -> Result<(), MediaError> {
        self.entries.lock().unwrap().remove(id);
        Ok(())
    }
}

// ─── ObjectStore ─────────────────────────────────────────────────────────────────

pub struct StubObjectStore {
    objects: Mutex<HashMap<String, ObjectHead>>,
}

impl StubObjectStore {
    pub fn new() -> Self {
        Self { objects: Mutex::new(HashMap::new()) }
    }

    /// Simulates the client's direct-to-store upload landing.
    pub fn put_object(&self, key: &StorageKey, size_bytes: u64, etag: &str) {
        self.objects
            .lock()
            .unwrap()
            .insert(key.as_str().to_owned(), ObjectHead { size_bytes, etag: etag.to_owned() });
    }
}

#[async_trait]
impl ObjectStore for StubObjectStore {
    async fn presign_put(
        &self,
        key: &StorageKey,
        content_type: &MimeType,
        _max_bytes: u64,
        _expires_in: Duration,
    ) -> Result<PresignedUpload, MediaError> {
        let mut required_headers = HashMap::new();
        required_headers.insert("Content-Type".to_owned(), content_type.as_str().to_owned());
        Ok(PresignedUpload {
            url: format!("https://store.local/{}?X-Sig=fake", key.as_str()),
            method: "PUT".to_owned(),
            required_headers,
        })
    }

    async fn head(&self, key: &StorageKey) -> Result<Option<ObjectHead>, MediaError> {
        Ok(self.objects.lock().unwrap().get(key.as_str()).cloned())
    }

    async fn delete(&self, key: &StorageKey) -> Result<(), MediaError> {
        self.objects.lock().unwrap().remove(key.as_str());
        Ok(())
    }

    async fn list(&self, prefix: &str) -> Result<Vec<StorageKey>, MediaError> {
        let mut keys: Vec<StorageKey> = self
            .objects
            .lock()
            .unwrap()
            .keys()
            .filter(|k| k.starts_with(prefix))
            .map(|k| StorageKey::from_raw(k.clone()))
            .collect();
        keys.sort_by(|a, b| a.as_str().cmp(b.as_str()));
        Ok(keys)
    }

    async fn relocate(&self, from: &StorageKey, to: &StorageKey) -> Result<(), MediaError> {
        let mut objects = self.objects.lock().unwrap();
        if let Some(head) = objects.remove(from.as_str()) {
            objects.insert(to.as_str().to_owned(), head);
        }
        Ok(())
    }
}

impl StubObjectStore {
    /// Every key currently stored.
    pub fn keys(&self) -> Vec<String> {
        let mut keys: Vec<String> = self.objects.lock().unwrap().keys().cloned().collect();
        keys.sort();
        keys
    }
}

// ─── CdnGateway ──────────────────────────────────────────────────────────────────

pub struct RecordingCdnGateway {
    invalidated: Mutex<Vec<String>>,
    failing: std::sync::atomic::AtomicBool,
}

impl RecordingCdnGateway {
    pub fn new() -> Self {
        Self { invalidated: Mutex::new(Vec::new()), failing: std::sync::atomic::AtomicBool::new(false) }
    }

    pub fn invalidated_keys(&self) -> Vec<String> {
        self.invalidated.lock().unwrap().clone()
    }

    /// Makes `invalidate` fail (a CloudFront outage) until called with `false`.
    pub fn fail(&self, failing: bool) {
        self.failing.store(failing, std::sync::atomic::Ordering::SeqCst);
    }
}

#[async_trait]
impl CdnGateway for RecordingCdnGateway {
    async fn resolve(
        &self,
        key: &StorageKey,
        visibility: DeliveryVisibility,
        now: DateTime<Utc>,
    ) -> Result<ResolvedUrl, MediaError> {
        let expires_at = visibility.is_expiring().then(|| now + Duration::minutes(5));
        Ok(ResolvedUrl {
            url: format!("https://cdn.local/{}", key.as_str()),
            expires_at,
        })
    }

    async fn invalidate(&self, keys: &[StorageKey]) -> Result<(), MediaError> {
        if self.failing.load(std::sync::atomic::Ordering::SeqCst) {
            return Err(MediaError::CdnInvalidationFailed { reason: "edge down (fake)".into() });
        }
        let mut log = self.invalidated.lock().unwrap();
        for k in keys {
            log.push(k.as_str().to_owned());
        }
        Ok(())
    }
}

// ─── MediaProbe ──────────────────────────────────────────────────────────────────

pub struct StubMediaProbe {
    report: Mutex<MediaProbeReport>,
}

impl StubMediaProbe {
    pub fn new() -> Self {
        Self {
            report: Mutex::new(MediaProbeReport {
                mime_type: jpeg(),
                byte_size: 2_000_000,
                dimensions: Dimensions::new(1920, 1080).unwrap(),
                content_hash: ContentHash::new(TEST_HASH).unwrap(),
            }),
        }
    }

    pub fn set_report(&self, report: MediaProbeReport) {
        *self.report.lock().unwrap() = report;
    }
}

#[async_trait]
impl MediaProbe for StubMediaProbe {
    async fn probe(
        &self,
        _key: &StorageKey,
        declared_mime: &MimeType,
    ) -> Result<MediaProbeReport, MediaError> {
        // Echo the declared type so a video asset yields a video report (the stub
        // reports fixed dimensions/size/hash); real probes derive it from bytes.
        let mut report = self.report.lock().unwrap().clone();
        report.mime_type = declared_mime.clone();
        Ok(report)
    }
}

// ─── ImageProcessor ──────────────────────────────────────────────────────────────

pub struct StubImageProcessor;

impl StubImageProcessor {
    pub fn new() -> Self {
        Self
    }
}

#[async_trait]
impl ImageProcessor for StubImageProcessor {
    async fn derive(
        &self,
        _source: &StorageKey,
        kind: MediaKind,
        hash: &ContentHash,
    ) -> Result<DerivedRenditions, MediaError> {
        let original = Rendition::new(
            RenditionKind::Original,
            jpeg(),
            StorageKey::rendition(kind, hash, RenditionKind::Original, "jpg"),
            Dimensions::new(1920, 1080).unwrap(),
            2_000_000,
        );
        let thumbnail = Rendition::new(
            RenditionKind::Thumbnail,
            MimeType::new("image/webp").unwrap(),
            StorageKey::rendition(kind, hash, RenditionKind::Thumbnail, "webp"),
            Dimensions::new(320, 180).unwrap(),
            20_000,
        );
        Ok(DerivedRenditions {
            renditions: vec![original, thumbnail],
            blurhash: Blurhash::new("LEHV6nWB2yk8pyo0adR*").unwrap(),
        })
    }
}

// ─── VideoTranscoder ─────────────────────────────────────────────────────────────

pub struct StubVideoTranscoder;

impl StubVideoTranscoder {
    pub fn new() -> Self {
        Self
    }
}

#[async_trait]
impl VideoTranscoder for StubVideoTranscoder {
    async fn transcode(
        &self,
        _source: &StorageKey,
        hash: &ContentHash,
    ) -> Result<TranscodeOutput, MediaError> {
        let manifest = Rendition::new(
            RenditionKind::Manifest,
            MimeType::new("application/vnd.apple.mpegurl").unwrap(),
            StorageKey::video_object(MediaKind::Video, hash, "master.m3u8"),
            Dimensions::new(1080, 1920).unwrap(),
            1_200,
        );
        let poster = Rendition::new(
            RenditionKind::Poster,
            jpeg(),
            StorageKey::video_object(MediaKind::Video, hash, "poster.jpg"),
            Dimensions::new(1080, 1920).unwrap(),
            40_000,
        );
        Ok(TranscodeOutput {
            renditions: vec![manifest, poster],
            blurhash: Blurhash::new("LEHV6nWB2yk8pyo0adR*").unwrap(),
        })
    }
}

// ─── MalwareScanner ──────────────────────────────────────────────────────────────

pub struct StubMalwareScanner {
    infected: Mutex<bool>,
}

impl StubMalwareScanner {
    pub fn new() -> Self {
        Self { infected: Mutex::new(false) }
    }

    pub fn set_infected(&self) {
        *self.infected.lock().unwrap() = true;
    }
}

#[async_trait]
impl MalwareScanner for StubMalwareScanner {
    async fn scan(&self, _key: &StorageKey) -> Result<ScanVerdict, MediaError> {
        Ok(if *self.infected.lock().unwrap() {
            ScanVerdict::Infected
        } else {
            ScanVerdict::Clean
        })
    }
}

// ─── ModerationScreen ────────────────────────────────────────────────────────────

#[derive(Clone, Copy)]
enum ScreenMode {
    Allow,
    Block { csam: bool },
    Unavailable,
}

pub struct StubModerationScreen {
    mode: Mutex<ScreenMode>,
    delay: Mutex<Option<StdDuration>>,
}

impl StubModerationScreen {
    pub fn new() -> Self {
        Self {
            mode: Mutex::new(ScreenMode::Allow),
            delay: Mutex::new(None),
        }
    }

    /// `csam = true` flags a catastrophic-category match (warrants a legal hold).
    pub fn set_block(&self, csam: bool) {
        *self.mode.lock().unwrap() = ScreenMode::Block { csam };
    }

    pub fn set_unavailable(&self) {
        *self.mode.lock().unwrap() = ScreenMode::Unavailable;
    }

    /// Makes the gate answer after `delay` — to drive the hard-timeout path.
    pub fn set_delay(&self, delay: StdDuration) {
        *self.delay.lock().unwrap() = Some(delay);
    }
}

#[async_trait]
impl ModerationScreen for StubModerationScreen {
    async fn screen(
        &self,
        _asset_id: &AssetId,
        _owner_id: &OwnerId,
        _content_hash: &ContentHash,
        _kind: MediaKind,
    ) -> Result<ScreenDecision, MediaError> {
        let delay = *self.delay.lock().unwrap();
        if let Some(d) = delay {
            tokio::time::sleep(d).await;
        }
        let mode = *self.mode.lock().unwrap();
        match mode {
            ScreenMode::Allow => Ok(ScreenDecision::allow()),
            ScreenMode::Block { csam } => Ok(ScreenDecision {
                blocked: true,
                csam,
                reference: Some("ncmec:test".to_owned()),
            }),
            ScreenMode::Unavailable => Err(MediaError::ScreenUnavailable),
        }
    }
}

// ─── EventPublisher ──────────────────────────────────────────────────────────────

pub struct RecordingEventPublisher {
    events: Mutex<Vec<DomainEvent>>,
}

impl RecordingEventPublisher {
    pub fn new() -> Self {
        Self { events: Mutex::new(Vec::new()) }
    }

    pub fn event_types(&self) -> Vec<&'static str> {
        self.events.lock().unwrap().iter().map(|e| e.event_type()).collect()
    }

    pub fn count(&self) -> usize {
        self.events.lock().unwrap().len()
    }

    pub fn clear(&self) {
        self.events.lock().unwrap().clear();
    }
}

#[async_trait]
impl EventPublisher for RecordingEventPublisher {
    async fn publish(&self, event: &DomainEvent) -> Result<(), MediaError> {
        self.events.lock().unwrap().push(event.clone());
        Ok(())
    }
}

// ─── Fixture ─────────────────────────────────────────────────────────────────────

/// Bundles concrete fakes and builds handlers wired to them. Handlers receive the
/// fakes as `Arc<dyn Port>`; tests keep the concrete `Arc`s to assert on recorded
/// state (published events, invalidated keys, stored assets).
pub struct Fixture {
    pub assets: Arc<InMemoryAssetRepository>,
    pub cache: Arc<InMemoryDeliveryCache>,
    pub store: Arc<StubObjectStore>,
    pub cdn: Arc<RecordingCdnGateway>,
    pub probe: Arc<StubMediaProbe>,
    pub processor: Arc<StubImageProcessor>,
    pub transcoder: Arc<StubVideoTranscoder>,
    pub scanner: Arc<StubMalwareScanner>,
    pub screen: Arc<StubModerationScreen>,
    pub publisher: Arc<RecordingEventPublisher>,
    pub policy: MediaPolicy,
}

impl Fixture {
    pub fn new() -> Self {
        Self {
            assets: Arc::new(InMemoryAssetRepository::new()),
            cache: Arc::new(InMemoryDeliveryCache::new()),
            store: Arc::new(StubObjectStore::new()),
            cdn: Arc::new(RecordingCdnGateway::new()),
            probe: Arc::new(StubMediaProbe::new()),
            processor: Arc::new(StubImageProcessor::new()),
            transcoder: Arc::new(StubVideoTranscoder::new()),
            scanner: Arc::new(StubMalwareScanner::new()),
            screen: Arc::new(StubModerationScreen::new()),
            publisher: Arc::new(RecordingEventPublisher::new()),
            policy: MediaPolicy::test_default(),
        }
    }

    // ─── Handler builders ────────────────────────────────────────────────────

    pub fn issue_ticket_handler(&self) -> super::command::IssueUploadTicketHandler {
        super::command::IssueUploadTicketHandler::new(
            Arc::clone(&self.assets) as _,
            Arc::clone(&self.store) as _,
            self.policy.clone(),
        )
    }

    pub fn commit_handler(&self) -> super::command::CommitUploadHandler {
        super::command::CommitUploadHandler::new(
            Arc::clone(&self.assets) as _,
            Arc::clone(&self.store) as _,
            Arc::clone(&self.probe) as _,
            Arc::clone(&self.publisher) as _,
        )
    }

    pub fn process_handler(&self) -> super::command::ProcessAssetHandler {
        super::command::ProcessAssetHandler::new(
            Arc::clone(&self.assets) as _,
            Arc::clone(&self.scanner) as _,
            Arc::clone(&self.screen) as _,
            Arc::clone(&self.processor) as _,
            Arc::clone(&self.cache) as _,
            Arc::clone(&self.publisher) as _,
            self.policy.clone(),
        )
    }

    pub fn transcode_handler(&self) -> super::command::TranscodeAssetHandler {
        super::command::TranscodeAssetHandler::new(
            Arc::clone(&self.assets) as _,
            Arc::clone(&self.scanner) as _,
            Arc::clone(&self.screen) as _,
            Arc::clone(&self.transcoder) as _,
            Arc::clone(&self.cache) as _,
            Arc::clone(&self.publisher) as _,
            self.policy.clone(),
        )
    }

    pub fn delete_handler(&self) -> super::command::DeleteAssetHandler {
        super::command::DeleteAssetHandler::new(
            Arc::clone(&self.assets) as _,
            Arc::clone(&self.store) as _,
            Arc::clone(&self.cdn) as _,
            Arc::clone(&self.cache) as _,
            Arc::clone(&self.publisher) as _,
        )
    }

    pub fn apply_moderation_handler(&self) -> super::command::ApplyModerationHandler {
        super::command::ApplyModerationHandler::new(
            Arc::clone(&self.assets) as _,
            Arc::clone(&self.store) as _,
            Arc::clone(&self.cdn) as _,
            Arc::clone(&self.cache) as _,
            Arc::clone(&self.publisher) as _,
        )
    }

    pub fn get_asset_handler(&self) -> super::query::GetAssetHandler {
        super::query::GetAssetHandler::new(Arc::clone(&self.assets) as _)
    }

    pub fn resolve_delivery_handler(&self) -> super::query::ResolveDeliveryHandler {
        super::query::ResolveDeliveryHandler::new(
            Arc::clone(&self.assets) as _,
            Arc::clone(&self.cache) as _,
            Arc::clone(&self.cdn) as _,
        )
    }

    // ─── Scenario builders ───────────────────────────────────────────────────

    /// Reserves a `Pending` asset via the real handler; does NOT upload bytes.
    pub async fn reserve_only(&self, kind: MediaKind) -> AssetId {
        let cmd = IssueUploadTicketCommand {
            owner_id: owner(),
            kind,
            declared_mime: mime_for_kind(kind),
            declared_size: 2_000_000,
            content_sha256: None,
            idempotency_key: None,
        };
        self.issue_ticket_handler()
            .handle(Envelope::new(Uuid::now_v7(), cmd), t0())
            .await
            .unwrap()
            .asset_id
    }

    /// Reserves and lands the bytes in the store (so `commit` finds them).
    pub async fn reserve_and_upload(&self, kind: MediaKind) -> AssetId {
        let id = self.reserve_only(kind).await;
        self.store.put_object(&StorageKey::staging(id), 2_000_000, "etag-1");
        id
    }

    /// Drives an asset to `Uploaded` (reserve → upload → commit).
    pub async fn uploaded_asset(&self, kind: MediaKind) -> AssetId {
        let id = self.reserve_and_upload(kind).await;
        self.commit_handler()
            .handle(
                Envelope::new(
                    Uuid::now_v7(),
                    CommitUploadCommand { asset_id: id, etag: None, content_sha256: None },
                ),
                t0(),
            )
            .await
            .unwrap();
        id
    }

    /// Drives an asset all the way to `Ready` and returns `(id, owner)`.
    pub async fn ready_asset(&self, kind: MediaKind) -> (AssetId, OwnerId) {
        let id = self.uploaded_asset(kind).await;
        self.process_handler()
            .handle(Envelope::new(Uuid::now_v7(), ProcessAssetCommand { asset_id: id }), t0())
            .await
            .unwrap();
        (id, owner())
    }

    /// Seeds a READY asset with a specific content hash directly into the repo
    /// (for the dedup lookup, independent of the pipeline).
    pub async fn seed_ready_asset(&self, hash_hex: &str) -> AssetId {
        let id = AssetId::new();
        let hash = ContentHash::new(hash_hex).unwrap();
        let kind = MediaKind::PostImage;
        let original = Rendition::new(
            RenditionKind::Original,
            jpeg(),
            StorageKey::rendition(kind, &hash, RenditionKind::Original, "jpg"),
            Dimensions::new(1920, 1080).unwrap(),
            2_000_000,
        );
        let asset = Asset::reconstitute(AssetSnapshot {
            id,
            owner_id: owner(),
            kind,
            state: AssetState::Ready,
            declared_mime: jpeg(),
            declared_size: 2_000_000,
            mime_type: Some(jpeg()),
            byte_size: Some(2_000_000),
            dimensions: Some(Dimensions::new(1920, 1080).unwrap()),
            content_hash: Some(hash),
            blurhash: Some(Blurhash::new("LEHV6nWB2yk8pyo0adR*").unwrap()),
            renditions: vec![original],
            legal_hold: false,
            prior_state: None,
            created_at: t0(),
            updated_at: t0(),
        });
        self.assets.save(&asset).await.unwrap();
        id
    }
}
