use std::sync::Arc;

use chrono::{DateTime, Utc};
use cqrs::Envelope;

use crate::application::command::ProcessOutcome;
use crate::application::policy::MediaPolicy;
use crate::application::command::asset_objects;
use crate::application::port::{
    AssetRepository, DeliveryCache, EventPublisher, MalwareScanner, ModerationScreen, ScanVerdict, SheetBaker,
    VideoTranscoder,
};
use crate::domain::value_object::{AssetId, AssetState, MediaKind, StorageKey};
use crate::error::MediaError;

/// Plane B for **video** — transcode a finalized (`Uploaded`) video asset into its
/// HLS ladder + poster and mark it READY. Driven off the `AssetUploaded` event in
/// the dedicated `media-worker` (ffmpeg is CPU-heavy), the sibling of
/// [`ProcessAssetHandler`](super::ProcessAssetHandler) for images.
///
/// The gate steps (malware scan → fail-closed moderation screen → persist) mirror
/// the image handler deliberately: the two paths are expected to diverge (e.g.
/// frame-sampling video screen) rather than share a premature abstraction.
#[derive(Debug, Clone)]
pub struct TranscodeAssetCommand {
    pub asset_id: AssetId,
}

pub struct TranscodeAssetHandler {
    assets: Arc<dyn AssetRepository>,
    scanner: Arc<dyn MalwareScanner>,
    screen: Arc<dyn ModerationScreen>,
    transcoder: Arc<dyn VideoTranscoder>,
    cache: Arc<dyn DeliveryCache>,
    publisher: Arc<dyn EventPublisher>,
    policy: MediaPolicy,
    /// Bakes animated avatars' clips (#945); `None`: a clip is not processed.
    baker: Option<Arc<dyn SheetBaker>>,
}

impl TranscodeAssetHandler {
    /// Animated avatars' clips are baked into sprite sheets (#945).
    pub fn with_baker(mut self, baker: Arc<dyn SheetBaker>) -> Self {
        self.baker = Some(baker);
        self
    }

    #[allow(clippy::too_many_arguments)]
    pub fn new(
        assets: Arc<dyn AssetRepository>,
        scanner: Arc<dyn MalwareScanner>,
        screen: Arc<dyn ModerationScreen>,
        transcoder: Arc<dyn VideoTranscoder>,
        cache: Arc<dyn DeliveryCache>,
        publisher: Arc<dyn EventPublisher>,
        policy: MediaPolicy,
    ) -> Self {
        Self { assets, scanner, screen, transcoder, cache, publisher, policy, baker: None }
    }

    pub async fn handle(
        &self,
        envelope: Envelope<TranscodeAssetCommand>,
        now: DateTime<Utc>,
    ) -> Result<ProcessOutcome, MediaError> {
        let cmd = envelope.payload;
        let mut asset = self
            .assets
            .find_by_id(&cmd.asset_id)
            .await?
            .ok_or_else(|| MediaError::AssetNotFound { id: cmd.asset_id.as_str() })?;

        // Defense in depth: the consumer already routes only video here, but a
        // non-video asset (or an already-processed one) is a committed no-op.
        if !matches!(asset.kind(), MediaKind::Video | MediaKind::AvatarClip) || asset.state() != AssetState::Uploaded {
            return Ok(ProcessOutcome::Skipped);
        }

        let staging = StorageKey::staging(asset.id());
        let hash = asset.content_hash().cloned().ok_or(MediaError::DomainViolation {
            field: "content_hash".into(),
            message: "a finalized asset must carry a content hash".into(),
        })?;

        // 1. Malware scan — a hit fails the asset terminally.
        if self.scanner.scan(&staging).await? == ScanVerdict::Infected {
            asset.mark_failed("malware detected", now)?;
            self.persist_and_publish(&mut asset).await?;
            return Ok(ProcessOutcome::Failed("malware detected".into()));
        }

        // Bytes that were taken down stay down, whoever uploads them again.
        if asset_objects::quarantine_if_taken_down(self.assets.as_ref(), &mut asset, now).await? {
            self.cache.invalidate(&asset.id()).await?;
            self.persist_and_publish(&mut asset).await?;
            return Ok(ProcessOutcome::Quarantined);
        }

        // 2. Pre-publish moderation screen (fail-closed, hard timeout).
        let asset_id = asset.id();
        let owner_id = asset.owner_id();
        let lookup = self.screen.screen(&asset_id, &owner_id, &hash, asset.kind());
        let decision = match tokio::time::timeout(self.policy.screen_timeout, lookup).await {
            Ok(result) => result?,
            Err(_elapsed) => return Err(MediaError::ScreenUnavailable),
        };
        if decision.blocked {
            asset.quarantine(now)?;
            if decision.csam {
                asset.place_legal_hold(now);
            }
            self.cache.invalidate(&asset.id()).await?;
            self.persist_and_publish(&mut asset).await?;
            return Ok(ProcessOutcome::Quarantined);
        }

        // 3. Transcode: HLS ladder + poster from the validated master.
        asset.begin_processing(now)?;
        // A SIGNED asset's own restricted tree, once enabled (#919).
        let root = asset.storage_root(self.policy.restricted_prefix_enabled).ok_or(MediaError::DomainViolation {
            field: "content_hash".into(),
            message: "a finalized asset must carry a content hash".into(),
        })?;
        let output = if asset.kind() == MediaKind::AvatarClip {
            let baker = self.baker.as_ref().ok_or_else(|| MediaError::ProcessingFailed { reason: "no sheet baker".into() })?;
            match baker.bake(&staging, &hash, &root).await {
                // A clip that cannot make a sheet fails for good (#945).
                Err(MediaError::CorruptMedia { reason }) => {
                    asset.mark_failed(reason.clone(), now)?;
                    self.persist_and_publish(&mut asset).await?;
                    return Ok(ProcessOutcome::Failed(reason));
                }
                output => output?,
            }
        } else {
            self.transcoder.transcode(&staging, &hash, &root).await?
        };
        asset.set_blurhash(output.blurhash, now)?;
        for rendition in output.renditions {
            asset.attach_rendition(rendition, now)?;
        }

        // 4. Publish.
        asset.mark_ready(now)?;
        self.persist_and_publish(&mut asset).await?;
        let _ = self.cache.put(&asset).await;
        Ok(ProcessOutcome::Ready)
    }

    async fn persist_and_publish(
        &self,
        asset: &mut crate::domain::aggregate::Asset,
    ) -> Result<(), MediaError> {
        self.assets.save(asset).await?;
        for event in asset.drain_events() {
            self.publisher.publish(&event).await?;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::application::fakes::{t0, Fixture};
    use crate::domain::value_object::RenditionKind;
    use uuid::Uuid;

    fn env(asset_id: AssetId) -> Envelope<TranscodeAssetCommand> {
        Envelope::new(Uuid::now_v7(), TranscodeAssetCommand { asset_id })
    }

    #[tokio::test]
    async fn clean_video_transcodes_to_ready_with_manifest_and_poster() {
        let fx = Fixture::new();
        let asset_id = fx.uploaded_asset(MediaKind::Video).await;
        fx.publisher.clear();

        let outcome = fx.transcode_handler().handle(env(asset_id), t0()).await.unwrap();
        assert_eq!(outcome, ProcessOutcome::Ready);

        let asset = fx.assets.find_by_id(&asset_id).await.unwrap().unwrap();
        assert_eq!(asset.state(), AssetState::Ready);
        assert!(asset.rendition(RenditionKind::Manifest).is_some());
        assert!(asset.rendition(RenditionKind::Poster).is_some());
        assert_eq!(*fx.publisher.event_types().last().unwrap(), "media.asset_ready");
    }

    #[tokio::test]
    async fn a_non_video_asset_is_skipped() {
        let fx = Fixture::new();
        // An image routed here by mistake must be a committed no-op, not transcoded.
        let asset_id = fx.uploaded_asset(MediaKind::PostImage).await;
        let outcome = fx.transcode_handler().handle(env(asset_id), t0()).await.unwrap();
        assert_eq!(outcome, ProcessOutcome::Skipped);
    }

    #[tokio::test]
    async fn a_screen_block_quarantines_and_skips_transcode() {
        let fx = Fixture::new();
        fx.screen.set_block(false);
        let asset_id = fx.uploaded_asset(MediaKind::Video).await;
        fx.publisher.clear();

        let outcome = fx.transcode_handler().handle(env(asset_id), t0()).await.unwrap();
        assert_eq!(outcome, ProcessOutcome::Quarantined);
        let asset = fx.assets.find_by_id(&asset_id).await.unwrap().unwrap();
        assert_eq!(asset.state(), AssetState::Quarantined);
    }

    /// #945: an animated avatar's clip is baked into its two sheets and the
    /// still, then READY (its master is the large sheet).
    #[tokio::test]
    async fn an_avatar_clip_is_baked_into_sheets_and_a_still() {
        let fx = Fixture::new();
        let asset_id = fx.uploaded_asset(MediaKind::AvatarClip).await;
        let outcome = fx.transcode_handler().handle(env(asset_id), t0()).await.unwrap();
        assert_eq!(outcome, ProcessOutcome::Ready);
        let asset = fx.assets.find_by_id(&asset_id).await.unwrap().unwrap();
        assert_eq!(asset.state(), AssetState::Ready);
        for kind in [RenditionKind::SheetSmall, RenditionKind::SheetLarge, RenditionKind::Poster] {
            assert!(asset.rendition(kind).is_some(), "{kind}");
        }
        assert!(asset.rendition(RenditionKind::Manifest).is_none(), "never served as a video");
    }

    /// A clip that cannot make a sheet fails for good, not retried.
    #[tokio::test]
    async fn a_clip_that_does_not_loop_fails() {
        let fx = Fixture::new();
        fx.transcoder.refuse_bake.store(true, std::sync::atomic::Ordering::SeqCst);
        let asset_id = fx.uploaded_asset(MediaKind::AvatarClip).await;
        let outcome = fx.transcode_handler().handle(env(asset_id), t0()).await.unwrap();
        assert!(matches!(outcome, ProcessOutcome::Failed(ref reason) if reason.contains("loop")));
        assert_eq!(fx.assets.find_by_id(&asset_id).await.unwrap().unwrap().state(), AssetState::Failed);
    }
}
