use std::sync::Arc;

use chrono::{DateTime, Utc};
use cqrs::Envelope;

use crate::application::policy::MediaPolicy;
use crate::application::command::asset_objects;
use crate::application::port::{
    AssetRepository, DeliveryCache, EventPublisher, ImageProcessor, MalwareScanner, ModerationScreen, ObjectStore,
    ScanVerdict,
};
use crate::domain::value_object::{AssetId, AssetState, StorageKey};
use crate::error::MediaError;

/// Plane B — process a finalized (`Uploaded`) asset. Driven off the `AssetUploaded`
/// event by a worker (Phase 5), not the synchronous RPC path.
#[derive(Debug, Clone)]
pub struct ProcessAssetCommand {
    pub asset_id: AssetId,
}

/// The terminal outcome of a processing run (all `Ok` at the consumer — the
/// distinctions drive observability and the worker's commit decision).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ProcessOutcome {
    Ready,
    Quarantined,
    Failed(String),
    /// The asset was not in `Uploaded` state (already processed / not finalized) —
    /// a redelivery no-op.
    Skipped,
}

/// Runs the transformation pipeline: malware scan → pre-publish moderation screen
/// (fail-closed) → derive the rendition ladder + BlurHash → mark READY. A screen
/// block quarantines (and, for CSAM, places a legal hold); a scan hit fails the
/// asset; a screen outage surfaces `ScreenUnavailable` so the worker retries.
pub struct ProcessAssetHandler {
    assets: Arc<dyn AssetRepository>,
    scanner: Arc<dyn MalwareScanner>,
    screen: Arc<dyn ModerationScreen>,
    processor: Arc<dyn ImageProcessor>,
    /// Moves a checked private document to its private key (#777).
    store: Arc<dyn ObjectStore>,
    cache: Arc<dyn DeliveryCache>,
    publisher: Arc<dyn EventPublisher>,
    policy: MediaPolicy,
}

impl ProcessAssetHandler {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        assets: Arc<dyn AssetRepository>,
        scanner: Arc<dyn MalwareScanner>,
        screen: Arc<dyn ModerationScreen>,
        processor: Arc<dyn ImageProcessor>,
        store: Arc<dyn ObjectStore>,
        cache: Arc<dyn DeliveryCache>,
        publisher: Arc<dyn EventPublisher>,
        policy: MediaPolicy,
    ) -> Self {
        Self { assets, scanner, screen, processor, store, cache, publisher, policy }
    }

    pub async fn handle(
        &self,
        envelope: Envelope<ProcessAssetCommand>,
        now: DateTime<Utc>,
    ) -> Result<ProcessOutcome, MediaError> {
        let cmd = envelope.payload;
        let mut asset = self
            .assets
            .find_by_id(&cmd.asset_id)
            .await?
            .ok_or_else(|| MediaError::AssetNotFound { id: cmd.asset_id.as_str() })?;

        if asset.state() != AssetState::Uploaded {
            return Ok(ProcessOutcome::Skipped);
        }

        let staging = StorageKey::staging(asset.id());
        // Owned so it doesn't borrow `asset` across the upcoming &mut transitions.
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
                // Catastrophic-category match → preserve evidence (blocks deletion).
                asset.place_legal_hold(now);
            }
            self.cache.invalidate(&asset.id()).await?;
            self.persist_and_publish(&mut asset).await?;
            return Ok(ProcessOutcome::Quarantined);
        }

        // 3'. A private document (#777): no renditions, never on the CDN. Its
        //     checked bytes move under `private/` (not granted to the CDN's
        //     origin access); it is ready as is.
        if asset.kind().is_private() {
            asset.begin_processing(now)?;
            self.store.relocate(&staging, &StorageKey::private_document(asset.id())).await?;
            asset.mark_ready(now)?;
            self.persist_and_publish(&mut asset).await?;
            return Ok(ProcessOutcome::Ready);
        }

        // 3. Derive the rendition ladder + BlurHash from the validated master.
        asset.begin_processing(now)?;
        let derived = self.processor.derive(&staging, asset.kind(), &hash).await?;
        asset.set_blurhash(derived.blurhash, now)?;
        for rendition in derived.renditions {
            asset.attach_rendition(rendition, now)?;
        }

        // 4. Publish.
        asset.mark_ready(now)?;
        self.persist_and_publish(&mut asset).await?;
        // Warm the delivery cache so the first read is hot.
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
    use crate::domain::value_object::{MediaKind, RenditionKind};
    use std::time::Duration as StdDuration;
    use uuid::Uuid;

    fn env(asset_id: AssetId) -> Envelope<ProcessAssetCommand> {
        Envelope::new(Uuid::now_v7(), ProcessAssetCommand { asset_id })
    }

    #[tokio::test]
    async fn clean_asset_processes_to_ready_with_renditions() {
        let fx = Fixture::new();
        let asset_id = fx.uploaded_asset(MediaKind::PostImage).await;
        fx.publisher.clear();

        let outcome = fx.process_handler().handle(env(asset_id), t0()).await.unwrap();
        assert_eq!(outcome, ProcessOutcome::Ready);

        let asset = fx.assets.find_by_id(&asset_id).await.unwrap().unwrap();
        assert_eq!(asset.state(), AssetState::Ready);
        assert!(asset.is_deliverable());
        assert!(asset.rendition(RenditionKind::Original).is_some());
        assert!(asset.blurhash().is_some());
        // variant-ready (x2) then ready, in order.
        let types = fx.publisher.event_types();
        assert_eq!(*types.last().unwrap(), "media.asset_ready");
        assert!(types.contains(&"media.asset_variant_ready"));
    }

    #[tokio::test]
    async fn a_screen_block_quarantines_and_skips_renditions() {
        let fx = Fixture::new();
        fx.screen.set_block(false);
        let asset_id = fx.uploaded_asset(MediaKind::PostImage).await;
        fx.publisher.clear();

        let outcome = fx.process_handler().handle(env(asset_id), t0()).await.unwrap();
        assert_eq!(outcome, ProcessOutcome::Quarantined);

        let asset = fx.assets.find_by_id(&asset_id).await.unwrap().unwrap();
        assert_eq!(asset.state(), AssetState::Quarantined);
        assert!(!asset.legal_hold(), "a non-CSAM block does not place a legal hold");
        assert_eq!(fx.publisher.event_types(), vec!["media.asset_quarantined"]);
    }

    #[tokio::test]
    async fn a_re_upload_of_taken_down_bytes_is_quarantined_on_arrival() {
        use crate::application::command::{ApplyModerationCommand, ModerationAction};
        let fx = Fixture::new();
        let (first, _) = fx.ready_asset(MediaKind::PostImage).await;
        fx.apply_moderation_handler()
            .handle(
                Envelope::new(Uuid::now_v7(), ApplyModerationCommand {
                    asset_id: first,
                    action: ModerationAction::Quarantine,
                    enforcement_id: Some("enf-1".into()),
                }),
                t0(),
            )
            .await
            .unwrap();
        {
            let mut held = fx.assets.find_by_id(&first).await.unwrap().unwrap();
            held.place_legal_hold(t0());
            fx.assets.save(&held).await.unwrap();
        }

        // The same bytes again (the fake probe hashes every upload alike).
        let again = fx.uploaded_asset(MediaKind::PostImage).await;
        let out = fx.process_handler().handle(Envelope::new(Uuid::now_v7(), ProcessAssetCommand { asset_id: again }), t0()).await.unwrap();
        assert_eq!(out, ProcessOutcome::Quarantined);
        let asset = fx.assets.find_by_id(&again).await.unwrap().unwrap();
        assert_eq!(asset.state(), AssetState::Quarantined);
        assert!(asset.renditions().is_empty(), "nothing written back to the public keys");
        assert!(asset.legal_hold(), "the evidence hold carries over");
        assert_eq!(asset.enforcements().iter().collect::<Vec<_>>(), vec!["enf-1"]);
    }

    #[tokio::test]
    async fn a_csam_block_also_places_a_legal_hold() {
        let fx = Fixture::new();
        fx.screen.set_block(true); // csam = true
        let asset_id = fx.uploaded_asset(MediaKind::PostImage).await;

        fx.process_handler().handle(env(asset_id), t0()).await.unwrap();
        let asset = fx.assets.find_by_id(&asset_id).await.unwrap().unwrap();
        assert_eq!(asset.state(), AssetState::Quarantined);
        assert!(asset.legal_hold(), "CSAM match preserves evidence via a legal hold");
    }

    #[tokio::test]
    async fn malware_fails_the_asset() {
        let fx = Fixture::new();
        fx.scanner.set_infected();
        let asset_id = fx.uploaded_asset(MediaKind::Avatar).await;

        let outcome = fx.process_handler().handle(env(asset_id), t0()).await.unwrap();
        assert!(matches!(outcome, ProcessOutcome::Failed(_)));
        let asset = fx.assets.find_by_id(&asset_id).await.unwrap().unwrap();
        assert_eq!(asset.state(), AssetState::Failed);
    }

    #[tokio::test]
    async fn a_screen_outage_fails_closed_as_unavailable() {
        let fx = Fixture::new();
        fx.screen.set_unavailable();
        let asset_id = fx.uploaded_asset(MediaKind::PostImage).await;

        let err = fx.process_handler().handle(env(asset_id), t0()).await.unwrap_err();
        assert!(matches!(err, MediaError::ScreenUnavailable));
        // Nothing published; the asset stays Uploaded for the worker to retry.
        let asset = fx.assets.find_by_id(&asset_id).await.unwrap().unwrap();
        assert_eq!(asset.state(), AssetState::Uploaded);
    }

    #[tokio::test]
    async fn a_slow_screen_trips_the_hard_timeout() {
        let mut fx = Fixture::new();
        fx.policy.screen_timeout = StdDuration::from_millis(10);
        fx.screen.set_delay(StdDuration::from_millis(150));
        let asset_id = fx.uploaded_asset(MediaKind::PostImage).await;

        let err = fx.process_handler().handle(env(asset_id), t0()).await.unwrap_err();
        assert!(matches!(err, MediaError::ScreenUnavailable));
    }

    #[tokio::test]
    async fn reprocessing_an_already_ready_asset_is_a_no_op() {
        let fx = Fixture::new();
        let asset_id = fx.uploaded_asset(MediaKind::PostImage).await;
        fx.process_handler().handle(env(asset_id), t0()).await.unwrap();
        let outcome = fx.process_handler().handle(env(asset_id), t0()).await.unwrap();
        assert_eq!(outcome, ProcessOutcome::Skipped);
    }
}
