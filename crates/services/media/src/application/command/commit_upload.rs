use std::sync::Arc;

use chrono::{DateTime, Utc};
use cqrs::Envelope;

use crate::application::port::{AssetRepository, EventPublisher, MediaProbe, ObjectStore};
use crate::domain::aggregate::{Asset, FinalizeParams};
use crate::domain::value_object::{AssetId, AssetState, ContentHash, StorageKey, UploadConstraints};
use crate::error::MediaError;

/// Plane A — the low-latency finalize nudge after the client's direct upload. The
/// object-store event is the authoritative trigger; this RPC and that event
/// converge idempotently on the same finalize.
#[derive(Debug, Clone)]
pub struct CommitUploadCommand {
    pub asset_id: AssetId,
    pub etag: Option<String>,
    /// Optional client-computed SHA-256 (hex), cross-checked against the probe.
    pub content_sha256: Option<String>,
}

/// Finalizes a pending upload: confirms the bytes landed, probes them for the
/// verified facts (never trusting the client's declaration), transitions the asset
/// to `Uploaded`, and emits `AssetUploaded` — the trigger for the async pipeline.
pub struct CommitUploadHandler {
    assets: Arc<dyn AssetRepository>,
    store: Arc<dyn ObjectStore>,
    probe: Arc<dyn MediaProbe>,
    publisher: Arc<dyn EventPublisher>,
}

impl CommitUploadHandler {
    pub fn new(
        assets: Arc<dyn AssetRepository>,
        store: Arc<dyn ObjectStore>,
        probe: Arc<dyn MediaProbe>,
        publisher: Arc<dyn EventPublisher>,
    ) -> Self {
        Self { assets, store, probe, publisher }
    }

    pub async fn handle(
        &self,
        envelope: Envelope<CommitUploadCommand>,
        now: DateTime<Utc>,
    ) -> Result<Asset, MediaError> {
        let cmd = envelope.payload;
        let mut asset = self
            .assets
            .find_by_id(&cmd.asset_id)
            .await?
            .ok_or_else(|| MediaError::AssetNotFound { id: cmd.asset_id.as_str() })?;

        // Idempotent: a commit that races the S3 event (or a double-tap) finds the
        // asset already finalized — return it unchanged.
        if asset.state() != AssetState::Pending {
            return Ok(asset);
        }

        let staging = StorageKey::staging(asset.id());
        // The bytes must actually be present — a commit ahead of the upload is a
        // precondition failure, not a finalize.
        if self.store.head(&staging).await?.is_none() {
            return Err(MediaError::UploadNotFinalized);
        }

        // Probe the real bytes for the verified facts (magic-byte type, true size,
        // dimensions, content hash).
        let report = self.probe.probe(&staging, asset.declared_mime()).await?;

        // If the client asserted a hash, it must match what we actually stored.
        if let Some(declared) = cmd.content_sha256.as_deref()
            && ContentHash::new(declared)? != report.content_hash
        {
            return Err(MediaError::CorruptMedia {
                reason: "client-declared content hash does not match the stored object".into(),
            });
        }

        let constraints = UploadConstraints::for_kind(asset.kind());
        asset.finalize(
            FinalizeParams {
                mime_type: report.mime_type,
                byte_size: report.byte_size,
                dimensions: report.dimensions,
                content_hash: report.content_hash,
            },
            &constraints,
            now,
        )?;

        self.assets.save(&asset).await?;
        for event in asset.drain_events() {
            self.publisher.publish(&event).await?;
        }
        Ok(asset)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::application::fakes::{t0, Fixture, TEST_HASH};
    use crate::domain::value_object::MediaKind;
    use uuid::Uuid;

    fn env(c: CommitUploadCommand) -> Envelope<CommitUploadCommand> {
        Envelope::new(Uuid::now_v7(), c)
    }

    #[tokio::test]
    async fn finalizes_a_present_upload_and_emits_uploaded() {
        let fx = Fixture::new();
        let asset_id = fx.reserve_and_upload(MediaKind::PostImage).await;

        let asset = fx
            .commit_handler()
            .handle(env(CommitUploadCommand { asset_id, etag: None, content_sha256: None }), t0())
            .await
            .unwrap();

        assert_eq!(asset.state(), AssetState::Uploaded);
        assert_eq!(asset.content_hash().unwrap().as_str(), TEST_HASH);
        assert_eq!(fx.publisher.event_types(), vec!["media.asset_uploaded"]);
    }

    #[tokio::test]
    async fn commit_without_bytes_is_a_precondition_failure() {
        let fx = Fixture::new();
        // Reserve but do NOT upload the bytes.
        let asset_id = fx.reserve_only(MediaKind::Avatar).await;
        let err = fx
            .commit_handler()
            .handle(env(CommitUploadCommand { asset_id, etag: None, content_sha256: None }), t0())
            .await
            .unwrap_err();
        assert!(matches!(err, MediaError::UploadNotFinalized));
    }

    #[tokio::test]
    async fn commit_is_idempotent_on_an_already_finalized_asset() {
        let fx = Fixture::new();
        let asset_id = fx.reserve_and_upload(MediaKind::PostImage).await;
        let cmd = || CommitUploadCommand { asset_id, etag: None, content_sha256: None };
        fx.commit_handler().handle(env(cmd()), t0()).await.unwrap();
        fx.publisher.clear();
        // Second commit: no state change, no new event.
        let asset = fx.commit_handler().handle(env(cmd()), t0()).await.unwrap();
        assert_eq!(asset.state(), AssetState::Uploaded);
        assert_eq!(fx.publisher.count(), 0);
    }

    #[tokio::test]
    async fn a_mismatched_declared_hash_is_rejected_as_corrupt() {
        let fx = Fixture::new();
        let asset_id = fx.reserve_and_upload(MediaKind::PostImage).await;
        let err = fx
            .commit_handler()
            .handle(
                env(CommitUploadCommand {
                    asset_id,
                    etag: None,
                    content_sha256: Some("f".repeat(64)),
                }),
                t0(),
            )
            .await
            .unwrap_err();
        assert!(matches!(err, MediaError::CorruptMedia { .. }));
    }

    /// #975: a loader that is not one strip of 1–60 square 128 px frames is
    /// refused at commit, and stays PENDING (nothing to process or charge).
    #[tokio::test]
    async fn a_loader_that_is_not_a_frame_strip_is_refused_at_commit() {
        use crate::application::port::MediaProbeReport;
        use crate::domain::value_object::{ContentHash, Dimensions, MimeType};
        let fx = Fixture::new();
        for (width, height) in [(200, 128), (128 * 61, 128), (128, 256)] {
            let asset_id = fx.reserve_and_upload(MediaKind::Loader).await;
            fx.probe.set_report(MediaProbeReport {
                mime_type:    MimeType::new("image/png").unwrap(),
                byte_size:    300_000,
                dimensions:   Some(Dimensions::new(width, height).unwrap()),
                content_hash: ContentHash::new(TEST_HASH).unwrap(),
            });
            let err = fx
                .commit_handler()
                .handle(env(CommitUploadCommand { asset_id, etag: None, content_sha256: None }), t0())
                .await
                .unwrap_err();
            assert!(matches!(err, MediaError::CorruptMedia { .. }), "{width} × {height}: {err:?}");
            let asset = fx.assets.find_by_id(&asset_id).await.unwrap().unwrap();
            assert_eq!(asset.state(), AssetState::Pending);
        }
    }
}
