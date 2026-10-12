//! The happy path over real backends: ticket → direct-to-MinIO PUT → commit →
//! process → READY, with renditions actually present in the store and resolvable
//! public URLs; plus content-hash dedup.

use media::application::command::ProcessOutcome;
use media::domain::value_object::{AssetState, DeliveryVisibility, RenditionKind};

use crate::media_it::harness::Harness;

#[tokio::test]
async fn upload_commit_process_yields_a_ready_asset_with_real_renditions() {
    let h = Harness::start().await;

    let bytes = Harness::sample_jpeg(1200, 800);
    let out = h.issue_ticket(bytes.len() as u64, None, false).await.unwrap();
    let ticket = out.upload.expect("a presigned upload plan");
    assert_eq!(ticket.presigned.method, "PUT");

    // The client uploads the bytes straight to MinIO (off the mesh).
    assert!(h.put_to_url(&ticket.presigned.url, bytes).await, "direct PUT to MinIO");

    // Finalize: the real probe reads the bytes back from MinIO.
    let asset = h.commit(out.asset_id).await.unwrap();
    assert_eq!(asset.state(), AssetState::Uploaded);
    assert!(asset.content_hash().is_some());

    // Process: the real image pipeline derives the rendition ladder + BlurHash and
    // writes each derivative to a content-addressed key in MinIO.
    assert_eq!(h.process(out.asset_id).await.unwrap(), ProcessOutcome::Ready);

    let ready = h.get(out.asset_id).await.unwrap();
    assert_eq!(ready.state(), AssetState::Ready);
    assert!(ready.blurhash().is_some(), "a blurhash was computed");
    let original = ready.rendition(RenditionKind::Original).expect("an original rendition");

    // The derivative object really exists in object storage.
    assert!(h.object_exists(original.storage_key()).await, "original rendition stored in MinIO");

    // Plane C resolves public, immutable (no-expiry) CDN URLs.
    let delivered = h.resolve(out.asset_id, Some(DeliveryVisibility::Public)).await;
    assert!(!delivered.degraded);
    assert!(!delivered.renditions.is_empty());
    assert!(delivered.renditions.iter().all(|r| r.expires_at.is_none()));
}

#[tokio::test]
async fn identical_bytes_dedup_onto_the_existing_asset() {
    let h = Harness::start().await;

    // First upload of these exact bytes, processed to READY.
    let bytes = Harness::sample_jpeg(640, 640);
    let sha = Harness::sha256_hex(&bytes);
    let first = h.issue_ticket(bytes.len() as u64, None, false).await.unwrap();
    assert!(h.put_to_url(&first.upload.unwrap().presigned.url, bytes).await);
    h.commit(first.asset_id).await.unwrap();
    assert_eq!(h.process(first.asset_id).await.unwrap(), ProcessOutcome::Ready);

    // A new ticket for the same content hash, with dedup enabled, short-circuits.
    let second = h.issue_ticket(1, Some(sha), true).await.unwrap();
    assert!(second.deduplicated, "the second upload deduplicated");
    assert!(second.upload.is_none(), "no upload needed on a dedup hit");
    // #924: an asset of its own over the same bytes, never another's id.
    assert_ne!(second.asset_id, first.asset_id);
    let (mine, theirs) = (h.get(second.asset_id).await.unwrap(), h.get(first.asset_id).await.unwrap());
    assert_eq!(mine.state(), AssetState::Ready);
    assert_eq!(mine.renditions(), theirs.renditions(), "the same content-addressed objects");
}
