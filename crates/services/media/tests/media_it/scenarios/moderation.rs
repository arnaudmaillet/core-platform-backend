//! Compliance over real backends: a CSAM screen block quarantines + legal-holds the
//! asset (and the hold blocks deletion); a takedown then restore round-trips.

use media::application::command::{ModerationAction, ProcessOutcome};
use media::domain::value_object::{AssetState, DeliveryVisibility};
use media::error::MediaError;

use crate::media_it::harness::Harness;

#[tokio::test]
async fn a_csam_block_quarantines_legal_holds_and_blocks_deletion() {
    let h = Harness::start().await;
    h.screen.set_block(true); // blocked + csam

    let bytes = Harness::sample_jpeg(800, 600);
    let out = h.issue_ticket(bytes.len() as u64, None, false).await.unwrap();
    assert!(h.put_to_url(&out.upload.unwrap().presigned.url, bytes).await);
    h.commit(out.asset_id).await.unwrap();

    // The pipeline screens the content, blocks it, and quarantines + legal-holds.
    assert_eq!(h.process(out.asset_id).await.unwrap(), ProcessOutcome::Quarantined);

    let asset = h.get(out.asset_id).await.unwrap();
    assert_eq!(asset.state(), AssetState::Quarantined);
    assert!(asset.legal_hold(), "a CSAM match places a legal hold");

    // Delivery is revoked: resolve fails open to a placeholder.
    let delivered = h.resolve(out.asset_id, Some(DeliveryVisibility::Public)).await;
    assert!(delivered.degraded);
    assert!(delivered.renditions.is_empty());

    // The legal hold blocks erasure (MED-7003).
    let err = h.delete(out.asset_id).await.unwrap_err();
    assert!(matches!(err, MediaError::LegalHoldActive));
}

#[tokio::test]
async fn a_takedown_then_restore_round_trips_delivery() {
    let h = Harness::start().await;
    let asset_id = h.upload_and_process().await;

    let keys: Vec<_> =
        h.get(asset_id).await.unwrap().renditions().iter().map(|r| r.storage_key().clone()).collect();
    assert!(!keys.is_empty());
    for key in &keys {
        assert!(h.store.object_size(key.as_str()).await.unwrap().is_some(), "published: {key}");
    }

    // Moderation quarantine revokes delivery — at the origin too: a public URL
    // handed out before no longer resolves; the bytes wait under quarantine/.
    h.apply_moderation(asset_id, ModerationAction::Quarantine).await;
    assert_eq!(h.get(asset_id).await.unwrap().state(), AssetState::Quarantined);
    assert!(h.resolve(asset_id, None).await.degraded);
    for key in &keys {
        assert!(h.store.object_size(key.as_str()).await.unwrap().is_none(), "gone from the public key: {key}");
        assert!(h.store.object_size(key.quarantined().as_str()).await.unwrap().is_some(), "kept: {key}");
    }
    // A redelivered takedown is harmless.
    h.apply_moderation(asset_id, ModerationAction::Quarantine).await;
    assert!(h.store.object_size(keys[0].quarantined().as_str()).await.unwrap().is_some());

    // Reversal restores it to deliverable, bytes back at their public keys.
    h.apply_moderation(asset_id, ModerationAction::Restore).await;
    let restored = h.get(asset_id).await.unwrap();
    assert_eq!(restored.state(), AssetState::Ready);
    assert!(!h.resolve(asset_id, Some(DeliveryVisibility::Public)).await.degraded);
    for key in &keys {
        assert!(h.store.object_size(key.as_str()).await.unwrap().is_some(), "restored: {key}");
        assert!(h.store.object_size(key.quarantined().as_str()).await.unwrap().is_none());
    }
}
