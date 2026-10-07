//! #777 over real Postgres + MinIO: a PDF uploaded as a private document is
//! checked by its magic bytes, moved under `private/`, never delivered, its
//! staff link serves the bytes, and the purge after its request's decision
//! removes it.

use chrono::{Duration, Utc};
use uuid::Uuid;

use media::domain::value_object::{AssetState, StorageKey};
use media::error::MediaError;

use crate::media_it::harness::Harness;

fn pdf() -> Vec<u8> {
    let mut bytes = b"%PDF-1.7\n%\xe2\xe3\xcf\xd3\n".to_vec();
    bytes.extend_from_slice(Uuid::now_v7().as_bytes()); // bytes of its own
    bytes.extend_from_slice(b"\n%%EOF\n");
    bytes
}

#[tokio::test]
async fn a_private_pdf_is_kept_privately_handed_to_staff_and_purged_after_the_decision() {
    let h = Harness::start().await;
    let bytes = pdf();
    let doc = h.upload_document(bytes.clone(), "application/pdf").await;

    let asset = h.get(doc).await.unwrap();
    assert_eq!(asset.state(), AssetState::Ready);
    assert!(asset.renditions().is_empty() && asset.dimensions().is_none());
    let key = StorageKey::private_document(doc);
    assert!(h.object_exists(&key).await, "moved under private/");
    assert!(!h.object_exists(&StorageKey::staging(doc)).await, "no staging copy left");

    // Never delivered.
    let delivered = h
        .resolve_result(doc)
        .await;
    assert!(matches!(delivered, Err(MediaError::AssetNotFound { .. })), "{delivered:?}");

    // Staff: a signed link that serves the very bytes.
    let link = h.staff.handle_at(doc, "staff-1", Utc::now()).await.unwrap();
    let served = reqwest::get(&link.url).await.unwrap();
    assert!(served.status().is_success(), "{}", served.status());
    assert_eq!(served.bytes().await.unwrap().to_vec(), bytes);

    // A file that claims to be a PDF but is not is refused at commit.
    let fake = h.upload_document_raw(b"<html>not a pdf</html>".to_vec(), "application/pdf").await;
    assert!(matches!(fake, Err(MediaError::CorruptMedia { .. })), "{fake:?}");

    // The decision: purged 30 days later, not before.
    let decided = Utc::now();
    assert_eq!(h.retention.on_decision(&[doc], &h.owner(), decided, decided).await.unwrap(), 1);
    assert_eq!(h.retention.sweep(decided + Duration::days(29)).await.unwrap(), 0);
    h.retention.sweep(decided + Duration::days(31)).await.unwrap();
    assert_eq!(h.get(doc).await.unwrap().state(), AssetState::Deleted);
    assert!(!h.object_exists(&key).await, "the private object is gone");
}
