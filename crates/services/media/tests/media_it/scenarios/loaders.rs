//! #975 over real Postgres + MinIO: a PNG strip uploaded as a loader is
//! checked at commit (one strip of square 128 px frames), moved under
//! `private/loaders/`, never delivered, served to its owner alone by a signed
//! link, and gone once deleted.

use std::io::Cursor;

use chrono::Utc;
use image::{DynamicImage, ImageFormat, Rgba, RgbaImage};

use media::domain::value_object::{AssetState, MediaKind, OwnerId, StorageKey};
use media::error::MediaError;

use crate::media_it::harness::Harness;

/// A PNG strip of `width` × `height` with alpha.
fn png(width: u32, height: u32) -> Vec<u8> {
    let img = RgbaImage::from_fn(width, height, |x, y| Rgba([(x % 256) as u8, (y % 256) as u8, 90, (x % 200) as u8 + 55]));
    let mut buf = Vec::new();
    DynamicImage::ImageRgba8(img).write_to(&mut Cursor::new(&mut buf), ImageFormat::Png).expect("encode png");
    buf
}

#[tokio::test]
async fn a_loader_strip_is_checked_kept_privately_and_served_to_its_owner_alone() {
    let h = Harness::start().await;
    let bytes = png(128 * 12, 128);
    let committed = h.upload_raw(MediaKind::Loader, bytes.clone(), "image/png").await.expect("commit");
    let loader = committed.id();
    assert_eq!(h.process(loader).await.expect("process"), media::application::command::ProcessOutcome::Ready);

    let asset = h.get(loader).await.unwrap();
    assert_eq!(asset.state(), AssetState::Ready);
    assert!(asset.renditions().is_empty());
    let key = StorageKey::private(MediaKind::Loader, loader);
    assert!(h.object_exists(&key).await, "moved under private/loaders/");
    assert!(!h.object_exists(&StorageKey::staging(loader)).await, "no staging copy left");
    assert!(matches!(h.resolve_result(loader).await, Err(MediaError::AssetNotFound { .. })), "never delivered");

    // Its owner: a signed link serving the very bytes, and its frame count.
    let resolved = h.loaders.handle_at(h.owner(), &[loader], Utc::now()).await.unwrap();
    assert_eq!(resolved.len(), 1);
    assert_eq!(resolved[0].frames, 12);
    let served = reqwest::get(&resolved[0].link.url).await.unwrap();
    assert!(served.status().is_success(), "{}", served.status());
    assert_eq!(served.bytes().await.unwrap().to_vec(), bytes);
    // Anyone else: nothing.
    let stranger = OwnerId::from_uuid(uuid::Uuid::now_v7());
    assert!(h.loaders.handle_at(stranger, &[loader], Utc::now()).await.unwrap().is_empty());

    // Not a strip of square 128 px frames: refused at commit.
    for (w, h_px) in [(200, 128), (128, 256), (128 * 61, 128)] {
        let refused = h.upload_raw(MediaKind::Loader, png(w, h_px), "image/png").await;
        assert!(matches!(refused, Err(MediaError::CorruptMedia { .. })), "{w} × {h_px}: {refused:?}");
    }
    // Not PNG or HEIC: refused at the ticket.
    let jpeg = h.upload_raw(MediaKind::Loader, Harness::sample_jpeg(128, 128), "image/jpeg").await;
    assert!(matches!(jpeg, Err(MediaError::UnsupportedMimeType { .. })), "{jpeg:?}");

    // Deleted: the strip goes and the link no longer resolves.
    h.delete(loader).await.expect("delete");
    assert!(!h.object_exists(&key).await, "the private object is gone");
    assert!(h.loaders.handle_at(h.owner(), &[loader], Utc::now()).await.unwrap().is_empty());
}
