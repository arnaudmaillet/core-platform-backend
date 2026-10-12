use std::io::Cursor;
use std::sync::Arc;

use async_trait::async_trait;
use image::{DynamicImage, ImageFormat};

use crate::application::port::{DerivedRenditions, ImageProcessor};
use crate::domain::aggregate::Rendition;
use crate::domain::value_object::{
    Blurhash, Dimensions, MediaKind, MimeType, ObjectRoot, RenditionKind, StorageKey,
};
use crate::error::MediaError;

use crate::infrastructure::store::S3Client;

/// The resize ladder (longest-side caps). The master (`Original`) and `Thumbnail`
/// are always produced; larger buckets only when the source exceeds them (never
/// upscale).
const LADDER: &[(RenditionKind, u32)] = &[
    (RenditionKind::Thumbnail, 320),
    (RenditionKind::Small, 640),
    (RenditionKind::Medium, 1280),
    (RenditionKind::Large, 1920),
];

/// `image`-backed [`ImageProcessor`]. v1 encodes every rendition as JPEG (broad
/// support); WebP/AVIF format optimization is a follow-up. Derivatives are written
/// under the root they are given: content-addressed keys (identical bytes land on
/// identical URLs), or a SIGNED asset's own restricted tree (#919).
pub struct ImageRenditionProcessor {
    store: Arc<S3Client>,
}

impl ImageRenditionProcessor {
    pub fn new(store: Arc<S3Client>) -> Self {
        Self { store }
    }

    /// Encodes an image as JPEG, uploads it to its key under `root`, and builds
    /// the [`Rendition`] record.
    async fn encode_put(
        &self,
        img: &DynamicImage,
        root: &ObjectRoot,
        rk: RenditionKind,
    ) -> Result<Rendition, MediaError> {
        let mut buf = Vec::new();
        img.write_to(&mut Cursor::new(&mut buf), ImageFormat::Jpeg).map_err(|e| {
            MediaError::ProcessingFailed { reason: format!("jpeg encode failed: {e}") }
        })?;
        let key = root.rendition(rk, "jpg");
        let byte_size = buf.len() as u64;
        self.store.put_bytes(key.as_str(), buf, "image/jpeg").await?;
        Ok(Rendition::new(
            rk,
            MimeType::new("image/jpeg")?,
            key,
            Dimensions::new(img.width(), img.height())?,
            byte_size,
        ))
    }
}

#[async_trait]
impl ImageProcessor for ImageRenditionProcessor {
    async fn derive(
        &self,
        source: &StorageKey,
        _kind: MediaKind,
        root: &ObjectRoot,
    ) -> Result<DerivedRenditions, MediaError> {
        let bytes = self.store.get_bytes(source.as_str()).await?;
        let img = image::load_from_memory(&bytes).map_err(|e| MediaError::CorruptMedia {
            reason: format!("decode failed: {e}"),
        })?;
        let longest = img.width().max(img.height());

        let mut renditions = Vec::new();
        // The master, re-encoded at full resolution.
        renditions.push(self.encode_put(&img, root, RenditionKind::Original).await?);
        // The ladder: thumbnail always; larger buckets only if the source exceeds them.
        for &(rk, target) in LADDER {
            if rk == RenditionKind::Thumbnail || target < longest {
                let resized = img.resize(target, target, image::imageops::FilterType::Triangle);
                renditions.push(self.encode_put(&resized, root, rk).await?);
            }
        }

        // BlurHash from a small RGBA thumbnail (4×3 components is the common choice).
        let small = img.thumbnail(64, 64).to_rgba8();
        let blur = blurhash::encode(4, 3, small.width(), small.height(), small.as_raw())
            .map_err(|e| MediaError::ProcessingFailed { reason: format!("blurhash: {e}") })?;
        let blurhash = Blurhash::new(blur)?;

        Ok(DerivedRenditions { renditions, blurhash })
    }
}
