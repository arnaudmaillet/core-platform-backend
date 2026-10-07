use std::sync::Arc;

use async_trait::async_trait;
use sha2::{Digest, Sha256};

use crate::application::port::{MediaProbe, MediaProbeReport};
use crate::domain::value_object::{ContentHash, Dimensions, MimeType, StorageKey};
use crate::error::MediaError;

use crate::infrastructure::store::S3Client;

/// Image-backed [`MediaProbe`]: downloads the uploaded object and establishes the
/// verified facts — the real format (from magic bytes), the true dimensions (with
/// the decode-bomb guard in [`Dimensions`]), the byte size, and the SHA-256.
pub struct ImageMediaProbe {
    store: Arc<S3Client>,
}

impl ImageMediaProbe {
    pub fn new(store: Arc<S3Client>) -> Self {
        Self { store }
    }
}

#[async_trait]
impl MediaProbe for ImageMediaProbe {
    async fn probe(
        &self,
        key: &StorageKey,
        declared_mime: &MimeType,
    ) -> Result<MediaProbeReport, MediaError> {
        let bytes = self.store.get_bytes(key.as_str()).await?;

        // SHA-256 of the original bytes (content-addressing + dedup).
        let mut hasher = Sha256::new();
        hasher.update(&bytes);
        let hex: String = hasher.finalize().iter().map(|b| format!("{b:02x}")).collect();
        let content_hash = ContentHash::new(hex)?;

        // Real format from magic bytes — never the declared type.
        let format = image::guess_format(&bytes).map_err(|e| MediaError::CorruptMedia {
            reason: format!("unrecognized media format: {e}"),
        })?;
        let mime_type = MimeType::new(format.to_mime_type())?;
        if !mime_type.is_image() {
            return Err(MediaError::ContentTypeMismatch {
                declared: declared_mime.as_str().to_owned(),
                actual: mime_type.as_str().to_owned(),
            });
        }

        // Decode for true dimensions (the decode-bomb guard lives in Dimensions).
        let img = image::load_from_memory(&bytes).map_err(|e| MediaError::CorruptMedia {
            reason: format!("decode failed: {e}"),
        })?;
        let dimensions = Dimensions::new(img.width(), img.height())?;

        Ok(MediaProbeReport {
            mime_type,
            byte_size: bytes.len() as u64,
            dimensions: Some(dimensions),
            content_hash,
        })
    }
}
