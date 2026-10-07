//! Document-backed [`MediaProbe`] (#777): a private document that is not a
//! plain still image — a PDF or a HEIC photo — is checked by its magic bytes
//! (never the declared type), sized and hashed. It has no dimensions and is
//! never decoded: it is only ever handed, as is, to staff.

use std::sync::Arc;

use async_trait::async_trait;
use sha2::{Digest, Sha256};

use crate::application::port::{MediaProbe, MediaProbeReport};
use crate::domain::value_object::{ContentHash, MimeType, StorageKey};
use crate::error::MediaError;
use crate::infrastructure::store::S3Client;

/// HEIF brands a HEIC still is filed under (`ftyp` major brand).
const HEIC_BRANDS: [&[u8; 4]; 5] = [b"heic", b"heix", b"hevc", b"mif1", b"msf1"];

/// The document type the bytes really are, by their magic bytes.
pub fn sniff_document(bytes: &[u8]) -> Option<&'static str> {
    if bytes.starts_with(b"%PDF-") {
        return Some("application/pdf");
    }
    if bytes.starts_with(&[0xFF, 0xD8, 0xFF]) {
        return Some("image/jpeg");
    }
    if bytes.starts_with(&[0x89, b'P', b'N', b'G', 0x0D, 0x0A, 0x1A, 0x0A]) {
        return Some("image/png");
    }
    if bytes.len() >= 12 && &bytes[4..8] == b"ftyp" && HEIC_BRANDS.iter().any(|b| &bytes[8..12] == *b) {
        return Some("image/heic");
    }
    None
}

pub struct DocumentMediaProbe {
    store: Arc<S3Client>,
}

impl DocumentMediaProbe {
    pub fn new(store: Arc<S3Client>) -> Self {
        Self { store }
    }
}

#[async_trait]
impl MediaProbe for DocumentMediaProbe {
    async fn probe(&self, key: &StorageKey, declared_mime: &MimeType) -> Result<MediaProbeReport, MediaError> {
        let bytes = self.store.get_bytes(key.as_str()).await?;
        let actual = sniff_document(&bytes)
            .ok_or_else(|| MediaError::CorruptMedia { reason: "not a PDF, JPEG, PNG or HEIC document".into() })?;
        if actual != declared_mime.as_str() {
            return Err(MediaError::ContentTypeMismatch {
                declared: declared_mime.as_str().to_owned(),
                actual:   actual.to_owned(),
            });
        }
        let hex: String = Sha256::digest(&bytes).iter().map(|b| format!("{b:02x}")).collect();
        Ok(MediaProbeReport {
            mime_type:    MimeType::new(actual)?,
            byte_size:    bytes.len() as u64,
            dimensions:   None,
            content_hash: ContentHash::new(hex)?,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn documents_are_told_by_their_magic_bytes() {
        assert_eq!(sniff_document(b"%PDF-1.7\n..."), Some("application/pdf"));
        assert_eq!(sniff_document(&[0xFF, 0xD8, 0xFF, 0xE0, 0, 0]), Some("image/jpeg"));
        assert_eq!(sniff_document(&[0x89, b'P', b'N', b'G', 0x0D, 0x0A, 0x1A, 0x0A, 0]), Some("image/png"));
        assert_eq!(sniff_document(b"\0\0\0\x18ftypheic\0\0\0\0"), Some("image/heic"));
        assert_eq!(sniff_document(b"\0\0\0\x18ftypmp42\0\0\0\0"), None, "an MP4 is not a document");
        assert_eq!(sniff_document(b"<html>"), None);
        assert_eq!(sniff_document(b""), None);
    }
}
