//! Document-backed [`MediaProbe`] (#777): a private document that is not a
//! plain still image — a PDF or a HEIC photo — is checked by its magic bytes
//! (never the declared type), sized and hashed. It is never decoded: it is
//! only ever handed, as is, to staff — or, for a loader (#975), to its owner.
//! A HEIC's dimensions are read from its header (`ispe`), so a loader's
//! frame strip is checked at commit; a PDF has none.

use std::sync::Arc;

use async_trait::async_trait;
use sha2::{Digest, Sha256};

use crate::application::port::{MediaProbe, MediaProbeReport};
use crate::domain::value_object::{ContentHash, Dimensions, MimeType, StorageKey};
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

/// The ISOBMFF boxes in `data`: `(type, payload)`, stopping at a malformed one.
fn boxes(mut data: &[u8]) -> impl Iterator<Item = ([u8; 4], &[u8])> {
    std::iter::from_fn(move || {
        if data.len() < 8 {
            return None;
        }
        let size = u32::from_be_bytes(data[0..4].try_into().ok()?) as usize;
        let kind: [u8; 4] = data[4..8].try_into().ok()?;
        let (header, size) = match size {
            0 => (8, data.len()),
            1 => (16, usize::try_from(u64::from_be_bytes(data.get(8..16)?.try_into().ok()?)).ok()?),
            n => (8, n),
        };
        if size < header || size > data.len() {
            return None;
        }
        let payload = &data[header..size];
        data = &data[size..];
        Some((kind, payload))
    })
}

fn child<'a>(data: &'a [u8], kind: &[u8; 4]) -> Option<&'a [u8]> {
    boxes(data).find(|(k, _)| k == kind).map(|(_, payload)| payload)
}

/// A HEIC still's width × height: the largest image spatial extent (`ispe`)
/// among its item properties (`meta/iprp/ipco`) — the primary image's; tiles
/// and thumbnails are smaller. `None` when there is none.
pub fn heic_dimensions(bytes: &[u8]) -> Option<(u32, u32)> {
    // `meta` is a full box: 4 bytes of version and flags before its children.
    let meta = child(bytes, b"meta")?.get(4..)?;
    let ipco = child(child(meta, b"iprp")?, b"ipco")?;
    boxes(ipco)
        .filter(|(k, _)| k == b"ispe")
        .filter_map(|(_, p)| {
            let width = u32::from_be_bytes(p.get(4..8)?.try_into().ok()?);
            let height = u32::from_be_bytes(p.get(8..12)?.try_into().ok()?);
            Some((width, height))
        })
        .max_by_key(|(w, h)| u64::from(*w) * u64::from(*h))
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
        // Out-of-bounds dimensions read as none: a document does not need
        // them, and a loader without them is refused.
        let dimensions = (actual == "image/heic")
            .then(|| heic_dimensions(&bytes))
            .flatten()
            .and_then(|(w, h)| Dimensions::new(w, h).ok());
        Ok(MediaProbeReport {
            mime_type:    MimeType::new(actual)?,
            byte_size:    bytes.len() as u64,
            dimensions,
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

    fn full_box(kind: &[u8; 4], body: &[u8]) -> Vec<u8> {
        let mut out = ((body.len() + 12) as u32).to_be_bytes().to_vec();
        out.extend_from_slice(kind);
        out.extend_from_slice(&[0, 0, 0, 0]);
        out.extend_from_slice(body);
        out
    }

    fn plain_box(kind: &[u8; 4], body: &[u8]) -> Vec<u8> {
        let mut out = ((body.len() + 8) as u32).to_be_bytes().to_vec();
        out.extend_from_slice(kind);
        out.extend_from_slice(body);
        out
    }

    fn ispe(w: u32, h: u32) -> Vec<u8> {
        full_box(b"ispe", &[w.to_be_bytes(), h.to_be_bytes()].concat())
    }

    /// #975: a HEIC's size is its largest `ispe` (the primary image, not a
    /// tile or the thumbnail).
    #[test]
    fn a_heic_says_its_size_in_its_header() {
        let ipco = plain_box(b"ipco", &[ispe(512, 128), ispe(7680, 128), ispe(320, 5)].concat());
        let meta = full_box(b"meta", &[full_box(b"hdlr", b"pict"), plain_box(b"iprp", &ipco)].concat());
        let file = [plain_box(b"ftyp", b"heic\0\0\0\0mif1heic"), meta, plain_box(b"mdat", b"...")].concat();
        assert_eq!(sniff_document(&file), Some("image/heic"));
        assert_eq!(heic_dimensions(&file), Some((7680, 128)));
        assert_eq!(heic_dimensions(&file[..40]), None, "a truncated header has none");
        assert_eq!(heic_dimensions(b"\0\0\0\x18ftypheic\0\0\0\0"), None);
    }
}
