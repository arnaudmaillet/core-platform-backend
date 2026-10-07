//! The finalize-time content probe: inspect the *uploaded bytes* and return the
//! verified facts. This is the "never trust the client" gate — the adapter reads
//! the object's magic bytes, real dimensions, true size, and SHA-256, independent
//! of what the client declared. A type that contradicts the declaration is a
//! `ContentTypeMismatch`; an undecodable file is `CorruptMedia`.

use async_trait::async_trait;

use crate::domain::value_object::{ContentHash, Dimensions, MimeType, StorageKey};
use crate::error::MediaError;

/// The server-verified facts about an uploaded object.
#[derive(Debug, Clone, PartialEq)]
pub struct MediaProbeReport {
    pub mime_type: MimeType,
    pub byte_size: u64,
    /// `None` for a document (a PDF has none).
    pub dimensions: Option<Dimensions>,
    pub content_hash: ContentHash,
}

#[async_trait]
pub trait MediaProbe: Send + Sync + 'static {
    /// Probes the object at `key`, cross-checking against the client's
    /// `declared_mime`. Returns the verified report or a content error.
    async fn probe(
        &self,
        key: &StorageKey,
        declared_mime: &MimeType,
    ) -> Result<MediaProbeReport, MediaError>;
}
