//! The byte-plane port: pre-signed uploads, object inspection, and deletion. The
//! concrete adapter (S3/MinIO) lives in `infrastructure` (Phase 4). Bytes never
//! cross this trait — only keys, URLs, and small metadata.

use std::collections::HashMap;

use async_trait::async_trait;
use chrono::Duration;

use crate::domain::value_object::{MimeType, StorageKey};
use crate::error::MediaError;

/// A pre-signed upload plan the client uses to PUT bytes directly to the store.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PresignedUpload {
    pub url: String,
    pub method: String,
    /// Headers the client MUST echo for the signature to validate.
    pub required_headers: HashMap<String, String>,
}

/// The result of inspecting a stored object (`HEAD`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ObjectHead {
    pub size_bytes: u64,
    pub etag: String,
}

#[async_trait]
pub trait ObjectStore: Send + Sync + 'static {
    /// Mints a pre-signed PUT URL scoped to `key`, bounded by `content_type` and
    /// `max_bytes`, valid for `expires_in`.
    async fn presign_put(
        &self,
        key: &StorageKey,
        content_type: &MimeType,
        max_bytes: u64,
        expires_in: Duration,
    ) -> Result<PresignedUpload, MediaError>;

    /// Inspects an object; `None` when it does not (yet) exist — the signal a
    /// commit raced ahead of the upload (`UploadNotFinalized`).
    async fn head(&self, key: &StorageKey) -> Result<Option<ObjectHead>, MediaError>;

    /// Deletes an object. Idempotent: deleting an absent key is `Ok`.
    async fn delete(&self, key: &StorageKey) -> Result<(), MediaError>;

    /// Every key under `prefix`.
    async fn list(&self, prefix: &str) -> Result<Vec<StorageKey>, MediaError>;

    /// Moves `from` to `to` (server-side copy, then delete). Idempotent: an
    /// absent `from` (already moved) is `Ok`.
    async fn relocate(&self, from: &StorageKey, to: &StorageKey) -> Result<(), MediaError>;
}
