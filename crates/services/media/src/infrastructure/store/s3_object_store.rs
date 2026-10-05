use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration as StdDuration;

use async_trait::async_trait;
use chrono::Duration;

use crate::application::port::{ObjectHead, ObjectStore, PresignedUpload};
use crate::domain::value_object::{MimeType, StorageKey};
use crate::error::MediaError;

use super::s3_client::S3Client;

/// Adapts [`S3Client`] to the [`ObjectStore`] port.
pub struct S3ObjectStore {
    client: Arc<S3Client>,
}

impl S3ObjectStore {
    pub fn new(client: Arc<S3Client>) -> Self {
        Self { client }
    }
}

#[async_trait]
impl ObjectStore for S3ObjectStore {
    async fn presign_put(
        &self,
        key: &StorageKey,
        content_type: &MimeType,
        _max_bytes: u64,
        expires_in: Duration,
    ) -> Result<PresignedUpload, MediaError> {
        let ttl = expires_in.to_std().unwrap_or(StdDuration::from_secs(900));
        // Handed to the client for a direct upload — sign against the public host.
        let url = self.client.presign_put_public(key.as_str(), ttl);
        let mut required_headers = HashMap::new();
        required_headers.insert("Content-Type".to_owned(), content_type.as_str().to_owned());
        Ok(PresignedUpload {
            url: url.to_string(),
            method: "PUT".to_owned(),
            required_headers,
        })
    }

    async fn head(&self, key: &StorageKey) -> Result<Option<ObjectHead>, MediaError> {
        Ok(self
            .client
            .object_size(key.as_str())
            .await?
            .map(|size_bytes| ObjectHead { size_bytes, etag: String::new() }))
    }

    async fn delete(&self, key: &StorageKey) -> Result<(), MediaError> {
        self.client.delete(key.as_str()).await
    }

    async fn list(&self, prefix: &str) -> Result<Vec<StorageKey>, MediaError> {
        Ok(self.client.list_keys(prefix).await?.into_iter().map(StorageKey::from_raw).collect())
    }

    async fn relocate(&self, from: &StorageKey, to: &StorageKey) -> Result<(), MediaError> {
        // Copy through the service (rusty-s3 has no CopyObject), then delete: a
        // retry after a crash between the two re-copies the same bytes.
        let Some((bytes, content_type)) = self.client.get_object(from.as_str()).await? else {
            return Ok(());
        };
        self.client.put_bytes(to.as_str(), bytes, &content_type).await?;
        self.client.delete(from.as_str()).await
    }
}
