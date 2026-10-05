use std::time::Duration;

use reqwest::header::{CONTENT_LENGTH, CONTENT_RANGE, CONTENT_TYPE, RANGE};
use reqwest::StatusCode;
use rusty_s3::{Bucket, Credentials, S3Action, UrlStyle};
use url::Url;

use super::config::S3Config;
use crate::error::MediaError;

/// A thin S3-compatible client: presigns URLs (for the client's direct upload and
/// for signed delivery) and performs the server-side byte I/O the pipeline needs.
/// Bytes only ever flow store ⇄ this worker — never through the gRPC/Kafka mesh.
pub struct S3Client {
    bucket: Bucket,
    /// Same bucket signed against the client-reachable host — used only for the
    /// URLs handed back to callers (upload PUT, signed delivery GET), never for
    /// this service's own byte I/O.
    public_bucket: Bucket,
    credentials: Credentials,
    http: reqwest::Client,
    presign_ttl: Duration,
}

impl S3Client {
    pub fn new(config: S3Config) -> Result<Self, MediaError> {
        let endpoint = Url::parse(&config.endpoint).map_err(|e| MediaError::PresignFailed {
            reason: format!("invalid object-store endpoint: {e}"),
        })?;
        let public_endpoint =
            Url::parse(&config.public_endpoint).map_err(|e| MediaError::PresignFailed {
                reason: format!("invalid object-store public endpoint: {e}"),
            })?;
        let bucket =
            Bucket::new(endpoint, UrlStyle::Path, config.bucket.clone(), config.region.clone())
                .map_err(|e| MediaError::PresignFailed {
                    reason: format!("invalid bucket: {e}"),
                })?;
        let public_bucket =
            Bucket::new(public_endpoint, UrlStyle::Path, config.bucket, config.region).map_err(
                |e| MediaError::PresignFailed { reason: format!("invalid public bucket: {e}") },
            )?;
        let credentials = Credentials::new(config.access_key, config.secret_key);
        // The HTTP client carries the hard request timeout: a stuck object-store
        // call elapses into `ObjectStoreTimeout` (retryable) rather than hanging.
        let http = reqwest::Client::builder()
            .timeout(config.request_timeout)
            .build()
            .map_err(|e| MediaError::PresignFailed {
                reason: format!("failed to build the object-store HTTP client: {e}"),
            })?;
        Ok(Self { bucket, public_bucket, credentials, http, presign_ttl: config.presign_ttl })
    }

    /// Presigned PUT URL for this service's own server-side byte I/O (rendition
    /// upload). Signed against the internal endpoint — not for handing to clients.
    pub fn presign_put(&self, key: &str, ttl: Duration) -> Url {
        self.bucket.put_object(Some(&self.credentials), key).sign(ttl)
    }

    /// Presigned GET URL for this service's own server-side byte I/O (probe
    /// download, ranged HEAD). Signed against the internal endpoint.
    pub fn presign_get(&self, key: &str, ttl: Duration) -> Url {
        self.bucket.get_object(Some(&self.credentials), key).sign(ttl)
    }

    /// Presigned PUT URL handed to a client for its direct upload. Signed against
    /// the client-reachable public endpoint.
    pub fn presign_put_public(&self, key: &str, ttl: Duration) -> Url {
        self.public_bucket.put_object(Some(&self.credentials), key).sign(ttl)
    }

    /// Presigned GET URL handed to a client for signed delivery. Signed against
    /// the client-reachable public endpoint.
    pub fn presign_get_public(&self, key: &str, ttl: Duration) -> Url {
        self.public_bucket.get_object(Some(&self.credentials), key).sign(ttl)
    }

    fn presign_delete(&self, key: &str, ttl: Duration) -> Url {
        self.bucket.delete_object(Some(&self.credentials), key).sign(ttl)
    }

    /// Server-side download of an object's bytes (for probe / derive).
    pub async fn get_bytes(&self, key: &str) -> Result<Vec<u8>, MediaError> {
        let url = self.presign_get(key, self.presign_ttl);
        let resp = self.http.get(url).send().await.map_err(reqwest_err)?;
        if resp.status() == StatusCode::NOT_FOUND {
            return Err(MediaError::ObjectNotFound { key: key.to_owned() });
        }
        if !resp.status().is_success() {
            return Err(MediaError::ObjectStoreUnavailable);
        }
        Ok(resp.bytes().await.map_err(reqwest_err)?.to_vec())
    }

    /// Server-side upload of a derivative object.
    pub async fn put_bytes(
        &self,
        key: &str,
        bytes: Vec<u8>,
        content_type: &str,
    ) -> Result<(), MediaError> {
        let url = self.presign_put(key, self.presign_ttl);
        let resp = self
            .http
            .put(url)
            .header(CONTENT_TYPE, content_type)
            .body(bytes)
            .send()
            .await
            .map_err(reqwest_err)?;
        if !resp.status().is_success() {
            return Err(MediaError::ObjectStoreUnavailable);
        }
        Ok(())
    }

    /// An object's bytes and content type, or `None` when it does not exist.
    pub async fn get_object(&self, key: &str) -> Result<Option<(Vec<u8>, String)>, MediaError> {
        let url = self.presign_get(key, self.presign_ttl);
        let resp = self.http.get(url).send().await.map_err(reqwest_err)?;
        if resp.status() == StatusCode::NOT_FOUND {
            return Ok(None);
        }
        if !resp.status().is_success() {
            return Err(MediaError::ObjectStoreUnavailable);
        }
        let content_type = resp
            .headers()
            .get(CONTENT_TYPE)
            .and_then(|v| v.to_str().ok())
            .unwrap_or("application/octet-stream")
            .to_owned();
        Ok(Some((resp.bytes().await.map_err(reqwest_err)?.to_vec(), content_type)))
    }

    /// Every key under `prefix` (ListObjectsV2, followed across pages).
    pub async fn list_keys(&self, prefix: &str) -> Result<Vec<String>, MediaError> {
        let mut keys = Vec::new();
        let mut token: Option<String> = None;
        loop {
            let mut action = self.bucket.list_objects_v2(Some(&self.credentials));
            action.with_prefix(prefix);
            if let Some(token) = &token {
                action.with_continuation_token(token.as_str());
            }
            let url = action.sign(self.presign_ttl);
            let resp = self.http.get(url).send().await.map_err(reqwest_err)?;
            if !resp.status().is_success() {
                return Err(MediaError::ObjectStoreUnavailable);
            }
            let body = resp.text().await.map_err(reqwest_err)?;
            let page = rusty_s3::actions::ListObjectsV2::parse_response(&body)
                .map_err(|_| MediaError::ObjectStoreUnavailable)?;
            keys.extend(page.contents.into_iter().map(|c| c.key));
            match page.next_continuation_token {
                Some(next) => token = Some(next),
                None => return Ok(keys),
            }
        }
    }

    /// Deletes an object. Idempotent: a 404 is treated as success.
    pub async fn delete(&self, key: &str) -> Result<(), MediaError> {
        let url = self.presign_delete(key, self.presign_ttl);
        let resp = self.http.delete(url).send().await.map_err(reqwest_err)?;
        let status = resp.status();
        if status.is_success() || status == StatusCode::NOT_FOUND {
            Ok(())
        } else {
            Err(MediaError::ObjectStoreUnavailable)
        }
    }

    /// Returns an object's size, or `None` if it does not exist. Implemented as a
    /// ranged GET (`bytes=0-0`) so it works without a presigned HEAD action.
    pub async fn object_size(&self, key: &str) -> Result<Option<u64>, MediaError> {
        let url = self.presign_get(key, self.presign_ttl);
        let resp = self
            .http
            .get(url)
            .header(RANGE, "bytes=0-0")
            .send()
            .await
            .map_err(reqwest_err)?;
        let status = resp.status();
        if status == StatusCode::NOT_FOUND {
            return Ok(None);
        }
        if !(status.is_success() || status == StatusCode::PARTIAL_CONTENT) {
            return Err(MediaError::ObjectStoreUnavailable);
        }
        // Prefer the total from `Content-Range: bytes 0-0/<total>`, else
        // `Content-Length`.
        let size = resp
            .headers()
            .get(CONTENT_RANGE)
            .and_then(|v| v.to_str().ok())
            .and_then(|s| s.rsplit('/').next().map(str::to_owned))
            .and_then(|total| total.parse::<u64>().ok())
            .or_else(|| {
                resp.headers()
                    .get(CONTENT_LENGTH)
                    .and_then(|v| v.to_str().ok())
                    .and_then(|s| s.parse::<u64>().ok())
            })
            .unwrap_or(0);
        Ok(Some(size))
    }

    pub fn presign_ttl(&self) -> Duration {
        self.presign_ttl
    }

    /// Liveness probe: a cheap reachability check against the bucket. A missing
    /// sentinel object (404) is healthy; only a transport/5xx fault is unhealthy.
    pub async fn health(&self) -> Result<(), MediaError> {
        self.object_size("__healthcheck__").await.map(|_| ())
    }

    /// Verifies the bucket is reachable, creating it only when absent.
    ///
    /// PROBE FIRST (HeadBucket — `s3:ListBucket` is granted to the media static
    /// keys), CreateBucket only on 404 (local/test path). Against provisioned
    /// AWS the keys deliberately lack s3:CreateBucket, so the old create-first
    /// probe 403'd at boot — found live on the staging bring-up.
    pub async fn ensure_bucket(&self) -> Result<(), MediaError> {
        let head = self.bucket.head_bucket(Some(&self.credentials)).sign(self.presign_ttl);
        let resp = self.http.head(head).send().await.map_err(reqwest_err)?;
        if resp.status().is_success() {
            return Ok(());
        }
        if resp.status() != StatusCode::NOT_FOUND {
            return Err(MediaError::ObjectStoreUnavailable);
        }
        let url = self.bucket.create_bucket(&self.credentials).sign(self.presign_ttl);
        let resp = self.http.put(url).send().await.map_err(reqwest_err)?;
        let status = resp.status();
        if status.is_success() || status == StatusCode::CONFLICT {
            Ok(())
        } else {
            Err(MediaError::ObjectStoreUnavailable)
        }
    }
}

fn reqwest_err(e: reqwest::Error) -> MediaError {
    if e.is_timeout() {
        MediaError::ObjectStoreTimeout
    } else {
        MediaError::ObjectStoreUnavailable
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn client() -> S3Client {
        S3Client::new(S3Config {
            // The fleet shape: pods reach the store in-network, clients don't.
            endpoint: "http://minio:9000".into(),
            public_endpoint: "http://localhost:9000".into(),
            region: "us-east-1".into(),
            bucket: "media".into(),
            access_key: "minioadmin".into(),
            secret_key: "minioadmin".into(),
            presign_ttl: Duration::from_secs(900),
            request_timeout: Duration::from_secs(10),
        })
        .expect("client")
    }

    #[test]
    fn client_facing_presign_signs_the_public_host() {
        let c = client();
        let ttl = Duration::from_secs(300);
        // The URLs handed to callers must resolve from the caller's network.
        assert_eq!(c.presign_put_public("staging/a", ttl).host_str(), Some("localhost"));
        assert_eq!(c.presign_get_public("staging/a", ttl).host_str(), Some("localhost"));
    }

    #[test]
    fn server_side_presign_signs_the_internal_host() {
        let c = client();
        let ttl = Duration::from_secs(300);
        // This service's own byte I/O stays on the in-network endpoint.
        assert_eq!(c.presign_put("staging/a", ttl).host_str(), Some("minio"));
        assert_eq!(c.presign_get("staging/a", ttl).host_str(), Some("minio"));
    }

    #[test]
    fn a_signed_url_still_carries_the_sigv4_query() {
        // Splitting the host must not drop the signature — the URL is still signed.
        let url = client().presign_put_public("staging/a", Duration::from_secs(300));
        assert!(url.query().unwrap_or_default().contains("X-Amz-Signature"));
    }
}
