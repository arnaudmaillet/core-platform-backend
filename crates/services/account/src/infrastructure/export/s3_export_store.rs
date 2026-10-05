//! The GDPR export archives' store (#653): a private S3 bucket with static
//! keys (like media — a 7-day presign needs non-session credentials). The
//! archive is PUT through a presigned URL; the holder's link is a presigned
//! GET against the public endpoint.

use std::time::Duration as StdDuration;

use async_trait::async_trait;
use chrono::Duration;
use reqwest::StatusCode;
use rusty_s3::{Bucket, Credentials, S3Action, UrlStyle};
use url::Url;

use crate::application::port::ExportStore;
use crate::error::AccountError;

/// Where the archives go (from `ACCOUNT_EXPORT_*`).
#[derive(Debug, Clone)]
pub struct ExportStoreConfig {
    pub endpoint: String,
    /// The endpoint the holder's link is signed against.
    pub public_endpoint: String,
    pub region: String,
    pub bucket: String,
    pub access_key: String,
    pub secret_key: String,
}

impl ExportStoreConfig {
    /// From the environment; `None` when `ACCOUNT_EXPORT_BUCKET` (or a key)
    /// is unset — the export pass then stays off.
    pub fn from_env() -> Option<Self> {
        let var = |name: &str| std::env::var(name).ok().map(|v| v.trim().to_owned()).filter(|v| !v.is_empty());
        let bucket = var("ACCOUNT_EXPORT_BUCKET")?;
        let endpoint = var("ACCOUNT_EXPORT_S3_ENDPOINT").unwrap_or_else(|| "https://s3.amazonaws.com".into());
        Some(Self {
            public_endpoint: var("ACCOUNT_EXPORT_S3_PUBLIC_ENDPOINT").unwrap_or_else(|| endpoint.clone()),
            endpoint,
            region: var("ACCOUNT_EXPORT_S3_REGION").unwrap_or_else(|| "us-east-1".into()),
            bucket,
            access_key: var("ACCOUNT_EXPORT_S3_ACCESS_KEY")?,
            secret_key: var("ACCOUNT_EXPORT_S3_SECRET_KEY")?,
        })
    }
}

pub struct S3ExportStore {
    bucket: Bucket,
    public_bucket: Bucket,
    credentials: Credentials,
    http: reqwest::Client,
}

fn unavailable(reason: impl ToString) -> AccountError {
    AccountError::DataExportUnavailable { reason: reason.to_string() }
}

/// How long the store's own PUT URL lives.
const PUT_TTL: StdDuration = StdDuration::from_secs(300);

impl S3ExportStore {
    pub fn new(config: ExportStoreConfig) -> Result<Self, AccountError> {
        let bucket_at = |endpoint: &str| {
            let url = Url::parse(endpoint).map_err(|e| unavailable(format!("invalid export endpoint: {e}")))?;
            Bucket::new(url, UrlStyle::Path, config.bucket.clone(), config.region.clone())
                .map_err(|e| unavailable(format!("invalid export bucket: {e}")))
        };
        Ok(Self {
            bucket: bucket_at(&config.endpoint)?,
            public_bucket: bucket_at(&config.public_endpoint)?,
            credentials: Credentials::new(config.access_key, config.secret_key),
            http: reqwest::Client::builder()
                .timeout(StdDuration::from_secs(60))
                .build()
                .map_err(|e| unavailable(format!("export HTTP client: {e}")))?,
        })
    }

    /// Creates the bucket when missing (local runs and tests; in the cloud the
    /// bucket is Terraform's).
    pub async fn ensure_bucket(&self) -> Result<(), AccountError> {
        let head = self.bucket.head_bucket(Some(&self.credentials)).sign(PUT_TTL);
        let resp = self.http.head(head).send().await.map_err(unavailable)?;
        if resp.status().is_success() {
            return Ok(());
        }
        let url = self.bucket.create_bucket(&self.credentials).sign(PUT_TTL);
        let status = self.http.put(url).send().await.map_err(unavailable)?.status();
        if status.is_success() || status == StatusCode::CONFLICT {
            Ok(())
        } else {
            Err(unavailable(format!("create bucket: {status}")))
        }
    }
}

#[async_trait]
impl ExportStore for S3ExportStore {
    async fn put(&self, key: &str, archive: Vec<u8>) -> Result<(), AccountError> {
        let url = self.bucket.put_object(Some(&self.credentials), key).sign(PUT_TTL);
        let status = self
            .http
            .put(url)
            .header(reqwest::header::CONTENT_TYPE, "application/zip")
            .body(archive)
            .send()
            .await
            .map_err(unavailable)?
            .status();
        if status.is_success() { Ok(()) } else { Err(unavailable(format!("store the archive: {status}"))) }
    }

    fn signed_link(&self, key: &str, ttl: Duration) -> Result<String, AccountError> {
        let ttl = ttl.clamp(Duration::seconds(60), Duration::days(7)).to_std().map_err(unavailable)?;
        Ok(self.public_bucket.get_object(Some(&self.credentials), key).sign(ttl).to_string())
    }
}
