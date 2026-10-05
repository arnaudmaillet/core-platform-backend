//! The GDPR data export's ports (#653, Art. 15/20): where the holder's data
//! comes from (the other services, over the mesh) and where the archive goes
//! (a private bucket, behind a signed link).

use async_trait::async_trait;
use chrono::{DateTime, Duration, Utc};

use crate::domain::value_object::AccountId;
use crate::error::AccountError;

/// One file of the archive: a path inside the ZIP and its content.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ExportFile {
    pub path: String,
    pub content: Vec<u8>,
}

impl ExportFile {
    /// A pretty-printed JSON file.
    pub fn json(path: impl Into<String>, value: &serde_json::Value) -> Self {
        Self { path: path.into(), content: serde_json::to_vec_pretty(value).unwrap_or_default() }
    }
}

/// Everything the other services hold about an account and its profiles:
/// profiles, posts, comments, reactions, the social graph, conversations and
/// messages, media (signed links). A section a service cannot answer for now
/// fails the whole export, which the next pass retries — a partial archive is
/// never delivered.
#[async_trait]
pub trait ExportSources: Send + Sync + 'static {
    async fn gather(&self, account_id: &AccountId, now: DateTime<Utc>) -> Result<Vec<ExportFile>, AccountError>;
}

/// The private store the archives go to.
#[async_trait]
pub trait ExportStore: Send + Sync + 'static {
    /// Stores the archive under `key`.
    async fn put(&self, key: &str, archive: Vec<u8>) -> Result<(), AccountError>;

    /// A signed download link for `key`, valid `ttl` (at most 7 days).
    fn signed_link(&self, key: &str, ttl: Duration) -> Result<String, AccountError>;
}
