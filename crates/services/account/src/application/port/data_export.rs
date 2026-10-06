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

/// A conversation a profile is a member of (#653).
#[derive(Debug, Clone)]
pub struct ConversationExport {
    pub conversation_id: String,
    /// Its membership row (role, joined at), as the chat service gives it.
    pub membership: serde_json::Value,
    /// A direct (one-to-one) conversation by its kind, exported in full. Never
    /// inferred from the roster: a group that shrank to two still holds the
    /// departed members' words, and a two-member channel is still a channel.
    pub direct: bool,
    /// The profile left it (#656): its own messages up to the departure.
    pub left: bool,
}

/// One message of a conversation: who sent it, when, and the whole of it.
#[derive(Debug, Clone)]
pub struct MessageExport {
    pub sender_id: String,
    pub created_at_ms: i64,
    pub message: serde_json::Value,
}

/// What each other service holds of an account and its profiles, read over
/// the mesh (every page; JSON as the service shapes it). Any failure fails
/// the whole export, retried next pass.
#[async_trait]
pub trait ExportPeers: Send + Sync + 'static {
    /// The account's profiles: `(profile_id, profile)`.
    async fn profiles(&self, account_id: &AccountId) -> Result<Vec<(String, serde_json::Value)>, AccountError>;
    async fn posts(&self, profile_id: &str) -> Result<Vec<serde_json::Value>, AccountError>;
    async fn comments(&self, profile_id: &str) -> Result<Vec<serde_json::Value>, AccountError>;
    async fn reactions(&self, profile_id: &str) -> Result<Vec<serde_json::Value>, AccountError>;
    /// The profile's recent searches, newest first (#816; search keeps 50 for 90 days).
    async fn recent_searches(&self, profile_id: &str) -> Result<Vec<serde_json::Value>, AccountError>;
    /// `{following, followers, blocks}`.
    async fn social(&self, profile_id: &str) -> Result<serde_json::Value, AccountError>;
    async fn conversations(&self, profile_id: &str) -> Result<Vec<ConversationExport>, AccountError>;
    /// The member profile ids of a conversation `as_member` is still in.
    async fn members(&self, conversation_id: &str, as_member: &str) -> Result<Vec<String>, AccountError>;
    /// Its history as `as_member` sees it for the export: a direct
    /// conversation whole; elsewhere `as_member`'s own messages, the others'
    /// reduced to their time by chat — up to the departure, if it left.
    async fn messages(&self, conversation: &ConversationExport, as_member: &str) -> Result<Vec<MessageExport>, AccountError>;
    /// The account's media, each with a download link valid `ttl`.
    async fn media(&self, account_id: &AccountId, ttl: Duration) -> Result<Vec<serde_json::Value>, AccountError>;
}
