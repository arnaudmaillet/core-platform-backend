use async_trait::async_trait;
use chrono::{DateTime, Utc};

use crate::domain::value_object::AccountId;
use crate::error::AuthError;

/// What an account let a third-party app do (#667), as stored.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AppAuthorization {
    pub app_id:       String,
    pub display_name: String,
    /// https URL; empty when the app has no icon.
    pub icon_url:     String,
    /// Sorted, deduplicated.
    pub scopes:       Vec<String>,
    pub granted_at:   DateTime<Utc>,
    pub last_used_at: Option<DateTime<Utc>>,
}

/// An account's app authorisations (Postgres, on the account's shard).
#[async_trait]
pub trait AppAuthorizationRepository: Send + Sync + 'static {
    /// The active ones (not revoked), most recently granted first.
    async fn list(&self, account_id: &AccountId) -> Result<Vec<AppAuthorization>, AuthError>;

    /// Grants `authorization`, and returns the grant as stored. Granting again
    /// the same scopes to an active app changes nothing (its `granted_at`
    /// stays: a retry is the same consent); other scopes, or a revoked app,
    /// make a new consent (`granted_at` = the given one, `revoked_at` cleared).
    async fn grant(&self, account_id: &AccountId, authorization: &AppAuthorization) -> Result<AppAuthorization, AuthError>;

    /// Revokes the app's grant at `at`, and returns when it was revoked: `at`,
    /// or the earlier instant if it already was (idempotent). `None` when the
    /// account never authorised the app.
    async fn revoke(&self, account_id: &AccountId, app_id: &str, at: DateTime<Utc>) -> Result<Option<DateTime<Utc>>, AuthError>;

    /// The app used its grant: `false` when it has no active one.
    async fn record_use(&self, account_id: &AccountId, app_id: &str, at: DateTime<Utc>) -> Result<bool, AuthError>;

    /// Whether the app holds an active grant — what a token issued under it
    /// must check before it is honoured.
    async fn is_active(&self, account_id: &AccountId, app_id: &str) -> Result<bool, AuthError>;
}
