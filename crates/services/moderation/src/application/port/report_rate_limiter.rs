use async_trait::async_trait;

use crate::error::ModerationError;

/// Caps how many reports one reporter files (per hour and per day). Keyed by an
/// opaque reporter key (`member:<id>` / `guest:<id>`).
#[async_trait]
pub trait ReportRateLimiter: Send + Sync + 'static {
    /// Counts one report and returns whether it is within the quota.
    async fn admit(&self, reporter_key: &str) -> Result<bool, ModerationError>;
}
