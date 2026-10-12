//! The delivery-plane port: turn a content-addressed [`StorageKey`] into a CDN URL,
//! and invalidate the edge on a takedown. The concrete adapter (CloudFront/Fastly
//! signer, Phase 4) lives in `infrastructure`. Bytes never cross this trait.

use async_trait::async_trait;
use chrono::{DateTime, Utc};

use crate::domain::value_object::{DeliveryVisibility, StorageKey};
use crate::error::MediaError;

/// A resolved delivery URL. `expires_at` is set for signed (private) URLs and
/// `None` for public, content-addressed-immutable ones.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ResolvedUrl {
    pub url: String,
    pub expires_at: Option<DateTime<Utc>>,
}

/// Cookies granting a CDN path prefix until `expires_at` (#921: how a SIGNED
/// video's whole HLS tree plays). The player sends them to `domain`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SignedCookies {
    /// `(name, value)` pairs.
    pub cookies:    Vec<(String, String)>,
    pub domain:     String,
    pub path:       String,
    pub expires_at: DateTime<Utc>,
}

#[async_trait]
pub trait CdnGateway: Send + Sync + 'static {
    /// Resolves a delivery URL for `key` under the given visibility. Public ⇒ a
    /// stable immutable URL; Signed ⇒ a short-lived signed URL minted at `now`.
    async fn resolve(
        &self,
        key: &StorageKey,
        visibility: DeliveryVisibility,
        now: DateTime<Utc>,
    ) -> Result<ResolvedUrl, MediaError>;

    /// A signed download URL for `key` valid `ttl` from `now` — longer-lived
    /// than delivery's, for a GDPR export's archive (#653; S3 caps it at 7
    /// days).
    async fn signed_download(
        &self,
        key: &StorageKey,
        ttl: chrono::Duration,
        now: DateTime<Utc>,
    ) -> Result<ResolvedUrl, MediaError>;

    /// Invalidates edge caches for these keys — the takedown path (delete /
    /// quarantine) and an asset going SIGNED (#919: its objects left their public
    /// paths). Content-addressed immutability means this never fires on edit.
    async fn invalidate(&self, keys: &[StorageKey]) -> Result<(), MediaError>;

    /// Signed cookies (#921) granting every CDN object under `prefix` (an
    /// asset's tree, trailing `/`) from `now`; `None` when no signer is
    /// configured.
    async fn signed_cookies(&self, prefix: &str, now: DateTime<Utc>) -> Result<Option<SignedCookies>, MediaError>;
}
