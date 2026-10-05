//! Environment-sourced configuration, resolved once at boot and threaded into the
//! composition root ([`crate::app::App::build`]). The Postgres / Redis / Kafka
//! backend configs are resolved separately (via their own `from_env`) in
//! [`crate::service`], mirroring the rest of the fleet.

use std::time::Duration as StdDuration;

use chrono::Duration;

use crate::application::MediaPolicy;
use crate::infrastructure::store::S3Config;

/// Fully-resolved media configuration (the media-specific knobs; the shared
/// storage backends are built in `service`).
pub struct MediaConfig {
    pub s3: S3Config,
    /// CDN base URL for public, content-addressed delivery.
    pub cdn_base_url: String,
    /// `MEDIA_CLOUDFRONT_DISTRIBUTION_ID`: takedowns purge this distribution
    /// (signed with the S3 keys, whose IAM user needs
    /// `cloudfront:CreateInvalidation`). Unset: takedowns only log.
    pub cloudfront_distribution_id: Option<String>,
    /// gRPC endpoint of `moderation` (the pre-publish Screen gate).
    pub screen_endpoint: String,
    pub policy: MediaPolicy,
}

impl MediaConfig {
    pub fn from_env() -> Self {
        let policy = MediaPolicy {
            upload_ticket_ttl: Duration::seconds(env_u64("MEDIA_UPLOAD_TICKET_TTL_SECS", 900) as i64),
            signed_url_ttl: Duration::seconds(env_u64("MEDIA_SIGNED_URL_TTL_SECS", 300) as i64),
            dedup_enabled: env_bool("MEDIA_DEDUP_ENABLED", false),
            screen_timeout: StdDuration::from_millis(env_u64("MEDIA_SCREEN_TIMEOUT_MS", 200)),
        };
        let endpoint = env_or("MEDIA_OBJECT_STORE_ENDPOINT", "http://localhost:9000");
        let s3 = S3Config {
            // Client-facing presign host; defaults to the internal endpoint (prod,
            // where both are the public S3/CDN host). The local fleet overrides it
            // to a device-reachable host so uploads/delivery URLs actually resolve.
            public_endpoint: env_or("MEDIA_OBJECT_STORE_PUBLIC_ENDPOINT", endpoint.clone()),
            endpoint,
            region: env_or("MEDIA_OBJECT_STORE_REGION", "us-east-1"),
            bucket: env_or("MEDIA_OBJECT_STORE_BUCKET", "media"),
            access_key: env_or("MEDIA_S3_ACCESS_KEY", "minioadmin"),
            secret_key: env_or("MEDIA_S3_SECRET_KEY", "minioadmin"),
            presign_ttl: StdDuration::from_secs(env_u64("MEDIA_PRESIGN_TTL_SECS", 900)),
            request_timeout: StdDuration::from_millis(env_u64("MEDIA_OBJECT_STORE_TIMEOUT_MS", 10_000)),
        };
        Self {
            s3,
            cdn_base_url: env_or("MEDIA_CDN_BASE_URL", "http://localhost:9000/media"),
            cloudfront_distribution_id: std::env::var("MEDIA_CLOUDFRONT_DISTRIBUTION_ID")
                .ok()
                .map(|id| id.trim().to_owned())
                .filter(|id| !id.is_empty()),
            screen_endpoint: env_or("MEDIA_SCREEN_GRPC_ENDPOINT", "http://localhost:50061"),
            policy,
        }
    }
}

fn env_or(key: &str, default: impl Into<String>) -> String {
    std::env::var(key).unwrap_or_else(|_| default.into())
}

fn env_u64(key: &str, default: u64) -> u64 {
    std::env::var(key).ok().and_then(|v| v.parse().ok()).unwrap_or(default)
}

fn env_bool(key: &str, default: bool) -> bool {
    std::env::var(key)
        .ok()
        .map(|v| matches!(v.to_ascii_lowercase().as_str(), "1" | "true" | "yes" | "on"))
        .unwrap_or(default)
}
