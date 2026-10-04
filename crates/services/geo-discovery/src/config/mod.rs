use std::time::Duration;

/// Runtime configuration for the geo-discovery service.
///
/// All values are sourced from environment variables at startup.
/// No field carries a default that would silently mask a misconfiguration in
/// production — callers must set every required variable explicitly.
pub struct GeoDiscoveryConfig {
    /// gRPC listen address (e.g. "0.0.0.0:50054").
    pub grpc_addr: String,

    /// Minimum virality score a post must have before its card is written to
    /// the Redis card cache. Posts below this threshold are served exclusively
    /// from ScyllaDB on cache miss. Set to 0.0 to cache all posts.
    ///
    /// Default: 10.0. Tune upward under high RAM pressure.
    pub card_cache_threshold: f64,

    /// Default post retention duration when `retention_secs` is absent from
    /// the Kafka event. Matches the ScyllaDB `default_time_to_live`.
    ///
    /// Default: 172 800 s (48 h).
    pub default_retention: Duration,

    /// How often the `TilePrunerWorker` wakes up to evict cold tile ZSETs.
    ///
    /// Default: 60 s.
    pub tile_pruner_interval: Duration,

    /// A tile ZSET is evicted if it has not been queried within this window.
    ///
    /// Default: 1 800 s (30 min).
    pub tile_cold_threshold: Duration,

    /// Maximum number of ZSET members retained per tile per resolution.
    /// Members below the score floor are removed atomically after each ZADD.
    ///
    /// Resolution 5 cap: 200, Resolution 7 cap: 500, Resolution 9 cap: 1 000.
    /// These are baked into `H3Resolution` and not overridable at runtime
    /// to keep the hot-path Lua script argument list stable.

    /// Kafka consumer group ID for `post.published`.
    pub post_indexer_group_id: String,

    /// Kafka consumer group ID for `counter.v1.popularity`.
    pub score_updater_group_id: String,

    /// Kafka consumer group ID for `post.deleted` + `moderation.v1.events`.
    pub visibility_group_id: String,

    /// How long a country granted from location stays open without being
    /// confirmed again (the app re-sends at each foreground).
    ///
    /// Default: 43 200 s (12 h).
    pub country_grant_ttl_secs: u64,

    /// Proxies in front of the edge listener that append to `X-Forwarded-For`
    /// (the ALB: 1). The client address is that many entries from the right;
    /// anything further left is client-supplied and never trusted.
    ///
    /// `GEO_TRUSTED_PROXY_HOPS`, else `GRPC_TRUSTED_PROXY_HOPS` (the runtime's, also used
    /// by `per_ip` rate limits — keep one value for the fleet), else 1.
    pub trusted_proxy_hops: usize,
}

impl GeoDiscoveryConfig {
    pub fn from_env() -> Self {
        Self {
            grpc_addr: std::env::var("GEO_GRPC_ADDR")
                .unwrap_or_else(|_| "0.0.0.0:50054".to_owned()),

            card_cache_threshold: std::env::var("GEO_CARD_CACHE_THRESHOLD")
                .ok()
                .and_then(|v| v.parse().ok())
                .unwrap_or(10.0),

            default_retention: Duration::from_secs(
                std::env::var("GEO_DEFAULT_RETENTION_SECS")
                    .ok()
                    .and_then(|v| v.parse().ok())
                    .unwrap_or(172_800),
            ),

            tile_pruner_interval: Duration::from_secs(
                std::env::var("GEO_TILE_PRUNER_INTERVAL_SECS")
                    .ok()
                    .and_then(|v| v.parse().ok())
                    .unwrap_or(60),
            ),

            tile_cold_threshold: Duration::from_secs(
                std::env::var("GEO_TILE_COLD_THRESHOLD_SECS")
                    .ok()
                    .and_then(|v| v.parse().ok())
                    .unwrap_or(1_800),
            ),

            post_indexer_group_id: std::env::var("GEO_POST_INDEXER_GROUP_ID")
                .unwrap_or_else(|_| "geo-discovery-post-indexer".to_owned()),

            score_updater_group_id: std::env::var("GEO_SCORE_UPDATER_GROUP_ID")
                .unwrap_or_else(|_| "geo-discovery-score-updater".to_owned()),

            visibility_group_id: std::env::var("GEO_VISIBILITY_GROUP_ID")
                .unwrap_or_else(|_| "geo-discovery-visibility".to_owned()),

            country_grant_ttl_secs: std::env::var("GEO_COUNTRY_GRANT_TTL_SECS")
                .ok()
                .and_then(|v| v.parse().ok())
                .unwrap_or(43_200),

            // One setting for the fleet: GEO_ overrides, else the runtime's
            // GRPC_TRUSTED_PROXY_HOPS (also used by per_ip rate limits), else 1.
            trusted_proxy_hops: ["GEO_TRUSTED_PROXY_HOPS", "GRPC_TRUSTED_PROXY_HOPS"]
                .iter()
                .find_map(|key| std::env::var(key).ok().and_then(|v| v.parse().ok()))
                .unwrap_or(1),
        }
    }
}
