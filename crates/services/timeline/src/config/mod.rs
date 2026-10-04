/// Runtime configuration for the timeline service.
///
/// All fields are loaded from environment variables at startup with
/// safe production defaults. No hot-reload — changing caps or TTLs
/// requires a service restart to prevent mid-flight divergence between
/// Redis state and the configured limits.
#[derive(Debug, Clone)]
pub struct TimelineConfig {
    /// Maximum number of post references stored in a user's Redis feed ZSET.
    /// Entries exceeding this cap are pruned oldest-first via Lua after each ZADD.
    pub feed_cap: u16,

    /// Maximum number of posts stored in an audio track's Redis ZSET.
    /// Entries exceeding this cap are pruned oldest-first after each ZADD.
    pub audio_feed_cap: u16,

    /// Maximum number of posts stored in a VIP author's Redis registry ZSET.
    /// Capped separately from regular feeds because VIP content is merged at
    /// read-time and only the most recent window is relevant.
    pub vip_registry_cap: u16,

    /// Number of recent posts to inject when a new follow.created event arrives
    /// for a Standard/Premium author. This bounds the backfill latency per event.
    pub backfill_limit: i32,

    /// TTL for the timeline:warm:{profile_id} Redis flag in seconds.
    /// After this duration the warm flag expires and the next feed request
    /// triggers a cold-start rebuild from ScyllaDB.
    pub warm_ttl_secs: u64,

    /// TTL for the timeline:tier:{author_id} Redis cache in seconds.
    /// Author tier changes are infrequent; 1 hour is safe for fan-out routing.
    pub tier_cache_ttl_secs: u64,

    /// TTL for the timeline:vip:{author_id} Redis ZSET in seconds.
    /// VIP posts older than this window are not merged at read-time.
    pub vip_registry_ttl_secs: u64,

    /// Maximum number of feed items returned per GetFollowingFeed page.
    pub max_page_size: i32,

    /// Maximum number of VIP followees merged per feed read.
    /// Prevents excessive Redis pipeline size for power users following many VIPs.
    pub max_vip_merge_sources: usize,

    /// Maximum number of concurrent background feed warm-ups (cold-start rebuilds
    /// from ScyllaDB). Caps the load a cold-cache stampede can place on ScyllaDB:
    /// requests beyond this bound return cold data and skip warming, which a later
    /// request retries once a permit frees.
    pub warm_max_concurrency: usize,

    /// Page size used when paginating the social-graph gRPC ListFollowers /
    /// ListFollowing RPCs during fan-out and cold-start rebuilds.
    pub social_graph_page_size: i32,

    /// gRPC endpoint for the social-graph service.
    /// Format: "http://host:port" (no trailing slash).
    pub social_graph_endpoint: String,

    /// How long a published post stays in the discovery pool, in seconds.
    pub discovery_window_secs: u64,

    /// Most posts the discovery pool holds (oldest evicted first).
    pub discovery_pool_cap: usize,

    /// Hot-score gravity, in seconds: a post this much newer ranks like one ten
    /// times more popular.
    pub discovery_hot_gravity_secs: f64,

    /// NEARBY: radius of the wide ring (popular posts) and of the close ring
    /// (any post), in km, and how many candidates are ranked per request.
    pub nearby_radius_km:       f64,
    pub nearby_close_radius_km: f64,
    pub nearby_candidates:      usize,

    /// gRPC endpoint for the geo-discovery service (NEARBY candidates).
    pub geo_discovery_endpoint: String,

    /// Kafka consumer group ID for the discovery pool worker (`post.v1.events`,
    /// `moderation.v1.events`, `counter.v1.popularity`).
    pub kafka_group_discovery: String,

    /// Kafka consumer group ID for the post-published worker (consumes the unified
    /// `post.v1.events` stream).
    pub kafka_group_post_published: String,

    /// Kafka consumer group ID for the post.deleted worker.
    pub kafka_group_post_deleted: String,

    /// Kafka consumer group ID for the social-graph.followed worker.
    pub kafka_group_sg_followed: String,

    /// Kafka consumer group ID for the social-graph.unfollowed worker.
    pub kafka_group_sg_unfollowed: String,
}

impl TimelineConfig {
    pub fn from_env() -> Self {
        Self {
            feed_cap:                   env_u16("TIMELINE_FEED_CAP",                   500),
            audio_feed_cap:             env_u16("TIMELINE_AUDIO_FEED_CAP",             1_000),
            vip_registry_cap:           env_u16("TIMELINE_VIP_REGISTRY_CAP",           200),
            backfill_limit:             env_i32("TIMELINE_BACKFILL_LIMIT",             100),
            warm_ttl_secs:              env_u64("TIMELINE_WARM_TTL_SECS",             86_400),
            tier_cache_ttl_secs:        env_u64("TIMELINE_TIER_CACHE_TTL_SECS",       3_600),
            vip_registry_ttl_secs:      env_u64("TIMELINE_VIP_REGISTRY_TTL_SECS",    604_800),
            max_page_size:              env_i32("TIMELINE_MAX_PAGE_SIZE",              50),
            max_vip_merge_sources:      env_usize("TIMELINE_MAX_VIP_MERGE_SOURCES",   50),
            warm_max_concurrency:       env_usize("TIMELINE_WARM_MAX_CONCURRENCY",   64),
            social_graph_page_size:     env_i32("TIMELINE_SOCIAL_GRAPH_PAGE_SIZE",    500),
            social_graph_endpoint:      env_str(
                "TIMELINE_SOCIAL_GRAPH_ENDPOINT",
                "http://social-graph:50051",
            ),
            discovery_window_secs:      env_u64("TIMELINE_DISCOVERY_WINDOW_SECS",     259_200),
            discovery_pool_cap:         env_usize("TIMELINE_DISCOVERY_POOL_CAP",      10_000),
            discovery_hot_gravity_secs: env_f64("TIMELINE_DISCOVERY_HOT_GRAVITY_SECS", 45_000.0),
            nearby_radius_km:           env_f64("TIMELINE_NEARBY_RADIUS_KM",          25.0),
            nearby_close_radius_km:     env_f64("TIMELINE_NEARBY_CLOSE_RADIUS_KM",    2.0),
            nearby_candidates:          env_usize("TIMELINE_NEARBY_CANDIDATES",       300),
            geo_discovery_endpoint:     env_str(
                "TIMELINE_GEO_DISCOVERY_ENDPOINT",
                "http://localhost:50054",
            ),
            kafka_group_discovery:      env_str(
                "TIMELINE_KAFKA_GROUP_DISCOVERY",
                "timeline-discovery",
            ),
            kafka_group_post_published: env_str(
                "TIMELINE_KAFKA_GROUP_POST_PUBLISHED",
                "timeline-post-published",
            ),
            kafka_group_post_deleted: env_str(
                "TIMELINE_KAFKA_GROUP_POST_DELETED",
                "timeline-post-deleted",
            ),
            kafka_group_sg_followed: env_str(
                "TIMELINE_KAFKA_GROUP_SG_FOLLOWED",
                "timeline-sg-followed",
            ),
            kafka_group_sg_unfollowed: env_str(
                "TIMELINE_KAFKA_GROUP_SG_UNFOLLOWED",
                "timeline-sg-unfollowed",
            ),
        }
    }
}

fn env_u16(var: &str, default: u16) -> u16 {
    std::env::var(var).ok().and_then(|v| v.parse().ok()).unwrap_or(default)
}

fn env_u64(var: &str, default: u64) -> u64 {
    std::env::var(var).ok().and_then(|v| v.parse().ok()).unwrap_or(default)
}

fn env_i32(var: &str, default: i32) -> i32 {
    std::env::var(var).ok().and_then(|v| v.parse().ok()).unwrap_or(default)
}

fn env_usize(var: &str, default: usize) -> usize {
    std::env::var(var).ok().and_then(|v| v.parse().ok()).unwrap_or(default)
}

fn env_f64(var: &str, default: f64) -> f64 {
    std::env::var(var).ok().and_then(|v| v.parse().ok()).filter(|v: &f64| v.is_finite() && *v > 0.0).unwrap_or(default)
}

fn env_str(var: &str, default: &str) -> String {
    std::env::var(var).unwrap_or_else(|_| default.to_owned())
}
