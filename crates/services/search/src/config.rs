//! Environment-sourced configuration, resolved once at boot and threaded into the
//! composition root ([`crate::app::App::build`]). Kafka connection config is
//! resolved separately via `KafkaClientConfig::from_env`.

use crate::infrastructure::index::OpenSearchConfig;

/// Fully-resolved search configuration.
pub struct SearchConfig {
    pub opensearch: OpenSearchConfig,
    /// gRPC endpoint of the `post` service — the ingestion hydrator fetches the
    /// authoritative snapshot behind a thin `post.v1.events` notification.
    pub post_endpoint: String,
    /// gRPC endpoint of the `profile` service — same role for `profile.v1.events`.
    pub profile_endpoint: String,
    /// Per-request deadline on hydration RPCs. Hydration runs inside a Kafka
    /// consumer, so an unbounded call would stall the partition indefinitely.
    pub hydrate_rpc_timeout: std::time::Duration,
    /// Connect deadline when dialing the `post` / `profile` channels.
    pub hydrate_connect_timeout: std::time::Duration,
    /// gRPC endpoint of `social-graph` — the query path's audience check
    /// (`CheckAccess`) on the authors of a page of hits.
    pub social_graph_endpoint: String,
    /// Deadlines on that call; it runs inside a user's search request, so short.
    pub audience_rpc_timeout: std::time::Duration,
    pub audience_connect_timeout: std::time::Duration,
}

impl SearchConfig {
    pub fn from_env() -> Self {
        let mut opensearch = OpenSearchConfig::new(
            env_or("SEARCH_OPENSEARCH_URL", "http://localhost:9200"),
            env_or("SEARCH_INDEX_PREFIX", "search"),
        );
        if let Ok(raw) = std::env::var("SEARCH_QUERY_TIMEOUT_MS")
            && let Ok(ms) = raw.parse::<u64>()
        {
            opensearch = opensearch.with_request_timeout(std::time::Duration::from_millis(ms));
        }
        if let (Ok(user), Ok(pass)) = (
            std::env::var("SEARCH_OPENSEARCH_USER"),
            std::env::var("SEARCH_OPENSEARCH_PASSWORD"),
        ) {
            opensearch = opensearch.with_basic_auth(user, pass);
        }
        Self {
            opensearch,
            post_endpoint: env_or("SEARCH_POST_GRPC_ENDPOINT", "http://localhost:50056"),
            profile_endpoint: env_or("SEARCH_PROFILE_GRPC_ENDPOINT", "http://localhost:50052"),
            hydrate_rpc_timeout: env_ms("SEARCH_HYDRATE_RPC_TIMEOUT_MS", 5_000),
            hydrate_connect_timeout: env_ms("SEARCH_HYDRATE_CONNECT_TIMEOUT_MS", 2_000),
            social_graph_endpoint: env_or("SEARCH_SOCIAL_GRAPH_GRPC_ENDPOINT", "http://localhost:50053"),
            audience_rpc_timeout: env_ms("SEARCH_AUDIENCE_RPC_TIMEOUT_MS", 500),
            audience_connect_timeout: env_ms("SEARCH_AUDIENCE_CONNECT_TIMEOUT_MS", 500),
        }
    }
}

fn env_or(key: &str, default: impl Into<String>) -> String {
    std::env::var(key).unwrap_or_else(|_| default.into())
}

fn env_ms(key: &str, default: u64) -> std::time::Duration {
    std::time::Duration::from_millis(
        std::env::var(key).ok().and_then(|v| v.parse().ok()).unwrap_or(default),
    )
}
