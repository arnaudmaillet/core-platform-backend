use fred::prelude::ServerConfig;

/// Deployment topology of the target Redis infrastructure.
///
/// Controls which `ServerConfig` variant is handed to the fred `Builder`.
/// Selected at runtime via `REDIS_TOPOLOGY` — no code change is required to
/// switch between standalone, cluster, and sentinel deployments.
///
/// ## Environment variable
///
/// `REDIS_TOPOLOGY` accepts `standalone` | `cluster` | `sentinel`.
/// Defaults to [`TopologyKind::Standalone`] when absent or unrecognised.
#[derive(Debug, Clone, Copy, Default)]
pub enum TopologyKind {
    /// Single Redis node (or a read-replica pair accessed via a single VIP).
    ///
    /// Uses the first entry in `REDIS_HOSTS` only. Suitable for local
    /// development and small-scale deployments where cluster is not required.
    #[default]
    Standalone,

    /// Redis Cluster with automatic hash-slot–aware routing and MOVED/ASK
    /// redirect handling via multi-master topology discovery.
    ///
    /// All entries in `REDIS_HOSTS` are used as seed nodes. Production default
    /// for hyperscale, horizontally-sharded workloads.
    Cluster,

    /// Redis Sentinel for automatic primary promotion and client-transparent
    /// failover.
    ///
    /// All entries in `REDIS_HOSTS` are treated as Sentinel node addresses.
    /// Suitable when Redis Cluster is not available but high-availability is
    /// required.
    Sentinel,
}

impl TopologyKind {
    /// Parses `REDIS_TOPOLOGY` (`standalone` | `cluster` | `sentinel`).
    ///
    /// Defaults to [`TopologyKind::Standalone`] when the variable is absent
    /// or contains an unrecognised value.
    pub fn from_env() -> Self {
        Self::from_env_prefixed("")
    }

    /// [`Self::from_env`] reading `<prefix>REDIS_TOPOLOGY`.
    pub fn from_env_prefixed(prefix: &str) -> Self {
        match std::env::var(format!("{prefix}REDIS_TOPOLOGY")).as_deref() {
            Ok("cluster")  => TopologyKind::Cluster,
            Ok("sentinel") => TopologyKind::Sentinel,
            _              => TopologyKind::Standalone,
        }
    }

    /// Converts the topology kind and the provided host/service parameters into
    /// the fred `ServerConfig` variant required by `Config.server`.
    ///
    /// ## Parameters
    ///
    /// - `hosts` — `"host:port"` strings. For `Standalone` only the first
    ///   entry is used. For `Cluster` and `Sentinel` all entries are used as
    ///   seed/sentinel addresses. Falls back to sensible defaults when empty.
    /// - `sentinel_service_name` — logical name of the Sentinel-managed primary
    ///   (default: `"mymaster"`). Ignored for other topologies.
    pub(crate) fn into_server_config(
        self,
        hosts: &[String],
        sentinel_service_name: Option<&str>,
    ) -> ServerConfig {
        match self {
            TopologyKind::Standalone => {
                let addr = hosts.first().map(String::as_str).unwrap_or("127.0.0.1:6379");
                let (host, port) = parse_addr(addr);
                ServerConfig::new_centralized(host, port)
            }

            TopologyKind::Cluster => {
                let seed_nodes: Vec<(String, u16)> = if hosts.is_empty() {
                    vec![("127.0.0.1".to_string(), 6379)]
                } else {
                    hosts.iter().map(|h| parse_addr(h.as_str())).collect()
                };
                ServerConfig::new_clustered(seed_nodes)
            }

            TopologyKind::Sentinel => {
                let sentinel_nodes: Vec<(String, u16)> = if hosts.is_empty() {
                    vec![("127.0.0.1".to_string(), 26379)]
                } else {
                    hosts.iter().map(|h| parse_addr(h.as_str())).collect()
                };
                let service = sentinel_service_name.unwrap_or("mymaster");
                ServerConfig::new_sentinel(sentinel_nodes, service)
            }
        }
    }
}

/// Parses a `"host:port"` string into `(host, port)`.
///
/// When no port suffix is present, the Redis default port `6379` is assumed.
fn parse_addr(addr: &str) -> (String, u16) {
    let (host, port_str) = addr.rsplit_once(':').unwrap_or((addr, "6379"));
    let port: u16 = port_str.parse().unwrap_or(6379);
    (host.to_string(), port)
}

