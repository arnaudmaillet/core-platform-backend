use std::time::Duration;

use fred::prelude::{Builder, Config, ConnectionConfig, PerformanceConfig, ReconnectPolicy};
use fred::types::config::{TlsConfig, TlsConnector};

use crate::config::topology::TopologyKind;

/// Complete configuration surface for a Redis connection.
///
/// Construct via [`RedisConfig::from_env`] for production, or build the
/// struct directly in tests and local development.
///
/// ## Topology selection
///
/// Set `REDIS_TOPOLOGY` to `standalone` (default), `cluster`, or `sentinel`.
/// The corresponding fred `ServerConfig` variant is assembled automatically
/// when [`RedisConfig::into_fred_builder`] is called by the builders.
#[derive(Debug, Clone)]
pub struct RedisConfig {
    // ── Topology ──────────────────────────────────────────────────────────────

    /// Deployment topology — drives which fred `ServerConfig` variant is used.
    /// `REDIS_TOPOLOGY` (`standalone` | `cluster` | `sentinel`, default: `standalone`)
    pub topology: TopologyKind,

    /// Comma-separated `host:port` entries.
    ///
    /// - `Standalone`: only the first entry is used.
    /// - `Cluster`:    all entries are treated as cluster seed nodes.
    /// - `Sentinel`:   all entries are treated as Sentinel addresses.
    ///
    /// `REDIS_HOSTS` (default: `"127.0.0.1:6379"`)
    pub hosts: Vec<String>,

    // ── Authentication ────────────────────────────────────────────────────────

    /// ACL username for the data-plane connection.
    /// `REDIS_USERNAME` (optional)
    pub username: Option<String>,

    /// Password for the data-plane connection.
    /// `REDIS_PASSWORD` (optional)
    pub password: Option<String>,

    /// Enable TLS for the connection (rustls, system CA roots).
    ///
    /// Managed Redis (ElastiCache) enforces transit encryption — without this
    /// flag fred speaks plaintext to a TLS listener and every command times out
    /// with no useful error (found live on the staging bring-up).
    /// `REDIS_TLS` (`true`/`false`, default: `false`)
    pub tls: bool,

    /// Database index for standalone and sentinel deployments (0–15).
    /// When `0`, no `SELECT` is sent after connecting.
    /// Ignored in cluster mode — use key prefixes instead.
    /// `REDIS_DATABASE` (default: 0)
    pub database: u8,

    // ── Sentinel-specific ─────────────────────────────────────────────────────

    /// Logical name of the Sentinel-managed primary.
    /// `REDIS_SENTINEL_SERVICE_NAME` (default: `"mymaster"`)
    pub sentinel_service_name: Option<String>,

    // ── Connection tuning ─────────────────────────────────────────────────────

    /// Maximum time allowed for the initial TCP handshake with a Redis node.
    /// After this deadline, the attempt is abandoned and the reconnect policy
    /// takes over.
    /// `REDIS_CONNECTION_TIMEOUT_SECS` (default: 10.0)
    pub connection_timeout: Duration,

    /// Per-command response deadline. `Duration::ZERO` disables the timeout —
    /// commands may block indefinitely (not recommended in production).
    /// `REDIS_COMMAND_TIMEOUT_MS` (default: 3000)
    pub command_timeout: Duration,

    /// When `true`, connection failures during startup return an error
    /// immediately instead of entering the reconnect loop. Recommended for
    /// readiness-probe-gated deployments.
    /// `REDIS_FAIL_FAST` (default: `true`)
    pub fail_fast: bool,

    // ── Pool size (used by RedisPoolBuilder) ──────────────────────────────────

    /// Number of independent `Client` connections in the pool.
    /// `REDIS_POOL_SIZE` (default: 8)
    pub pool_size: usize,

    // ── Pipelining ────────────────────────────────────────────────────────────

    /// Maximum number of frames fed to a socket before flushing.
    ///
    /// fred always pipelines automatically — this controls the batch ceiling
    /// per flush cycle. Higher values improve throughput; lower values reduce
    /// per-command latency under light load.
    /// `REDIS_MAX_FEED_COUNT` (default: 200)
    pub max_feed_count: u64,

    /// Maximum number of commands that may be queued in the internal command
    /// buffer before `ErrorKind::Backpressure` is returned to the caller.
    /// `0` means unlimited.
    /// `REDIS_MAX_COMMAND_BUFFER_LEN` (default: 0)
    pub max_command_buffer_len: usize,

    // ── Reconnect policy (exponential backoff) ────────────────────────────────

    /// Minimum delay between reconnection attempts in milliseconds.
    /// `REDIS_RECONNECT_MIN_DELAY_MS` (default: 100)
    pub reconnect_min_delay_ms: u32,

    /// Maximum delay cap between reconnection attempts in milliseconds.
    /// `REDIS_RECONNECT_MAX_DELAY_MS` (default: 30_000)
    pub reconnect_max_delay_ms: u32,

    /// Maximum number of reconnection attempts before giving up.
    /// `0` means unlimited — the client will keep retrying indefinitely.
    /// `REDIS_RECONNECT_MAX_ATTEMPTS` (default: 0)
    pub reconnect_max_attempts: u32,

    /// Exponential multiplier applied to the backoff delay on each attempt.
    /// `REDIS_RECONNECT_MULTIPLIER` (default: 2)
    pub reconnect_multiplier: u32,

    // ── Cluster ───────────────────────────────────────────────────────────────

    /// Maximum number of MOVED/ASK redirections followed per command before
    /// returning an error.
    /// `REDIS_MAX_REDIRECTIONS` (default: 5)
    pub max_redirections: u32,

    // ── Unresponsive connection detection ─────────────────────────────────────

    /// If set, connections that receive no response for this duration are
    /// forcefully closed and reconnected. `None` disables the check.
    /// `REDIS_UNRESPONSIVE_TIMEOUT_MS` (optional)
    pub unresponsive_timeout: Option<Duration>,
}

impl Default for RedisConfig {
    fn default() -> Self {
        Self {
            topology: TopologyKind::Standalone,
            hosts: vec!["127.0.0.1:6379".to_string()],
            username: None,
            password: None,
            tls: false,
            database: 0,
            sentinel_service_name: None,
            connection_timeout: Duration::from_secs(10),
            command_timeout: Duration::from_millis(3_000),
            fail_fast: true,
            pool_size: 8,
            max_feed_count: 200,
            max_command_buffer_len: 0,
            reconnect_min_delay_ms: 100,
            reconnect_max_delay_ms: 30_000,
            reconnect_max_attempts: 0,
            reconnect_multiplier: 2,
            max_redirections: 5,
            unresponsive_timeout: None,
        }
    }
}

impl RedisConfig {
    /// Populates every field from environment variables, falling back to
    /// production-safe defaults when variables are absent.
    pub fn from_env() -> Self {
        Self::from_env_prefixed("")
    }

    /// [`Self::from_env`] with every variable name prefixed (`<prefix>REDIS_HOSTS`,
    /// `<prefix>REDIS_TOPOLOGY`, …): a second Redis the process reads beside its
    /// own, e.g. `EDGE_REVOCATION_` for `auth`'s revocation state (#884).
    pub fn from_env_prefixed(prefix: &str) -> Self {
        let var = |key: &str| std::env::var(format!("{prefix}{key}"));
        let parse = |key: &str| format!("{prefix}{key}");
        let topology = TopologyKind::from_env_prefixed(prefix);

        let hosts = var("REDIS_HOSTS")
            .unwrap_or_else(|_| "127.0.0.1:6379".to_string())
            .split(',')
            .map(|s| s.trim().to_string())
            .filter(|s| !s.is_empty())
            .collect::<Vec<_>>();

        assert!(!hosts.is_empty(), "{prefix}REDIS_HOSTS must not be empty");

        Self {
            topology,
            hosts,
            username:              var("REDIS_USERNAME").ok(),
            password:              var("REDIS_PASSWORD").ok(),
            tls:                   parse_env(&parse("REDIS_TLS"), false),
            database:              parse_env(&parse("REDIS_DATABASE"), 0u8),
            sentinel_service_name: var("REDIS_SENTINEL_SERVICE_NAME").ok(),
            connection_timeout:    Duration::from_secs_f64(
                parse_env(&parse("REDIS_CONNECTION_TIMEOUT_SECS"), 10.0f64),
            ),
            command_timeout:       Duration::from_millis(
                parse_env(&parse("REDIS_COMMAND_TIMEOUT_MS"), 3_000u64),
            ),
            fail_fast:             parse_env(&parse("REDIS_FAIL_FAST"), true),
            pool_size:             parse_env(&parse("REDIS_POOL_SIZE"), 8usize),
            max_feed_count:        parse_env(&parse("REDIS_MAX_FEED_COUNT"), 200u64),
            max_command_buffer_len: parse_env(&parse("REDIS_MAX_COMMAND_BUFFER_LEN"), 0usize),
            reconnect_min_delay_ms: parse_env(&parse("REDIS_RECONNECT_MIN_DELAY_MS"), 100u32),
            reconnect_max_delay_ms: parse_env(&parse("REDIS_RECONNECT_MAX_DELAY_MS"), 30_000u32),
            reconnect_max_attempts: parse_env(&parse("REDIS_RECONNECT_MAX_ATTEMPTS"), 0u32),
            reconnect_multiplier:   parse_env(&parse("REDIS_RECONNECT_MULTIPLIER"), 2u32),
            max_redirections:       parse_env(&parse("REDIS_MAX_REDIRECTIONS"), 5u32),
            unresponsive_timeout:   parse_env_opt::<u64>(&parse("REDIS_UNRESPONSIVE_TIMEOUT_MS"))
                .map(Duration::from_millis),
        }
    }

    /// Converts this crate-level configuration into a fred `Builder` with all
    /// topology, performance, connection, and reconnect settings applied.
    ///
    /// This is the single point where our topology abstraction is resolved into
    /// a concrete fred `ServerConfig` variant, and where performance/connection
    /// tuning is wired into the driver. Kept `pub(crate)` so callers interact
    /// only with our `RedisConfig`, not fred types directly.
    pub(crate) fn into_fred_builder(self) -> Result<Builder, fred::error::Error> {
        let server = self.topology.into_server_config(
            &self.hosts,
            self.sentinel_service_name.as_deref(),
        );

        // System CA roots via rustls-native-certs; failure to load them is an
        // environment defect worth failing the build() for, not hiding.
        let tls: Option<TlsConfig> = if self.tls {
            Some(TlsConnector::default_rustls()?.into())
        } else {
            None
        };

        let fred_config = Config {
            server,
            username:  self.username,
            password:  self.password,
            database:  if self.database > 0 { Some(self.database) } else { None },
            fail_fast: self.fail_fast,
            tls,
            ..Default::default()
        };

        let policy = ReconnectPolicy::new_exponential(
            self.reconnect_max_attempts,
            self.reconnect_min_delay_ms,
            self.reconnect_max_delay_ms,
            self.reconnect_multiplier,
        );

        let max_feed_count       = self.max_feed_count;
        let command_timeout      = self.command_timeout;
        let connection_timeout   = self.connection_timeout;
        let max_command_buffer_len = self.max_command_buffer_len;
        let max_redirections     = self.max_redirections;
        let unresponsive_timeout = self.unresponsive_timeout;

        let mut builder = Builder::from_config(fred_config);
        builder
            .set_policy(policy)
            .with_performance_config(|pc: &mut PerformanceConfig| {
                pc.default_command_timeout = command_timeout;
                pc.max_feed_count          = max_feed_count;
            })
            .with_connection_config(|cc: &mut ConnectionConfig| {
                cc.connection_timeout      = connection_timeout;
                cc.max_command_buffer_len  = max_command_buffer_len;
                cc.max_redirections        = max_redirections;
                cc.unresponsive.max_timeout = unresponsive_timeout;
            });

        Ok(builder)
    }
}

fn parse_env<T>(key: &str, default: T) -> T
where
    T: std::str::FromStr,
    T::Err: std::fmt::Debug,
{
    std::env::var(key)
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(default)
}

fn parse_env_opt<T>(key: &str) -> Option<T>
where
    T: std::str::FromStr,
    T::Err: std::fmt::Debug,
{
    std::env::var(key).ok().and_then(|v| v.parse().ok())
}
