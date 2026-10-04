//! Environment-sourced configuration, resolved once at boot and threaded into the
//! composition root ([`crate::app`]). Each backend's connection config comes from
//! its own `from_env`; realtime-specific knobs are read here.

use std::time::Duration;

use auth_context::AuthContextConfig;
use redis_storage::RedisConfig;
use transport::kafka::config::KafkaClientConfig;

const DEFAULT_WS_ADDR: &str = "0.0.0.0:8443";
const DEFAULT_HEARTBEAT_INTERVAL_MS: u64 = 30_000;
const DEFAULT_HEARTBEAT_TIMEOUT_MS: u64 = 90_000;
const DEFAULT_SEND_QUEUE_CAP: usize = 256;
const DEFAULT_MAX_SUBSCRIPTIONS: usize = 256;
const DEFAULT_REGISTRY_TTL_MS: u64 = 120_000;
/// Minted by `auth` from the session's bound `DeviceContext.device_id`.
const DEFAULT_DEVICE_CLAIM: &str = auth_context::edge::EDGE_DEVICE_CLAIM;

/// Fully-resolved realtime configuration shared by both binaries (the gateway uses
/// the WS/auth/heartbeat knobs; the dispatcher uses Kafka + the routing fabric).
pub struct RealtimeConfig {
    pub redis: RedisConfig,
    pub kafka: KafkaClientConfig,
    pub auth: AuthContextConfig,
    /// This node's identity — the key the registry/node-hop address it by.
    pub node_id: String,
    /// Public WSS listen address for clients (gateway).
    pub ws_addr: String,
    /// App-level ping cadence (keeps NAT bindings, reaps half-open).
    pub heartbeat_interval: Duration,
    /// Deadline after which a connection with no inbound traffic is reaped.
    pub heartbeat_timeout: Duration,
    /// Per-connection outbound queue depth before shedding.
    pub send_queue_cap: usize,
    /// Per-connection channel-subscription cap.
    pub subscription_cap: usize,
    /// TTL applied to a connection's presence-registry entry (self-heal bound).
    pub registry_ttl: Duration,
    /// JWT claim carrying the device/session id on the edge token.
    pub device_claim: String,
}

impl RealtimeConfig {
    pub fn from_env() -> Self {
        Self {
            redis: RedisConfig::from_env(),
            kafka: KafkaClientConfig::from_env(),
            auth: auth_config_from_env(),
            node_id: std::env::var("REALTIME_NODE_ID").unwrap_or_else(|_| default_node_id()),
            ws_addr: std::env::var("REALTIME_GATEWAY_WS_ADDR")
                .unwrap_or_else(|_| DEFAULT_WS_ADDR.to_owned()),
            heartbeat_interval: Duration::from_millis(env_u64(
                "REALTIME_HEARTBEAT_INTERVAL_MS",
                DEFAULT_HEARTBEAT_INTERVAL_MS,
            )),
            heartbeat_timeout: Duration::from_millis(env_u64(
                "REALTIME_HEARTBEAT_TIMEOUT_MS",
                DEFAULT_HEARTBEAT_TIMEOUT_MS,
            )),
            send_queue_cap: env_usize("REALTIME_SEND_QUEUE_CAP", DEFAULT_SEND_QUEUE_CAP),
            subscription_cap: env_usize("REALTIME_MAX_SUBSCRIPTIONS", DEFAULT_MAX_SUBSCRIPTIONS),
            registry_ttl: Duration::from_millis(env_u64(
                "REALTIME_REGISTRY_TTL_MS",
                DEFAULT_REGISTRY_TTL_MS,
            )),
            device_claim: std::env::var("REALTIME_DEVICE_CLAIM")
                .unwrap_or_else(|_| DEFAULT_DEVICE_CLAIM.to_owned()),
        }
    }
}

/// Build the auth-context config from realtime's env surface (JWKS + expected
/// claims). The edge token is ES256; the decoder is built with EC algorithms in
/// the composition root.
fn auth_config_from_env() -> AuthContextConfig {
    let mut cfg = AuthContextConfig {
        jwks_url: std::env::var("REALTIME_JWKS_URL").unwrap_or_default(),
        ..AuthContextConfig::default()
    };
    cfg.expected_audience = std::env::var("REALTIME_TOKEN_AUDIENCE").ok();
    cfg.expected_issuer = std::env::var("REALTIME_TOKEN_ISSUER").ok();
    cfg
}

fn default_node_id() -> String {
    std::env::var("HOSTNAME").unwrap_or_else(|_| "realtime-gateway-0".to_owned())
}

fn env_u64(key: &str, default: u64) -> u64 {
    std::env::var(key)
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(default)
}

fn env_usize(key: &str, default: usize) -> usize {
    std::env::var(key)
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(default)
}
