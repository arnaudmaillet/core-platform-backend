//! Environment-sourced configuration for the auth service. Resolved once at boot
//! and threaded into the composition root ([`crate::app::App::build`]).

use chrono::Duration;

use crate::application::SessionPolicy;
use crate::infrastructure::idp::KeycloakConfig;
use crate::infrastructure::token::{EsKeyMaterial, EsVerifyingKey};

/// Fully-resolved auth configuration (token policy, signing material, IdP broker,
/// and the `account` service endpoint). Backend connection configs (Postgres /
/// Redis / Kafka) are resolved separately via their own `from_env`.
pub struct AuthConfig {
    pub policy: SessionPolicy,
    pub signing: EsKeyMaterial,
    /// Retiring keys still accepted for verification + published in the JWKS
    /// during a rotation window. Empty in steady state.
    pub retiring_keys: Vec<EsVerifyingKey>,
    pub keycloak: KeycloakConfig,
    /// gRPC endpoint of the `account` service, e.g. `http://account:50059`.
    pub account_endpoint: String,
    /// Per-request deadline on `account` RPCs. This sits on the login hot path,
    /// so a hung dependency must fail fast rather than pile up requests.
    pub account_rpc_timeout: std::time::Duration,
    /// Connect deadline when dialing the `account` channel.
    pub account_connect_timeout: std::time::Duration,
    /// gRPC endpoint of the `profile` service, e.g. `http://profile:50052` —
    /// read at every mint for the edge token's `pids` claim.
    pub profile_endpoint: String,
    /// Per-request deadline on `profile` RPCs (the lookup is fail-safe, but a
    /// hung dependency must not stall the login hot path).
    pub profile_rpc_timeout: std::time::Duration,
    /// Connect deadline when dialing the `profile` channel.
    pub profile_connect_timeout: std::time::Duration,
    /// Total request deadline for Keycloak HTTP calls (token exchange).
    pub idp_http_timeout: std::time::Duration,
    /// Connect deadline for Keycloak HTTP calls.
    pub idp_connect_timeout: std::time::Duration,
    /// `AUTH_GUEST_SESSIONS_ENABLED` (default **false**): `StartGuestSession`
    /// writes a session per call with no credential, so it stays off wherever
    /// the abuse controls (per-IP / per-device limits, App Attest — B5) are not
    /// in front of it yet. The local fleet turns it on.
    pub guest_sessions_enabled: bool,
}

impl AuthConfig {
    /// Resolves configuration from the environment.
    ///
    /// Required: `AUTH_SIGNING_PRIVATE_PEM`, `AUTH_SIGNING_PUBLIC_PEM`. Everything
    /// else has a production-shaped default.
    pub fn from_env() -> anyhow::Result<Self> {
        let policy = SessionPolicy::new(
            Duration::seconds(env_secs("AUTH_ACCESS_TTL_SECS", 600)),
            Duration::seconds(env_secs("AUTH_SESSION_TTL_SECS", 1_800)),
            Duration::seconds(env_secs("AUTH_ABSOLUTE_TTL_SECS", 28_800)),
            Duration::seconds(env_secs("AUTH_REFRESH_TTL_SECS", 604_800)),
        );

        let signing = EsKeyMaterial {
            private_pem: env_required("AUTH_SIGNING_PRIVATE_PEM")?.into_bytes(),
            public_pem: env_required("AUTH_SIGNING_PUBLIC_PEM")?.into_bytes(),
            key_id: env_or("AUTH_SIGNING_KID", "auth-es256-1"),
            issuer: env_or("AUTH_TOKEN_ISSUER", "https://auth.core-platform"),
            audience: env_or("AUTH_TOKEN_AUDIENCE", "core-platform"),
        };

        let keycloak = KeycloakConfig {
            token_endpoint: env_or("AUTH_KEYCLOAK_TOKEN_ENDPOINT", String::new()),
            client_id: env_or("AUTH_KEYCLOAK_CLIENT_ID", String::new()),
            client_secret: env_or("AUTH_KEYCLOAK_CLIENT_SECRET", String::new()),
            scope: env_or("AUTH_KEYCLOAK_SCOPE", "openid".to_owned()),
        };

        // Optional single retiring key for a rotation window.
        let retiring_keys = match (
            std::env::var("AUTH_SIGNING_RETIRING_PUBLIC_PEM").ok(),
            std::env::var("AUTH_SIGNING_RETIRING_KID").ok(),
        ) {
            (Some(pem), Some(kid)) if !pem.is_empty() && !kid.is_empty() => {
                vec![EsVerifyingKey { key_id: kid, public_pem: pem.into_bytes() }]
            }
            _ => Vec::new(),
        };

        Ok(Self {
            policy,
            signing,
            retiring_keys,
            keycloak,
            account_endpoint: env_or("AUTH_ACCOUNT_GRPC_ENDPOINT", "http://localhost:50059"),
            account_rpc_timeout: env_ms("AUTH_ACCOUNT_RPC_TIMEOUT_MS", 2_000),
            account_connect_timeout: env_ms("AUTH_ACCOUNT_CONNECT_TIMEOUT_MS", 2_000),
            profile_endpoint: env_or("AUTH_PROFILE_GRPC_ENDPOINT", "http://localhost:50052"),
            profile_rpc_timeout: env_ms("AUTH_PROFILE_RPC_TIMEOUT_MS", 2_000),
            profile_connect_timeout: env_ms("AUTH_PROFILE_CONNECT_TIMEOUT_MS", 2_000),
            idp_http_timeout: env_ms("AUTH_IDP_HTTP_TIMEOUT_MS", 5_000),
            idp_connect_timeout: env_ms("AUTH_IDP_CONNECT_TIMEOUT_MS", 2_000),
            guest_sessions_enabled: std::env::var("AUTH_GUEST_SESSIONS_ENABLED")
                .is_ok_and(|v| matches!(v.trim(), "1" | "true" | "TRUE" | "yes")),
        })
    }
}

fn env_or(key: &str, default: impl Into<String>) -> String {
    std::env::var(key).unwrap_or_else(|_| default.into())
}

fn env_secs(key: &str, default: i64) -> i64 {
    std::env::var(key).ok().and_then(|v| v.parse().ok()).unwrap_or(default)
}

fn env_ms(key: &str, default: u64) -> std::time::Duration {
    std::time::Duration::from_millis(
        std::env::var(key).ok().and_then(|v| v.parse().ok()).unwrap_or(default),
    )
}

fn env_required(key: &str) -> anyhow::Result<String> {
    std::env::var(key).map_err(|_| anyhow::anyhow!("required env var {key} is not set"))
}
