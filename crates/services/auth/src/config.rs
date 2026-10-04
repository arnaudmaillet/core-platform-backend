//! Environment-sourced configuration for the auth service. Resolved once at boot
//! and threaded into the composition root ([`crate::app::App::build`]).

use chrono::Duration;

use crate::application::SessionPolicy;
use crate::infrastructure::idp::{KeycloakAdminConfig, KeycloakConfig};
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
    /// The Keycloak Admin API client `ChangePassword` uses
    /// (`AUTH_KEYCLOAK_ADMIN_URL` / `_CLIENT_ID` / `_CLIENT_SECRET`). Unset ⇒
    /// changing a password answers UNAVAILABLE; everything else works.
    pub keycloak_admin: KeycloakAdminConfig,
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
    /// Native Sign in with Apple / Google: the app's client ids a provider's
    /// id_token must be minted for (`aud`). Empty = that provider is off.
    pub apple_audiences: Vec<String>,
    pub google_audiences: Vec<String>,
    /// Deadline on fetching a provider's JWKS.
    pub federated_jwks_timeout: std::time::Duration,
    /// One-time codes: how they are sent (`smtp`, `log` for local runs, or
    /// unset = off), the SMTP relay (Amazon SES), and their policy.
    pub verification_sender: String,
    pub smtp: Option<crate::infrastructure::notify::SmtpConfig>,
    /// SMS codes through Amazon SNS (`AUTH_SMS_SENDER=sns` + `AUTH_SNS_*`).
    pub sms_sender: String,
    pub sns: Option<crate::infrastructure::notify::SnsConfig>,
    pub verification: crate::application::command::VerificationPolicy,
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

        let keycloak_admin = KeycloakAdminConfig {
            admin_url: env_or("AUTH_KEYCLOAK_ADMIN_URL", String::new()),
            token_endpoint: keycloak.token_endpoint.clone(),
            client_id: env_or("AUTH_KEYCLOAK_ADMIN_CLIENT_ID", String::new()),
            client_secret: env_or("AUTH_KEYCLOAK_ADMIN_CLIENT_SECRET", String::new()),
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
            keycloak_admin,
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
            apple_audiences: env_list("AUTH_APPLE_AUDIENCES"),
            google_audiences: env_list("AUTH_GOOGLE_AUDIENCES"),
            federated_jwks_timeout: env_ms("AUTH_FEDERATED_JWKS_TIMEOUT_MS", 3_000),
            verification_sender: env_or("AUTH_VERIFICATION_SENDER", "").trim().to_ascii_lowercase(),
            smtp: smtp_from_env(),
            sms_sender: env_or("AUTH_SMS_SENDER", "").trim().to_ascii_lowercase(),
            sns: sns_from_env(),
            verification: crate::application::command::VerificationPolicy {
                ttl: chrono::Duration::seconds(env_secs("AUTH_VERIFICATION_TTL_SECS", 600)),
                max_attempts: env_secs("AUTH_VERIFICATION_MAX_ATTEMPTS", 5).max(1) as u32,
                per_hour: env_secs("AUTH_VERIFICATION_PER_HOUR", 5).max(1) as u32,
                per_day: env_secs("AUTH_VERIFICATION_PER_DAY", 20).max(1) as u32,
                resend: chrono::Duration::seconds(env_secs("AUTH_VERIFICATION_RESEND_SECS", 30)),
                max_failures: env_secs("AUTH_VERIFICATION_MAX_FAILURES", 15).max(1) as u32,
                failure_window: chrono::Duration::hours(24),
                sms_countries: sms_countries_from_env().map_err(anyhow::Error::msg)?,
                sms_daily_budget: env_secs(
                    "AUTH_SMS_DAILY_BUDGET",
                    i64::from(crate::application::command::DEFAULT_SMS_DAILY_BUDGET),
                )
                .max(0) as u32,
            },
        })
    }
}

/// Where SMS codes may go: `AUTH_SMS_COUNTRIES` (comma-separated ISO 3166-1
/// alpha-2), or the launch markets. An unknown code fails the boot.
fn sms_countries_from_env() -> Result<std::collections::BTreeSet<String>, String> {
    let listed = env_list("AUTH_SMS_COUNTRIES");
    if listed.is_empty() {
        return Ok(crate::application::command::SMS_LAUNCH_COUNTRIES.iter().map(|c| (*c).to_owned()).collect());
    }
    listed
        .iter()
        .map(|c| {
            let c = c.trim().to_ascii_uppercase();
            c.parse::<phonenumber::country::Id>()
                .map(|_| c.clone())
                .map_err(|_| format!("AUTH_SMS_COUNTRIES: unknown country code {c:?}"))
        })
        .collect()
}

/// The SMTP relay for email codes, when `AUTH_SMTP_HOST` is set.
fn smtp_from_env() -> Option<crate::infrastructure::notify::SmtpConfig> {
    let host = std::env::var("AUTH_SMTP_HOST").ok().filter(|h| !h.trim().is_empty())?;
    Some(crate::infrastructure::notify::SmtpConfig {
        host,
        port: std::env::var("AUTH_SMTP_PORT").ok().and_then(|p| p.parse().ok()).unwrap_or(587),
        username: env_or("AUTH_SMTP_USERNAME", ""),
        password: env_or("AUTH_SMTP_PASSWORD", ""),
        from: env_or("AUTH_SMTP_FROM", ""),
        code_ttl_minutes: env_secs("AUTH_VERIFICATION_TTL_SECS", 600) / 60,
    })
}

/// Amazon SNS for SMS codes, when `AUTH_SNS_REGION` is set.
fn sns_from_env() -> Option<crate::infrastructure::notify::SnsConfig> {
    let region = std::env::var("AUTH_SNS_REGION").ok().filter(|r| !r.trim().is_empty())?;
    Some(crate::infrastructure::notify::SnsConfig {
        region,
        access_key_id: env_or("AUTH_SNS_ACCESS_KEY_ID", ""),
        secret_access_key: env_or("AUTH_SNS_SECRET_ACCESS_KEY", ""),
        sender_id: std::env::var("AUTH_SNS_SENDER_ID").ok().filter(|s| !s.trim().is_empty()),
        code_ttl_minutes: env_secs("AUTH_VERIFICATION_TTL_SECS", 600) / 60,
    })
}

/// A comma-separated list (blank entries dropped).
fn env_list(key: &str) -> Vec<String> {
    std::env::var(key)
        .map(|v| v.split(',').map(str::trim).filter(|s| !s.is_empty()).map(str::to_owned).collect())
        .unwrap_or_default()
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
