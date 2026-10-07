//! Unified fleet bootstrap.
//!
//! [`serve`] owns the *one* boot sequence every service in the fleet runs:
//! telemetry init → externalized-config load + hot-reload watcher → service
//! composition → gRPC serving (trace + traffic layers, health, and the service's
//! own routes) → graceful shutdown. A service plugs in by implementing [`Service`];
//! the deployable binary is then a one-liner —
//! `service_runtime::serve::<MyService>(addr).await`.
//!
//! The split of responsibility is deliberate:
//! * the **runtime** owns process-wide concerns (observability, config IO and its
//!   hot-reload watcher, ingress rate-limiting, socket binding, shutdown, and the
//!   **readiness loop** that drives gRPC health from backend liveness) — written
//!   once, here;
//! * the **service** owns only its domain wiring, its concrete tonic services
//!   ([`Service::register`]), and the backend probes it wants polled
//!   ([`Service::health_probes`]).
//!
//! Services register onto a type-erased [`RoutesBuilder`], so the (statically
//! typed) Tower layer stack the runtime wraps every request in — inbound
//! trace-context extraction on the outside, ingress rate-limiting nested within —
//! never leaks into the [`Service`] contract.
//!
//! ## Ingress rate-limiting
//!
//! When the loaded config has a `[traffic]` section, [`serve`] installs
//! transport's `TrafficLayer` (keyed per the bound profile, honouring its
//! hot-reloadable shadow/enforce flag) and spawns a background loop calling
//! [`TrafficRegistry::prune_all`] to bound limiter memory for unbounded
//! keyspaces. With no `[traffic]` section the layer is a transparent pass-through
//! and no prune loop runs.
//!
//! ## The client edge (second listener)
//!
//! A service always serves its **mesh** listener on `addr` (in-cluster callers,
//! guarded by NetworkPolicy, no token check). When [`GRPC_EDGE_ADDR_ENV`] is set
//! it *also* serves a **client edge** listener — the one the public load balancer
//! targets — guarded by transport's `EdgeLayer`: only the RPCs the service
//! declares in [`Service::EDGE_POLICY`] are reachable, and unless a rule is public
//! the caller must present a valid ES256 edge token minted by `auth`, verified
//! in-process against the JWKS at [`EDGE_JWKS_URL_ENV`]. The edge is fail-closed
//! by construction: enabling it without a verifier config is a boot error, and an
//! empty policy exposes nothing but health.
//!
//! ## Dynamic health
//!
//! The gRPC `grpc.health.v1.Health` status is **not** pinned to `SERVING` at
//! boot. A service reports `SERVING` only once every [`HealthProbe`] it returns
//! has passed at least once, and is demoted to `NOT_SERVING` the moment any probe
//! fails — so Kubernetes readiness reflects real backend reachability.

use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use anyhow::Context;
use async_trait::async_trait;
use auth_context::{spawn_edge_decoder, AuthContextConfig};
use futures::FutureExt;
use infra_config::{
    load_from_path, spawn_watcher, ConfigError, TelemetrySamplingSpec, TelemetrySettings,
    TelemetrySink, TrafficRegistry,
};
// Re-exported so services name the build() parameter type without each taking a
// direct `infra-config` dependency.
pub use infra_config::InfraRegistry;
use telemetry::{SamplingStrategy, TelemetryConfig, TelemetryControl};
use tonic::service::RoutesBuilder;
use tonic_health::server::{health_reporter, HealthReporter};
use tonic_health::ServingStatus;
pub use transport::grpc::edge::{self as edge, EdgePolicy};
use transport::grpc::layer::edge::EdgeGuard;
use transport::grpc::server::{GrpcServerBuilder, GrpcServerConfig};

/// Environment variable naming the externalized-config document.
const INFRA_CONFIG_PATH_ENV: &str = "INFRA_CONFIG_PATH";
/// Path used when [`INFRA_CONFIG_PATH_ENV`] is unset (relative to the working dir).
const DEFAULT_INFRA_CONFIG_PATH: &str = "infrastructure.toml";
/// Environment variable overriding how often [`HealthProbe`]s are polled.
const HEALTH_INTERVAL_ENV: &str = "HEALTH_PROBE_INTERVAL_SECS";
/// Default readiness poll cadence.
const DEFAULT_HEALTH_INTERVAL_SECS: u64 = 10;
/// Environment variable overriding how often the traffic registry is pruned.
const TRAFFIC_PRUNE_INTERVAL_ENV: &str = "TRAFFIC_PRUNE_INTERVAL_SECS";
/// Default traffic-registry prune cadence.
const DEFAULT_TRAFFIC_PRUNE_INTERVAL_SECS: u64 = 60;
/// Environment variable overriding the server-side connection-recycling window
/// (seconds; `0` disables recycling entirely).
const MAX_CONNECTION_AGE_ENV: &str = "GRPC_MAX_CONNECTION_AGE_SECS";
/// Default connection-recycling window. Bounds how long a caller's HTTP/2
/// channel can stay pinned to this replica: on kube-proxy ClusterIP Services,
/// scale-out only rebalances traffic when connections are re-established.
const DEFAULT_MAX_CONNECTION_AGE_SECS: u64 = 300;
/// Environment variable enabling forced close of streams that outlive the age
/// deadline (seconds; unset/`0` = never sever in-flight streams).
const MAX_CONNECTION_AGE_GRACE_ENV: &str = "GRPC_MAX_CONNECTION_AGE_GRACE_SECS";
/// Proxies appending to `X-Forwarded-For` before the listeners (the ALB: 1, the default).
/// Read for `per_ip` rate-limit keying. Bump it when a CDN/WAF proxy sits in front.
const TRUSTED_PROXY_HOPS_ENV: &str = "GRPC_TRUSTED_PROXY_HOPS";

/// Environment variable binding the **client edge** listener (e.g. `0.0.0.0:9443`).
/// Unset ⇒ no edge listener: the service is mesh-only (local dev, workers, TIER-0
/// planes that are not client-facing).
pub const GRPC_EDGE_ADDR_ENV: &str = "GRPC_EDGE_ADDR";
/// JWKS URL of the `auth` service's edge-token keys. Required when the edge
/// listener is enabled.
pub const EDGE_JWKS_URL_ENV: &str = "EDGE_JWKS_URL";
/// Expected `iss` of edge tokens. Required when the edge listener is enabled.
pub const EDGE_TOKEN_ISSUER_ENV: &str = "EDGE_TOKEN_ISSUER";
/// Expected `aud` of edge tokens. Required when the edge listener is enabled.
pub const EDGE_TOKEN_AUDIENCE_ENV: &str = "EDGE_TOKEN_AUDIENCE";
/// Optional JWKS refresh cadence override (seconds; default 300).
const EDGE_JWKS_REFRESH_ENV: &str = "EDGE_JWKS_REFRESH_SECS";
/// Optional per-fetch JWKS timeout override (milliseconds; default 10 000).
const EDGE_JWKS_TIMEOUT_ENV: &str = "EDGE_JWKS_TIMEOUT_MS";

/// Backend health probes now live in the `health` foundation crate, so storage
/// crates can expose ready-made probes (`<storage>::health::probe(...)`) without
/// depending on this platform crate. Re-exported here for ergonomic use from
/// service `health_probes()` impls and the readiness loop below.
pub use health::{FnProbe, HealthProbe};

/// A fleet service the [`serve`] runtime can host.
///
/// Implementors supply domain wiring ([`build`](Service::build)), registration of
/// their concrete tonic services ([`register`](Service::register)), and —
/// optionally — the backend [`HealthProbe`]s the readiness loop should poll.
/// Everything else (telemetry, config, ingress layers, shutdown, health loop) is
/// the runtime's job.
#[async_trait]
pub trait Service: Sized + Send + 'static {
    /// Stable service name; the telemetry resource and a structured-log field.
    const NAME: &'static str;
    /// Service version, conventionally `env!("CARGO_PKG_VERSION")`.
    const VERSION: &'static str;
    /// Fully-qualified gRPC service name used as the health-reporting key,
    /// i.e. `<ConcreteServer as tonic::server::NamedService>::NAME`.
    const GRPC_SERVICE_NAME: &'static str;

    /// The RPCs this service exposes on the **client edge** listener, and what each
    /// requires (see [`edge`]). Default: none — a service with no policy serves
    /// health only on the edge, so nothing is public until it is reviewed and
    /// listed here. Handlers of listed methods must bind their actor field to the
    /// verified caller with [`edge::require_account`] / [`edge::require_profile`].
    const EDGE_POLICY: EdgePolicy = &[];

    /// Pure composition root: build the fully-wired service graph.
    ///
    /// `infra` carries the resolved, hot-reloadable infrastructure registries
    /// (resilience / cache / traffic) for services that consume externalized
    /// policy; services that don't may ignore it.
    async fn build(infra: Arc<InfraRegistry>) -> anyhow::Result<Self>;

    /// Backend liveness probes the readiness loop polls. Default: none — the
    /// service is reported `SERVING` as soon as it is built.
    fn health_probes(&self) -> Vec<Arc<dyn HealthProbe>> {
        Vec::new()
    }

    /// Register the service's concrete gRPC service(s) (typically the service plus
    /// reflection) onto the type-erased `routes`. The runtime applies the shared
    /// layer stack and serves, so the layer types never reach this signature.
    fn register(self, routes: &mut RoutesBuilder) -> anyhow::Result<()>;
}

/// Boots and runs a [`Service`] until a shutdown signal, then drains.
///
/// This is the entire production entrypoint: a deployable binary is just
/// `serve::<S>(addr)`.
pub async fn serve<S: Service>(addr: SocketAddr) -> anyhow::Result<()> {
    // ── Observability ──────────────────────────────────────────────────────────
    // The guard must outlive the server: dropping it flushes in-flight spans/logs.
    // Its control handle is wired into the config watcher below for live retuning.
    let telemetry_guard = telemetry::init(TelemetryConfig::from_env(S::NAME, S::VERSION))
        .context("telemetry init")?;

    // ── Externalized config + hot-reload ───────────────────────────────────────
    // Fail-closed at boot: a malformed document stops the pod from ever serving.
    // `_watcher` must stay alive for the process lifetime — dropping it ends the
    // watch and freezes config at its boot value.
    let path = PathBuf::from(
        std::env::var(INFRA_CONFIG_PATH_ENV)
            .unwrap_or_else(|_| DEFAULT_INFRA_CONFIG_PATH.to_owned()),
    );
    let document = load_from_path(&path)
        .with_context(|| format!("load infrastructure config from {}", path.display()))?;
    let infra = Arc::new(
        InfraRegistry::from_config(document).context("resolve infrastructure config")?,
    );

    // Bridge the `[telemetry]` section to the live pipeline: registering the sink
    // applies the boot-time dials immediately, and the watcher pushes every
    // subsequent change — so a config push retunes log filter + sampling with no
    // restart, fleet-wide.
    if let Some(registry) = infra.telemetry() {
        registry
            .set_sink(Arc::new(TelemetryControlSink { control: telemetry_guard.control() }))
            .context("register telemetry control sink")?;
    }

    let _watcher = spawn_watcher(path, Arc::clone(&infra)).context("spawn config watcher")?;

    // ── Compose the service graph ──────────────────────────────────────────────
    let service = S::build(Arc::clone(&infra)).await.context("service build")?;
    let probes = service.health_probes();

    // ── Routes: health (runtime-owned) + the service's own services ────────────
    let (health, health_service) = health_reporter();
    let mut routes = RoutesBuilder::default();
    routes.add_service(health_service);
    service
        .register(&mut routes)
        .context("register grpc routes")?;

    // ── gRPC servers: inbound-trace (outer) + edge + traffic (inner) layers ────
    // Connection recycling (GOAWAY after max_connection_age) is on by default:
    // it is what re-spreads long-lived HTTP/2 channels across replicas after a
    // scale-out. In-flight streams are never severed unless the grace env is set.
    let mut grpc_config = GrpcServerConfig::default();
    if let Some(age) = max_connection_age_from_env() {
        grpc_config = grpc_config.with_max_connection_age(age);
    }
    if let Some(grace) = max_connection_age_grace_from_env() {
        grpc_config = grpc_config.with_max_connection_age_grace(grace);
    }
    if let Some(hops) = std::env::var(TRUSTED_PROXY_HOPS_ENV).ok().and_then(|v| v.parse().ok()) {
        grpc_config = grpc_config.with_trusted_proxy_hops(hops);
    }
    let traffic = infra.traffic();
    let routes = routes.routes();

    // The mesh listener: no edge guard (in-cluster callers, NetworkPolicy-scoped).
    let mut mesh_builder = GrpcServerBuilder::new(grpc_config.clone());
    if let Some(registry) = &traffic {
        mesh_builder = mesh_builder.with_traffic(Arc::clone(registry));
    }
    let mut mesh_server = mesh_builder.build().context("build mesh gRPC server")?;
    let mesh_router = mesh_server.add_routes(routes.clone());

    // The client edge listener (optional): allow-listed methods + edge-token authn.
    // Fail-closed: enabling it without a verifier config is a boot error.
    let edge_router = match EdgeConfig::from_env()? {
        None => None,
        Some(cfg) => {
            edge::validate_policy(S::EDGE_POLICY).map_err(anyhow::Error::msg)?;
            let guard = Arc::new(EdgeGuard::new(spawn_edge_decoder(&cfg.auth), S::EDGE_POLICY));
            let mut edge_builder = GrpcServerBuilder::new(grpc_config).with_edge(Arc::clone(&guard));
            if let Some(registry) = &traffic {
                edge_builder = edge_builder.with_traffic(Arc::clone(registry));
            }
            let mut edge_server = edge_builder.build().context("build edge gRPC server")?;
            tracing::info!(
                service = S::NAME,
                addr = %cfg.addr,
                exposed_methods = guard.exposed_methods(),
                jwks_url = %cfg.auth.jwks_url,
                "client edge listener enabled"
            );
            Some((cfg.addr, edge_server.add_routes(routes)))
        }
    };

    // ── Background loops: readiness health + traffic-memory bounding ────────────
    spawn_readiness(S::GRPC_SERVICE_NAME, health, probes);
    if let Some(registry) = traffic {
        spawn_traffic_prune(registry);
    }

    tracing::info!(service = S::NAME, version = S::VERSION, %addr, "gRPC server listening");

    // One shutdown signal fans out to every listener.
    let shutdown = shutdown_signal().shared();
    match edge_router {
        None => mesh_router
            .serve_with_shutdown(addr, shutdown)
            .await
            .context("grpc server terminated")?,
        Some((edge_addr, edge_router)) => {
            tokio::try_join!(
                async {
                    mesh_router
                        .serve_with_shutdown(addr, shutdown.clone())
                        .await
                        .context("mesh grpc server terminated")
                },
                async {
                    edge_router
                        .serve_with_shutdown(edge_addr, shutdown.clone())
                        .await
                        .context("edge grpc server terminated")
                },
            )?;
        }
    }

    tracing::info!(service = S::NAME, "shutdown complete");
    Ok(())
}

/// The [`edge::StaffGate`] for staff RPCs on the **mesh** (#837): it verifies
/// the staff member's token against the same `auth` JWKS as the edge
/// ([`EDGE_JWKS_URL_ENV`], issuer, audience), whether or not this pod runs an
/// edge listener. Any of the three unset ⇒ every staff call is denied.
pub fn staff_gate_from_env() -> edge::StaffGate {
    let var = |key: &str| std::env::var(key).ok().filter(|v| !v.is_empty());
    let (Some(jwks_url), Some(issuer), Some(audience)) =
        (var(EDGE_JWKS_URL_ENV), var(EDGE_TOKEN_ISSUER_ENV), var(EDGE_TOKEN_AUDIENCE_ENV))
    else {
        tracing::warn!("{EDGE_JWKS_URL_ENV} / issuer / audience unset: staff RPCs deny every call");
        return edge::StaffGate::deny_all();
    };
    let auth = AuthContextConfig {
        jwks_url,
        expected_issuer: Some(issuer),
        expected_audience: Some(audience),
        refresh_interval: interval_from_env(EDGE_JWKS_REFRESH_ENV, 300),
        fetch_timeout: Duration::from_millis(
            std::env::var(EDGE_JWKS_TIMEOUT_ENV).ok().and_then(|v| v.parse().ok()).unwrap_or(10_000),
        ),
        ..AuthContextConfig::default()
    };
    edge::StaffGate::new(spawn_edge_decoder(&auth))
}

/// Resolved client-edge settings — `None` when [`GRPC_EDGE_ADDR_ENV`] is unset.
struct EdgeConfig {
    addr: SocketAddr,
    auth: AuthContextConfig,
}

impl EdgeConfig {
    /// Reads the edge settings. Enabling the listener without the verifier
    /// triple (JWKS URL, issuer, audience) is an error: an edge that cannot verify
    /// tokens must not come up at all.
    fn from_env() -> anyhow::Result<Option<Self>> {
        let Some(raw_addr) = std::env::var(GRPC_EDGE_ADDR_ENV).ok().filter(|v| !v.is_empty())
        else {
            return Ok(None);
        };
        let addr: SocketAddr = raw_addr
            .parse()
            .with_context(|| format!("{GRPC_EDGE_ADDR_ENV}={raw_addr} is not a socket address"))?;

        let required = |key: &str| -> anyhow::Result<String> {
            std::env::var(key)
                .ok()
                .filter(|v| !v.is_empty())
                .ok_or_else(|| {
                    anyhow::anyhow!(
                        "{GRPC_EDGE_ADDR_ENV} is set but {key} is not — the client edge \
                         cannot verify tokens without it (fail-closed)"
                    )
                })
        };
        let auth = AuthContextConfig {
            jwks_url: required(EDGE_JWKS_URL_ENV)?,
            expected_issuer: Some(required(EDGE_TOKEN_ISSUER_ENV)?),
            expected_audience: Some(required(EDGE_TOKEN_AUDIENCE_ENV)?),
            refresh_interval: interval_from_env(EDGE_JWKS_REFRESH_ENV, 300),
            fetch_timeout: Duration::from_millis(
                std::env::var(EDGE_JWKS_TIMEOUT_ENV)
                    .ok()
                    .and_then(|v| v.parse().ok())
                    .unwrap_or(10_000),
            ),
            ..AuthContextConfig::default()
        };
        Ok(Some(Self { addr, auth }))
    }
}

/// Spawns the background loop that maps backend [`HealthProbe`] results onto the
/// service's gRPC health status.
///
/// With no probes the service is marked `SERVING` once and the task exits. With
/// probes, the loop polls on [`HEALTH_INTERVAL_ENV`] cadence (first tick fires
/// immediately) and only writes the reporter on a *transition*, so watchers
/// aren't churned every tick.
fn spawn_readiness(
    service_name: &'static str,
    health: HealthReporter,
    probes: Vec<Arc<dyn HealthProbe>>,
) {
    if probes.is_empty() {
        tokio::spawn(async move {
            health
                .set_service_status(service_name, ServingStatus::Serving)
                .await;
        });
        return;
    }

    let interval = interval_from_env(HEALTH_INTERVAL_ENV, DEFAULT_HEALTH_INTERVAL_SECS);

    tokio::spawn(async move {
        let mut ticker = tokio::time::interval(interval);
        // `None` until the first poll establishes a baseline, so the first result
        // is always written.
        let mut last_serving: Option<bool> = None;

        loop {
            ticker.tick().await;

            let mut serving = true;
            for probe in &probes {
                if let Err(error) = probe.check().await {
                    serving = false;
                    tracing::warn!(
                        service = service_name,
                        probe = probe.name(),
                        %error,
                        "health probe failed; marking NOT_SERVING"
                    );
                    break; // one failed dependency is enough to fail readiness
                }
            }

            if last_serving != Some(serving) {
                let status = if serving {
                    ServingStatus::Serving
                } else {
                    ServingStatus::NotServing
                };
                health.set_service_status(service_name, status).await;
                tracing::info!(service = service_name, serving, "gRPC health status changed");
                last_serving = Some(serving);
            }
        }
    });
}

/// Spawns the background loop that bounds rate-limiter memory by dropping idle
/// keys across every traffic profile on [`TRAFFIC_PRUNE_INTERVAL_ENV`] cadence.
/// Cheap for bounded (`per_method`) profiles; essential for unbounded
/// (`per_caller`) ones.
fn spawn_traffic_prune(registry: Arc<TrafficRegistry>) {
    let interval = interval_from_env(
        TRAFFIC_PRUNE_INTERVAL_ENV,
        DEFAULT_TRAFFIC_PRUNE_INTERVAL_SECS,
    );

    tokio::spawn(async move {
        let mut ticker = tokio::time::interval(interval);
        ticker.tick().await; // first tick is immediate; nothing to prune at t=0
        loop {
            ticker.tick().await;
            registry.prune_all();
            tracing::debug!(tracked_keys = registry.tracked_keys(), "traffic registry pruned");
        }
    });
}

/// Reads a seconds-valued interval from `env_var`, falling back to `default_secs`
/// when unset or unparseable.
fn interval_from_env(env_var: &str, default_secs: u64) -> Duration {
    Duration::from_secs(
        std::env::var(env_var)
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(default_secs),
    )
}

/// Resolves the connection-recycling window from [`MAX_CONNECTION_AGE_ENV`]
/// (default [`DEFAULT_MAX_CONNECTION_AGE_SECS`], `0` disables).
fn max_connection_age_from_env() -> Option<Duration> {
    let raw = std::env::var(MAX_CONNECTION_AGE_ENV).ok();
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| u64::from(d.subsec_nanos()))
        .unwrap_or(0);
    resolve_connection_age(raw.as_deref(), nanos)
}

/// Pure core of [`max_connection_age_from_env`]: parses the override and applies
/// a ±10% per-process jitter (seeded by `entropy`) so replicas started together —
/// a rollout — don't GOAWAY every caller at the same instant.
fn resolve_connection_age(raw: Option<&str>, entropy: u64) -> Option<Duration> {
    let secs: u64 = match raw {
        Some(v) => v.parse().ok().unwrap_or(DEFAULT_MAX_CONNECTION_AGE_SECS),
        None => DEFAULT_MAX_CONNECTION_AGE_SECS,
    };
    if secs == 0 {
        return None;
    }
    let base_ms = secs.saturating_mul(1000);
    let span_ms = (base_ms / 5).max(1); // 20% wide band centred on base
    Some(Duration::from_millis(
        base_ms - span_ms / 2 + entropy % span_ms,
    ))
}

/// Grace is opt-in: only servers whose in-flight streams are safe to sever
/// should set [`MAX_CONNECTION_AGE_GRACE_ENV`] (chat/notification hold
/// long-lived server streams that must outlive the GOAWAY).
fn max_connection_age_grace_from_env() -> Option<Duration> {
    let secs: u64 = std::env::var(MAX_CONNECTION_AGE_GRACE_ENV)
        .ok()?
        .parse()
        .ok()?;
    (secs > 0).then(|| Duration::from_secs(secs))
}

/// Resolves when the process receives SIGINT (Ctrl-C) or, on Unix, SIGTERM —
/// the signal Kubernetes sends on pod termination. The tonic server drains
/// in-flight requests before returning. If neither handler can be installed we
/// park forever rather than shutting down spuriously.
async fn shutdown_signal() {
    #[cfg(unix)]
    {
        use tokio::signal::unix::{signal, SignalKind};

        let mut sigterm = match signal(SignalKind::terminate()) {
            Ok(stream) => Some(stream),
            Err(error) => {
                tracing::error!(%error, "failed to install SIGTERM handler");
                None
            }
        };

        match sigterm {
            Some(ref mut sigterm) => {
                tokio::select! {
                    result = tokio::signal::ctrl_c() => {
                        if result.is_err() {
                            // SIGTERM remains armed; wait for it instead.
                            tracing::error!("failed to install Ctrl-C handler");
                            sigterm.recv().await;
                        }
                    }
                    _ = sigterm.recv() => {}
                }
            }
            None => {
                if tokio::signal::ctrl_c().await.is_err() {
                    tracing::error!(
                        "failed to install Ctrl-C handler; shutdown signalling disabled"
                    );
                    std::future::pending::<()>().await;
                }
            }
        }
    }

    #[cfg(not(unix))]
    {
        if tokio::signal::ctrl_c().await.is_err() {
            tracing::error!("failed to install Ctrl-C handler; shutdown signalling disabled");
            std::future::pending::<()>().await;
        }
    }

    tracing::info!("shutdown signal received; draining in-flight requests");
}

/// Bridges the `infra-config` `[telemetry]` section to the live telemetry
/// pipeline. Lives here (the platform tier) because it depends on both
/// `infra-config` and `telemetry`, which must not depend on each other.
struct TelemetryControlSink {
    control: TelemetryControl,
}

impl TelemetrySink for TelemetryControlSink {
    fn apply(&self, settings: &TelemetrySettings) -> Result<(), ConfigError> {
        if let Some(directive) = &settings.log_filter {
            self.control
                .set_log_filter(directive)
                .map_err(|e| ConfigError::validation(format!("telemetry log filter: {e}")))?;
        }
        if let Some(spec) = &settings.sampling {
            let strategy = match spec {
                TelemetrySamplingSpec::AlwaysOn => SamplingStrategy::AlwaysOn,
                TelemetrySamplingSpec::AlwaysOff => SamplingStrategy::AlwaysOff,
                TelemetrySamplingSpec::TraceIdRatio { ratio } => {
                    SamplingStrategy::TraceIdRatio(*ratio)
                }
            };
            self.control
                .set_sampling(strategy)
                .map_err(|e| ConfigError::validation(format!("telemetry sampling: {e}")))?;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn connection_age_defaults_to_five_minutes_within_the_jitter_band() {
        for entropy in [0, 1, 59_999, 1_000_000_007] {
            let age = resolve_connection_age(None, entropy).expect("default is enabled");
            let ms = age.as_millis() as u64;
            // 300s ±10% ⇒ [270_000, 330_000)
            assert!((270_000..330_000).contains(&ms), "out of band: {ms}ms");
        }
    }

    #[test]
    fn connection_age_zero_disables_recycling() {
        assert_eq!(resolve_connection_age(Some("0"), 42), None);
    }

    #[test]
    fn connection_age_override_recentres_the_band() {
        let age = resolve_connection_age(Some("60"), 12_345).expect("enabled");
        let ms = age.as_millis() as u64;
        assert!((54_000..66_000).contains(&ms), "out of band: {ms}ms");
    }

    #[test]
    fn connection_age_garbage_falls_back_to_the_default() {
        let age = resolve_connection_age(Some("not-a-number"), 7).expect("enabled");
        let ms = age.as_millis() as u64;
        assert!((270_000..330_000).contains(&ms), "out of band: {ms}ms");
    }

    #[test]
    fn jitter_is_deterministic_per_entropy_and_varies_across_it() {
        let a = resolve_connection_age(None, 1000).unwrap();
        let b = resolve_connection_age(None, 1000).unwrap();
        let c = resolve_connection_age(None, 999_999).unwrap();
        assert_eq!(a, b);
        assert_ne!(a, c);
    }
}
