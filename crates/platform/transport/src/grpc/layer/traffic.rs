//! Server-side ingress rate-limiting Tower layer.
//!
//! Translates the pure [`traffic`] decision into gRPC: it resolves the inbound method's
//! profile from a [`TrafficRegistry`], extracts a key per the profile's [`Scope`], charges
//! one cell, and either forwards to the handler or short-circuits with a
//! `RESOURCE_EXHAUSTED` status — without ever calling the inner service.
//!
//! # Placement
//!
//! Installed on the **server** via [`crate::grpc::server::GrpcServerBuilder`], inside the
//! trace span so throttle decisions are observable.
//!
//! # Observability
//!
//! Every throttle *decision* (whether enforced or merely shadowed) increments the
//! `infra_traffic_throttled` counter — surfaced by the Prometheus exporter as
//! `infra_traffic_throttled_total` — labelled by `profile`, `route`, and `status`
//! (`enforced` | `shadow`). This is what makes a shadow-mode pilot legible: you watch the
//! `shadow` series to see what *would* be rejected before flipping `enforce`. Route
//! cardinality is bounded — unbound methods collapse to a single `<unbound>` label so a
//! flood of arbitrary paths can't blow up the time-series database.
//!
//! # `per_caller` and identity
//!
//! `per_caller` keys on the caller identity carried in an inbound header injected by the
//! edge/service mesh (configurable, default [`DEFAULT_IDENTITY_HEADER`]). We trust it
//! because the mesh sets/overwrites it and strips client-supplied values at the trust
//! boundary — so the layer needs no in-process token verification. When the header is
//! absent (an unauthenticated method, or — wrongly — a request that bypassed the mesh) the
//! layer **degrades to method-level keying** rather than collapsing all callers into one
//! bucket: it still limits, just not per-identity. This is logged at debug.
//!
//! # `per_ip`
//!
//! `per_ip` keys on the client address (see [`crate::grpc::client_ip`]: the entry the
//! trusted proxy appended to `X-Forwarded-For`, else the peer address) — for anonymous
//! methods with no principal. An unknown address degrades to method-level keying.
//!
//! # Mesh vs client edge
//!
//! On the client edge listener the profile comes from the `[traffic]` edge resolution
//! ([`TrafficRegistry::resolve_edge`]); on the mesh listener from the mesh one. A client
//! profile (`per_caller`) on the mesh would have no principal to key on.

use std::net::SocketAddr;
use std::sync::Arc;
use std::task::{Context, Poll};
use std::time::Duration;

use futures::future::BoxFuture;
use http::header::HeaderName;
use http::HeaderMap;
use infra_config::TrafficRegistry;
use opentelemetry::{global, metrics::Counter, KeyValue};
use tonic::{body::Body, Status};
use tower::{Layer, Service};
use traffic::{BackendError, QuotaBackend, Scope, TrafficDecision, TrafficProfile};

use crate::grpc::client_ip::{client_ip, DEFAULT_TRUSTED_PROXY_HOPS};
use crate::grpc::server::config::DEFAULT_IDENTITY_HEADER;

/// Instrument name. The Prometheus exporter appends `_total` for monotonic sums, so this
/// surfaces as `infra_traffic_throttled_total`; OTLP/collector backends see it as-is.
const THROTTLE_METRIC: &str = "infra_traffic_throttled";

/// Requests a `per_caller` / `per_ip` profile had to key per method (no identity / no
/// client address): every such caller shares one bucket. Behind the ALB this never
/// happens for `per_ip`; a non-zero rate there is a misconfiguration (proxy hops).
const KEY_FALLBACK_METRIC: &str = "infra_traffic_key_fallback";

/// Logged once per process per scope, then only counted.
static WARNED_CALLER_FALLBACK: std::sync::atomic::AtomicBool = std::sync::atomic::AtomicBool::new(false);
static WARNED_IP_FALLBACK: std::sync::atomic::AtomicBool = std::sync::atomic::AtomicBool::new(false);

fn key_fallback_counter() -> Counter<u64> {
    global::meter("transport")
        .u64_counter(KEY_FALLBACK_METRIC)
        .with_description(
            "Requests whose per_caller/per_ip rate-limit key fell back to the method \
             (no identity / no client address), labelled by scope and route.",
        )
        .build()
}

/// Route label for methods with no explicit binding — bounds metric cardinality.
const UNBOUND_ROUTE: &str = "<unbound>";

/// Builds the throttle counter from the global meter. The global provider is installed by
/// `telemetry::init`; before that (or in tests) this binds to a no-op meter, so `add` is a
/// harmless no-op rather than a panic.
fn throttle_counter() -> Counter<u64> {
    global::meter("transport")
        .u64_counter(THROTTLE_METRIC)
        .with_description(
            "Requests that triggered a rate-limit throttle decision, labelled by \
             profile, route, and status (enforced|shadow).",
        )
        .build()
}

/// Tower [`Layer`] that rate-limits inbound gRPC requests from a [`TrafficRegistry`].
///
/// Holds an `Option`: when `None` (no `[traffic]` section configured) the layer is a
/// transparent pass-through, so the server's type is identical whether or not limiting is
/// enabled.
#[derive(Clone)]
pub struct TrafficLayer {
    registry: Option<Arc<TrafficRegistry>>,
    counter: Counter<u64>,
    fallback_counter: Counter<u64>,
    identity_header: HeaderName,
    /// Distributed-mode coordination backend (`traffic-redis`). `None` → `distributed`
    /// profiles degrade to the local limiter (logged); `local` profiles never use it.
    backend: Option<Arc<dyn QuotaBackend>>,
    /// Whether this layer guards the client edge listener (edge profile resolution).
    edge: bool,
    /// Proxies appending to `X-Forwarded-For` before this listener (`per_ip` keying).
    trusted_proxy_hops: usize,
}

impl TrafficLayer {
    /// A pass-through layer (no limiting). Used when no `[traffic]` section is configured.
    pub fn disabled() -> Self {
        Self {
            registry: None,
            counter: throttle_counter(),
            fallback_counter: key_fallback_counter(),
            identity_header: HeaderName::from_static(DEFAULT_IDENTITY_HEADER),
            backend: None,
            edge: false,
            trusted_proxy_hops: DEFAULT_TRUSTED_PROXY_HOPS,
        }
    }

    /// A layer that enforces the profiles in `registry`, reading `per_caller` identity from
    /// `identity_header` (the edge-mesh-injected header).
    pub fn new(registry: Arc<TrafficRegistry>, identity_header: HeaderName) -> Self {
        Self {
            registry: Some(registry),
            counter: throttle_counter(),
            fallback_counter: key_fallback_counter(),
            identity_header,
            backend: None,
            edge: false,
            trusted_proxy_hops: DEFAULT_TRUSTED_PROXY_HOPS,
        }
    }

    /// Marks this layer as the client edge listener's: profiles resolve through the
    /// `[traffic]` edge bindings.
    pub fn for_edge(mut self, edge: bool) -> Self {
        self.edge = edge;
        self
    }

    /// Sets how many proxies append to `X-Forwarded-For` before this listener.
    pub fn with_trusted_proxy_hops(mut self, hops: usize) -> Self {
        self.trusted_proxy_hops = hops;
        self
    }

    /// Attaches the distributed-mode coordination backend (e.g. `traffic-redis`). Required
    /// for `distributed` profiles to enforce a fleet-global budget; without it they degrade
    /// to local per-replica limiting.
    pub fn with_backend(mut self, backend: Arc<dyn QuotaBackend>) -> Self {
        self.backend = Some(backend);
        self
    }
}

impl<S> Layer<S> for TrafficLayer {
    type Service = TrafficService<S>;

    fn layer(&self, inner: S) -> Self::Service {
        TrafficService {
            inner,
            registry: self.registry.clone(),
            counter: self.counter.clone(),
            fallback_counter: self.fallback_counter.clone(),
            identity_header: self.identity_header.clone(),
            backend: self.backend.clone(),
            edge: self.edge,
            trusted_proxy_hops: self.trusted_proxy_hops,
        }
    }
}

/// The concrete service produced by [`TrafficLayer`].
#[derive(Clone)]
pub struct TrafficService<S> {
    inner: S,
    registry: Option<Arc<TrafficRegistry>>,
    counter: Counter<u64>,
    fallback_counter: Counter<u64>,
    identity_header: HeaderName,
    backend: Option<Arc<dyn QuotaBackend>>,
    edge: bool,
    trusted_proxy_hops: usize,
}

impl<S> Service<http::Request<Body>> for TrafficService<S>
where
    S: Service<http::Request<Body>, Response = http::Response<Body>> + Clone + Send + 'static,
    S::Future: Send + 'static,
    S::Error: Send + 'static,
{
    type Response = S::Response;
    type Error = S::Error;
    type Future = BoxFuture<'static, Result<S::Response, S::Error>>;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.inner.poll_ready(cx)
    }

    fn call(&mut self, req: http::Request<Body>) -> Self::Future {
        let Some(registry) = self.registry.as_ref() else {
            // Limiting disabled — straight pass-through.
            return Box::pin(self.inner.call(req));
        };

        let method = req.uri().path();
        let (profile_name, bound, profile) =
            if self.edge { registry.resolve_edge(method) } else { registry.resolve(method) };
        let peer = req
            .extensions()
            .get::<tonic::transport::server::TcpConnectInfo>()
            .and_then(|info| info.remote_addr());
        let (key, fell_back) = extract_key(
            profile.scope(),
            method,
            req.headers(),
            &self.identity_header,
            peer,
            self.trusted_proxy_hops,
        );
        let route = if bound { method } else { UNBOUND_ROUTE };
        if fell_back {
            record_key_fallback(&self.fallback_counter, profile.scope(), route);
        }

        // Distributed profiles consult the (async) backend, so their decision is made inside
        // the returned future; local profiles decide synchronously here on the hot path.
        if profile.is_distributed() {
            let backend = self.backend.clone();
            let counter = self.counter.clone();
            let profile_name = profile_name.to_owned();
            let route = route.to_owned();
            let method = method.to_owned();
            // Move a ready clone of the inner service into the future (tower readiness idiom).
            let clone = self.inner.clone();
            let mut inner = std::mem::replace(&mut self.inner, clone);

            return Box::pin(async move {
                let decision = distributed_check(&profile, &key, backend.as_ref()).await;
                let enforce = profile.enforce();
                if let Some(response) =
                    handle_decision(decision, &counter, &profile_name, &route, enforce, &method)
                {
                    return Ok(response);
                }
                inner.call(req).await
            });
        }

        let enforce = profile.enforce();
        if let Some(response) =
            handle_decision(profile.check(&key), &self.counter, profile_name, route, enforce, method)
        {
            return Box::pin(async move { Ok(response) });
        }
        Box::pin(self.inner.call(req))
    }
}

/// Applies a throttle decision: records the metric, then short-circuits with a
/// `RESOURCE_EXHAUSTED` response when enforcing, or returns `None` to admit (shadow mode, or
/// the decision was `Allow`). Shared by the local and distributed paths.
fn handle_decision(
    decision: TrafficDecision,
    counter: &Counter<u64>,
    profile_name: &str,
    route: &str,
    enforce: bool,
    method: &str,
) -> Option<http::Response<Body>> {
    let TrafficDecision::Throttle { retry_after } = decision else {
        return None;
    };
    let retry_ms = u64::try_from(retry_after.as_millis()).unwrap_or(u64::MAX);
    counter.add(1, &throttle_attrs(profile_name, route, enforce));

    if enforce {
        tracing::debug!(rpc.method = %method, retry_after_ms = retry_ms, "traffic: request throttled");
        Some(throttle_response(retry_ms))
    } else {
        // Shadow mode: the cell was charged (so the metric is real), but admit the request.
        tracing::debug!(
            rpc.method = %method,
            retry_after_ms = retry_ms,
            "traffic: would throttle (shadow mode — admitted)"
        );
        None
    }
}

/// Resolves a `distributed` profile's decision via the global backend, applying the
/// `on_backend_error` policy when the backend is unreachable.
async fn distributed_check(
    profile: &TrafficProfile,
    key: &str,
    backend: Option<&Arc<dyn QuotaBackend>>,
) -> TrafficDecision {
    let Some(backend) = backend else {
        tracing::debug!("traffic: distributed profile but no backend wired — using local limiter");
        return profile.check(key);
    };

    match backend.check(key, profile.quota()).await {
        Ok(decision) => decision,
        Err(_unavailable) => match profile.on_backend_error() {
            // Reject: precision/safety over availability (hard abuse/billing quotas).
            Some(BackendError::FailClosed) => TrafficDecision::Throttle {
                retry_after: Duration::from_millis(profile.quota().lease_ms),
            },
            // Degrade to the local per-replica limiter (availability over precision).
            _ => profile.check(key),
        },
    }
}

/// Attribute set for the throttle counter. `status` distinguishes a real rejection from a
/// shadow-mode observation; `profile`/`route` scope it.
fn throttle_attrs(profile: &str, route: &str, enforce: bool) -> [KeyValue; 3] {
    [
        KeyValue::new("profile", profile.to_string()),
        KeyValue::new("route", route.to_string()),
        KeyValue::new("status", if enforce { "enforced" } else { "shadow" }),
    ]
}

/// Counts a per-method key fallback, and warns the first time per scope (a `per_ip`
/// fallback behind the ALB means the proxy-hop setting is wrong).
fn record_key_fallback(counter: &Counter<u64>, scope: Scope, route: &str) {
    let (label, warned) = match scope {
        Scope::PerIp => ("per_ip", &WARNED_IP_FALLBACK),
        _ => ("per_caller", &WARNED_CALLER_FALLBACK),
    };
    counter.add(1, &[KeyValue::new("scope", label), KeyValue::new("route", route.to_string())]);
    if !warned.swap(true, std::sync::atomic::Ordering::Relaxed) {
        tracing::warn!(
            scope = label,
            rpc.route = %route,
            "traffic: a {label} profile had no key to bucket on — every such caller shares the \
             method's bucket (logged once; see infra_traffic_key_fallback_total)"
        );
    }
}

/// Builds the rate-limit key for `method` under `scope`, and whether it fell back to
/// method-level keying.
///
/// `per_caller` reads the edge-mesh identity header; absent/non-ASCII/empty values degrade
/// to method-level keying (see module docs).
fn extract_key(
    scope: Scope,
    method: &str,
    headers: &HeaderMap,
    identity_header: &HeaderName,
    peer: Option<SocketAddr>,
    trusted_proxy_hops: usize,
) -> (String, bool) {
    match scope {
        Scope::PerIp => match client_ip(headers, peer, trusted_proxy_hops) {
            Some(ip) => (format!("{method}|ip:{ip}"), false),
            None => {
                tracing::debug!(rpc.method = %method, "traffic: per_ip profile but no client address — keying per-method");
                (method.to_owned(), true)
            }
        },
        Scope::PerMethod => (method.to_owned(), false),
        Scope::PerCaller => {
            match headers
                .get(identity_header)
                .and_then(|value| value.to_str().ok())
                .filter(|id| !id.is_empty())
            {
                Some(id) => (format!("{method}|{id}"), false),
                None => {
                    tracing::debug!(
                        rpc.method = %method,
                        identity_header = %identity_header,
                        "traffic: per_caller profile but no edge identity header — keying per-method"
                    );
                    (method.to_owned(), true)
                }
            }
        }
    }
}

/// A trailers-only gRPC `RESOURCE_EXHAUSTED` response carrying a `retry-after-ms` hint.
fn throttle_response(retry_ms: u64) -> http::Response<Body> {
    let mut status = Status::resource_exhausted("rate limit exceeded");
    if let Ok(value) = retry_ms.to_string().parse() {
        status.metadata_mut().insert("retry-after-ms", value);
    }
    status.into_http()
}

#[cfg(test)]
mod tests {
    use super::*;
    use opentelemetry::Value;

    fn has(attrs: &[KeyValue], key: &str, val: &str) -> bool {
        attrs
            .iter()
            .any(|kv| kv.key.as_str() == key && kv.value == Value::from(val.to_string()))
    }

    #[test]
    fn attrs_carry_profile_route_and_enforced_status() {
        let attrs = throttle_attrs("write-tight", "/post.PostService/CreatePost", true);
        assert!(has(&attrs, "profile", "write-tight"));
        assert!(has(&attrs, "route", "/post.PostService/CreatePost"));
        assert!(has(&attrs, "status", "enforced"));
    }

    #[test]
    fn keys_follow_the_scope() {
        let header = HeaderName::from_static(DEFAULT_IDENTITY_HEADER);
        let mut headers = HeaderMap::new();
        headers.insert(DEFAULT_IDENTITY_HEADER, "guest:42".parse().unwrap());
        headers.insert("x-forwarded-for", "6.6.6.6, 203.0.113.9".parse().unwrap());
        let peer: SocketAddr = "10.0.0.5:4000".parse().unwrap();
        let m = "/auth.v1.AuthService/StartGuestSession";

        assert_eq!(extract_key(Scope::PerMethod, m, &headers, &header, Some(peer), 1), (m.to_owned(), false));
        assert_eq!(
            extract_key(Scope::PerCaller, m, &headers, &header, Some(peer), 1),
            (format!("{m}|guest:42"), false)
        );
        assert_eq!(
            extract_key(Scope::PerIp, m, &headers, &header, Some(peer), 1),
            (format!("{m}|ip:203.0.113.9"), false)
        );
        // No forwarded header: the peer address.
        assert_eq!(
            extract_key(Scope::PerIp, m, &HeaderMap::new(), &header, Some(peer), 1),
            (format!("{m}|ip:10.0.0.5"), false)
        );
        // Nothing known: method-level, flagged as a fallback.
        assert_eq!(extract_key(Scope::PerIp, m, &HeaderMap::new(), &header, None, 1), (m.to_owned(), true));
        assert_eq!(extract_key(Scope::PerCaller, m, &HeaderMap::new(), &header, None, 1), (m.to_owned(), true));
    }

    #[test]
    fn attrs_distinguish_shadow_and_bounded_route() {
        let attrs = throttle_attrs("standard", UNBOUND_ROUTE, false);
        assert!(has(&attrs, "status", "shadow"));
        assert!(has(&attrs, "route", "<unbound>"));
    }
}
