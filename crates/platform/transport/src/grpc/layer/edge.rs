//! The client-edge Tower layer: method allow-list + edge-token authentication.
//!
//! Installed on the **edge** listener via [`crate::grpc::server::GrpcServerBuilder`],
//! between the trace layer (outside, so rejections are traced) and the traffic layer
//! (inside, so `per_caller` rate-limit keys see the identity this layer sets). On
//! the mesh listener the layer is [`EdgeLayer::disabled`] — a transparent
//! pass-through that keeps the server type identical.
//!
//! # Per request
//!
//! 1. The edge-identity header (`x-edge-user`) is **stripped** — a client can never
//!    smuggle one in; only this layer writes it, from a verified token.
//! 2. `grpc.health.v1.Health` always passes (the load balancer's health check).
//! 3. The method is looked up in the service's [`EdgePolicy`]. Absent ⇒
//!    `UNIMPLEMENTED`: the edge does not implement what it does not expose.
//! 4. [`EdgeAccess::Public`] ⇒ forwarded with no principal.
//! 5. Otherwise the `authorization: Bearer <token>` metadata is verified against
//!    the `auth` JWKS. Missing / malformed / expired / wrong issuer or audience ⇒
//!    `UNAUTHENTICATED` (the reason is never revealed beyond "expired" vs the
//!    rest, and nothing token-bearing is logged). [`EdgeAccess::Permission`] adds a
//!    `PERMISSION_DENIED` gate on the `perms` claim.
//! 6. On success the identity header is set to the token subject, an
//!    [`EdgePrincipal`] extension is attached, and the handler runs inside the
//!    `auth-context` task-local principal scope.
//!
//! # Observability
//!
//! Every rejection increments `infra_edge_rejected` (Prometheus:
//! `infra_edge_rejected_total`) labelled by `route` (policy-known methods only —
//! unknown paths collapse to `<unlisted>` to bound cardinality) and `reason`
//! (`unlisted` | `missing_token` | `invalid_token` | `expired_token` | `forbidden`).

use std::collections::HashMap;
use std::sync::Arc;
use std::task::{Context, Poll};

use auth_context::{with_principal, AuthError, EdgeDecoder};
use futures::future::BoxFuture;
use http::header::{HeaderName, HeaderValue, AUTHORIZATION};
use opentelemetry::{global, metrics::Counter, KeyValue};
use tonic::{body::Body, Status};
use tower::{Layer, Service};

use crate::grpc::edge::{EdgeAccess, EdgeAnonymous, EdgePolicy, EdgePrincipal};
use crate::grpc::server::config::DEFAULT_IDENTITY_HEADER;

/// Instrument name; the Prometheus exporter appends `_total`.
const REJECT_METRIC: &str = "infra_edge_rejected";
/// Route label for methods outside the policy — bounds metric cardinality.
const UNLISTED_ROUTE: &str = "<unlisted>";
/// The gRPC health service is always reachable on the edge (LB health checks).
const HEALTH_PREFIX: &str = "/grpc.health.v1.Health/";

fn reject_counter() -> Counter<u64> {
    global::meter("transport")
        .u64_counter(REJECT_METRIC)
        .with_description(
            "Requests rejected at the client edge, labelled by route and reason \
             (unlisted|missing_token|invalid_token|expired_token|forbidden).",
        )
        .build()
}

/// What the edge layer enforces: the verifier and the method allow-list.
pub struct EdgeGuard {
    decoder: Arc<EdgeDecoder>,
    policy: HashMap<&'static str, EdgeAccess>,
}

impl EdgeGuard {
    /// Builds the guard from the service's policy (validate it first with
    /// [`crate::grpc::edge::validate_policy`]).
    pub fn new(decoder: Arc<EdgeDecoder>, policy: EdgePolicy) -> Self {
        Self {
            decoder,
            policy: policy.iter().map(|r| (r.method, r.access)).collect(),
        }
    }

    /// Number of exposed methods.
    pub fn exposed_methods(&self) -> usize {
        self.policy.len()
    }
}

/// Tower [`Layer`] guarding the client-edge listener. `disabled()` is a transparent
/// pass-through for the mesh listener.
#[derive(Clone)]
pub struct EdgeLayer {
    guard: Option<Arc<EdgeGuard>>,
    counter: Counter<u64>,
    identity_header: HeaderName,
}

impl EdgeLayer {
    /// A pass-through layer (mesh listener).
    pub fn disabled() -> Self {
        Self {
            guard: None,
            counter: reject_counter(),
            identity_header: HeaderName::from_static(DEFAULT_IDENTITY_HEADER),
        }
    }

    /// An enforcing layer that writes the verified subject into `identity_header`
    /// (the header the traffic layer keys `per_caller` limits on).
    pub fn new(guard: Arc<EdgeGuard>, identity_header: HeaderName) -> Self {
        Self {
            guard: Some(guard),
            counter: reject_counter(),
            identity_header,
        }
    }

    pub fn is_enabled(&self) -> bool {
        self.guard.is_some()
    }
}

impl<S> Layer<S> for EdgeLayer {
    type Service = EdgeService<S>;

    fn layer(&self, inner: S) -> Self::Service {
        EdgeService {
            inner,
            guard: self.guard.clone(),
            counter: self.counter.clone(),
            identity_header: self.identity_header.clone(),
        }
    }
}

/// The concrete service produced by [`EdgeLayer`].
#[derive(Clone)]
pub struct EdgeService<S> {
    inner: S,
    guard: Option<Arc<EdgeGuard>>,
    counter: Counter<u64>,
    identity_header: HeaderName,
}

impl<S> Service<http::Request<Body>> for EdgeService<S>
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

    fn call(&mut self, mut req: http::Request<Body>) -> Self::Future {
        let Some(guard) = self.guard.as_ref() else {
            return Box::pin(self.inner.call(req));
        };

        // 1. Never trust a client-supplied identity header.
        req.headers_mut().remove(&self.identity_header);

        let method = req.uri().path().to_owned();

        // 2. The LB health check.
        if method.starts_with(HEALTH_PREFIX) {
            return Box::pin(self.inner.call(req));
        }

        // 3. The allow-list.
        let Some(access) = guard.policy.get(method.as_str()).copied() else {
            self.counter.add(1, &reject_attrs(UNLISTED_ROUTE, "unlisted"));
            tracing::debug!(rpc.method = %method, "edge: method not exposed");
            let response = Status::unimplemented("method is not exposed at the client edge").into_http();
            return Box::pin(async move { Ok(response) });
        };

        // 4. Public methods carry no principal, only the marker that tells the
        //    handler this is an anonymous client rather than a mesh peer.
        if access == EdgeAccess::Public {
            req.extensions_mut().insert(EdgeAnonymous);
            return Box::pin(self.inner.call(req));
        }

        // 5. Bearer token required from here on.
        let Some(token) = bearer_token(req.headers()) else {
            self.counter.add(1, &reject_attrs(&method, "missing_token"));
            tracing::debug!(rpc.method = %method, "edge: missing bearer token");
            let response = Status::unauthenticated("missing bearer token").into_http();
            return Box::pin(async move { Ok(response) });
        };

        // Verification is async (the JWKS cache is behind an async RwLock): move a
        // ready clone of the inner service into the future (tower readiness idiom).
        let guard = Arc::clone(guard);
        let counter = self.counter.clone();
        let identity_header = self.identity_header.clone();
        let clone = self.inner.clone();
        let mut inner = std::mem::replace(&mut self.inner, clone);

        Box::pin(async move {
            let principal = match guard.decoder.decode(&token).await {
                Ok(p) => p,
                Err(error) => {
                    let (reason, status) = match error {
                        AuthError::TokenExpired => {
                            ("expired_token", Status::unauthenticated("token expired"))
                        }
                        _ => ("invalid_token", Status::unauthenticated("invalid token")),
                    };
                    counter.add(1, &reject_attrs(&method, reason));
                    // The error kind is safe to log; the token never is.
                    tracing::debug!(rpc.method = %method, %error, "edge: token rejected");
                    return Ok(status.into_http());
                }
            };

            if let EdgeAccess::Permission(required) = access
                && !principal.has_permission(required)
            {
                counter.add(1, &reject_attrs(&method, "forbidden"));
                tracing::debug!(
                    rpc.method = %method,
                    principal.user_id = %principal.user_id,
                    required,
                    "edge: missing permission"
                );
                return Ok(Status::permission_denied("missing permission").into_http());
            }

            // 6. Attach the verified identity: header (rate-limit keying), extension
            //    (handlers), task-local (ambient auth-context API).
            if let Ok(value) = HeaderValue::from_str(principal.user_id.as_str()) {
                req.headers_mut().insert(identity_header, value);
            }
            let principal = Arc::new(principal);
            req.extensions_mut().insert(EdgePrincipal::new(Arc::clone(&principal)));
            with_principal(principal, inner.call(req)).await
        })
    }
}

/// Extracts the bearer token from `authorization` (scheme is case-insensitive;
/// empty tokens count as absent).
fn bearer_token(headers: &http::HeaderMap) -> Option<String> {
    let value = headers.get(AUTHORIZATION)?.to_str().ok()?;
    let (scheme, token) = value.split_once(' ')?;
    if !scheme.eq_ignore_ascii_case("bearer") {
        return None;
    }
    let token = token.trim();
    (!token.is_empty()).then(|| token.to_owned())
}

fn reject_attrs(route: &str, reason: &'static str) -> [KeyValue; 2] {
    [
        KeyValue::new("route", route.to_string()),
        KeyValue::new("reason", reason),
    ]
}

#[cfg(test)]
mod tests {
    use super::*;

    fn headers(value: &str) -> http::HeaderMap {
        let mut h = http::HeaderMap::new();
        h.insert(AUTHORIZATION, value.parse().unwrap());
        h
    }

    #[test]
    fn bearer_token_parses_the_scheme_case_insensitively() {
        assert_eq!(bearer_token(&headers("Bearer abc")).as_deref(), Some("abc"));
        assert_eq!(bearer_token(&headers("bearer  abc ")).as_deref(), Some("abc"));
    }

    #[test]
    fn bearer_token_rejects_other_schemes_and_empty_tokens() {
        assert!(bearer_token(&headers("Basic abc")).is_none());
        assert!(bearer_token(&headers("Bearer ")).is_none());
        assert!(bearer_token(&headers("abc")).is_none());
        assert!(bearer_token(&http::HeaderMap::new()).is_none());
    }
}
