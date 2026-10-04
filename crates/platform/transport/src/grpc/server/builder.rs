use std::sync::Arc;

use tower::layer::util::{Identity, Stack};
use tonic::transport::{Identity as TlsIdentity, Server, ServerTlsConfig};

use infra_config::TrafficRegistry;
use traffic::QuotaBackend;

use crate::{
    error::TransportError,
    grpc::{
        layer::{edge::EdgeLayer, inbound::InboundTraceLayer, traffic::TrafficLayer},
        server::config::GrpcServerConfig,
    },
};

/// Concrete server type produced by [`GrpcServerBuilder::build`].
///
/// The tonic type after applying [`InboundTraceLayer`], then [`EdgeLayer`], then
/// [`TrafficLayer`]: trace is the outer layer (so rejected and throttled requests are still
/// traced), the edge guard sits in the middle (so the identity it verifies is what
/// `per_caller` rate-limiting keys on), rate-limiting the inner. Both the [`EdgeLayer`]
/// and the [`TrafficLayer`] are always present in the type — each is a transparent
/// pass-through unless enabled via [`GrpcServerBuilder::with_edge`] /
/// [`GrpcServerBuilder::with_traffic`], keeping the return type stable.
pub type TracedGrpcServer =
    Server<Stack<TrafficLayer, Stack<EdgeLayer, Stack<InboundTraceLayer, Identity>>>>;

/// Builds a Tonic gRPC server with [`InboundTraceLayer`], [`EdgeLayer`] and [`TrafficLayer`]
/// pre-installed.
///
/// Every request has its W3C TraceContext extracted and linked as the parent span; if an
/// edge guard was supplied the listener is a client edge (allow-listed methods, edge-token
/// authentication); if a traffic registry was supplied, it is also rate-limited per the
/// bound `[traffic]` profile.
///
/// # Example
///
/// ```rust,ignore
/// let server = GrpcServerBuilder::new(GrpcServerConfig::default())
///     .with_traffic(infra.traffic().expect("[traffic] configured"))
///     .build()?
///     .add_service(PostServiceServer::new(my_handler))
///     .serve(config.addr)
///     .await?;
/// ```
pub struct GrpcServerBuilder {
    config: GrpcServerConfig,
    edge: Option<EdgeLayer>,
    traffic: Option<Arc<TrafficRegistry>>,
    traffic_backend: Option<Arc<dyn QuotaBackend>>,
}

impl GrpcServerBuilder {
    pub fn new(config: GrpcServerConfig) -> Self {
        Self { config, edge: None, traffic: None, traffic_backend: None }
    }

    /// Makes this server a **client edge**: only the guard's allow-listed methods are
    /// served, and (unless a rule is public) callers must present a valid edge token.
    /// Without this call the edge layer is a transparent pass-through (a mesh listener).
    pub fn with_edge(mut self, guard: Arc<crate::grpc::layer::edge::EdgeGuard>) -> Self {
        self.edge = Some(EdgeLayer::new(guard, self.config.identity_header.clone()));
        self
    }

    /// Enables ingress rate limiting from the given registry. Without this call the server
    /// installs a transparent (no-op) traffic layer.
    pub fn with_traffic(mut self, registry: Arc<TrafficRegistry>) -> Self {
        self.traffic = Some(registry);
        self
    }

    /// Attaches the distributed-mode coordination backend (e.g. `traffic-redis`). Only
    /// `distributed` profiles use it; without it they degrade to local per-replica limiting.
    /// No effect unless [`with_traffic`](Self::with_traffic) is also set.
    pub fn with_traffic_backend(mut self, backend: Arc<dyn QuotaBackend>) -> Self {
        self.traffic_backend = Some(backend);
        self
    }

    /// Returns a [`TracedGrpcServer`] with the trace and traffic layers applied.
    ///
    /// Call `.add_service(...)` and `.serve(addr)` on the returned server to start
    /// accepting connections.
    pub fn build(self) -> Result<TracedGrpcServer, TransportError> {
        let traffic_layer = match self.traffic {
            Some(registry) => {
                let layer = TrafficLayer::new(registry, self.config.identity_header.clone())
                    .for_edge(self.edge.is_some())
                    .with_trusted_proxy_hops(self.config.trusted_proxy_hops);
                match self.traffic_backend {
                    Some(backend) => layer.with_backend(backend),
                    None => layer,
                }
            }
            None => TrafficLayer::disabled(),
        };
        let edge_layer = self.edge.unwrap_or_else(EdgeLayer::disabled);
        // `.layer(InboundTraceLayer)` first makes trace the outer layer; `.layer(edge)` nests
        // the edge guard inside the span; `.layer(traffic)` nests rate-limiting innermost so
        // `per_caller` keys see the identity header the edge guard wrote.
        let mut server = Server::builder()
            .layer(InboundTraceLayer)
            .layer(edge_layer)
            .layer(traffic_layer);

        if let Some(age) = self.config.max_connection_age {
            server = server.max_connection_age(age);
        }
        if let Some(grace) = self.config.max_connection_age_grace {
            server = server.max_connection_age_grace(grace);
        }

        if let Some(tls) = self.config.tls {
            let identity = TlsIdentity::from_pem(&tls.cert_pem, &tls.key_pem);
            let mut tls_config = ServerTlsConfig::new().identity(identity);

            if let Some(ca) = tls.client_ca_pem {
                tls_config = tls_config
                    .client_ca_root(tonic::transport::Certificate::from_pem(ca));
            }

            server = server.tls_config(tls_config).map_err(|e| {
                TransportError::Grpc(crate::grpc::error::GrpcTransportError::Tls(e.to_string()))
            })?;
        }

        Ok(server)
    }
}
