use std::net::SocketAddr;
use std::time::Duration;

use http::header::HeaderName;

/// Default inbound header carrying the caller identity, injected by the edge/service mesh.
///
/// **Trust contract:** this header is only safe to key on if the mesh *sets or overwrites*
/// it on every request and *strips* any client-supplied value at the trust boundary. Inside
/// the cluster it is authoritative; never accept it from untrusted ingress.
pub const DEFAULT_IDENTITY_HEADER: &str = "x-edge-user";

/// Configuration for a Tonic gRPC server.
#[derive(Debug, Clone)]
pub struct GrpcServerConfig {
    /// Address to bind. Defaults to `0.0.0.0:50051`.
    pub addr: SocketAddr,

    /// Optional TLS settings. When `None` the server accepts plaintext connections.
    pub tls: Option<GrpcServerTlsConfig>,

    /// Enables gRPC server reflection (useful for `grpcurl` and Postman during development).
    pub enable_reflection: bool,

    /// Inbound header the edge mesh injects with the caller identity. Read by the traffic
    /// layer for `per_caller` rate-limit keying. Defaults to [`DEFAULT_IDENTITY_HEADER`].
    pub identity_header: HeaderName,

    /// Maximum lifetime of an accepted connection. At the deadline the server GOAWAYs it:
    /// in-flight streams keep running (never severed unless [`max_connection_age_grace`]
    /// is also set) but the caller's *next* stream goes over a fresh connection — which
    /// re-traverses the ClusterIP and lands on a current backend. This is what re-balances
    /// long-lived HTTP/2 channels after a scale-out; without it a tonic channel stays
    /// pinned to one pod for its whole life. `None` (default) = connections live forever.
    ///
    /// [`max_connection_age_grace`]: Self::max_connection_age_grace
    pub max_connection_age: Option<Duration>,

    /// How long in-flight streams may continue after [`max_connection_age`] before the
    /// connection is forcibly closed. `None` (default) = never force-close, so long-lived
    /// server streams (chat/notification) outlive the GOAWAY untouched. Only set this on
    /// servers whose streams are safe to sever.
    ///
    /// [`max_connection_age`]: Self::max_connection_age
    pub max_connection_age_grace: Option<Duration>,

    /// Proxies in front of this listener that append to `X-Forwarded-For` (the ALB: 1).
    /// Read by the traffic layer for `per_ip` keying; see [`crate::grpc::client_ip`].
    pub trusted_proxy_hops: usize,
}

impl Default for GrpcServerConfig {
    fn default() -> Self {
        Self {
            addr: "0.0.0.0:50051".parse().expect("static address is valid"),
            tls: None,
            enable_reflection: false,
            identity_header: HeaderName::from_static(DEFAULT_IDENTITY_HEADER),
            max_connection_age: None,
            max_connection_age_grace: None,
            trusted_proxy_hops: crate::grpc::client_ip::DEFAULT_TRUSTED_PROXY_HOPS,
        }
    }
}

impl GrpcServerConfig {
    pub fn with_addr(mut self, addr: SocketAddr) -> Self {
        self.addr = addr;
        self
    }

    pub fn with_tls(mut self, tls: GrpcServerTlsConfig) -> Self {
        self.tls = Some(tls);
        self
    }

    pub fn with_reflection(mut self) -> Self {
        self.enable_reflection = true;
        self
    }

    /// Overrides the edge-mesh identity header used for `per_caller` keying.
    pub fn with_identity_header(mut self, header: HeaderName) -> Self {
        self.identity_header = header;
        self
    }

    /// Bounds connection lifetime (GOAWAY-based recycling; see the field docs).
    pub fn with_max_connection_age(mut self, age: Duration) -> Self {
        self.max_connection_age = Some(age);
        self
    }

    /// Force-closes connections whose streams outlive the age deadline by `grace`.
    pub fn with_max_connection_age_grace(mut self, grace: Duration) -> Self {
        self.max_connection_age_grace = Some(grace);
        self
    }

    /// Sets how many proxies append to `X-Forwarded-For` before this listener.
    pub fn with_trusted_proxy_hops(mut self, hops: usize) -> Self {
        self.trusted_proxy_hops = hops;
        self
    }
}

/// TLS settings for a gRPC server.
#[derive(Debug, Clone)]
pub struct GrpcServerTlsConfig {
    /// PEM-encoded server certificate.
    pub cert_pem: Vec<u8>,
    /// PEM-encoded server private key.
    pub key_pem: Vec<u8>,
    /// Optional PEM-encoded CA for client certificate verification (mTLS).
    pub client_ca_pem: Option<Vec<u8>>,
}
