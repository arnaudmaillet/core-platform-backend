//! **Mesh caller identity** (#852): which service is calling a mesh-only RPC.
//!
//! The NetworkPolicy is L4: it lets a service reach a port, never a single RPC.
//! So a sensitive mesh RPC (gems spent, someone's reports, a wallet export)
//! also checks **who** calls it. The caller sends its Kubernetes projected
//! ServiceAccount token as [`MESH_TOKEN_HEADER`] ([`MeshTokenInterceptor`]);
//! the callee verifies it and requires one of the RPC's intended callers
//! ([`MeshCallerGate::require`]).
//!
//! Rolled out in three modes ([`MeshGateMode`]): `off` (the default until the
//! infra provides the tokens — nothing checked), `log` (a wrong or missing
//! caller is logged, the call goes through), `enforce` (refused).

use std::path::PathBuf;
use std::sync::{Arc, RwLock};
use std::time::{Duration, Instant};

use auth_context::mesh::{MeshCaller, MeshDecoder};
use futures::future::BoxFuture;
use tonic::metadata::MetadataValue;
use tonic::service::Interceptor;
use tonic::{Request, Status};

/// The gRPC metadata carrying the caller's mesh token.
pub const MESH_TOKEN_HEADER: &str = "x-mesh-token";

/// How strictly a callee checks its callers.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MeshGateMode {
    /// Nothing checked (until the tokens are deployed).
    Off,
    /// A wrong or missing caller is logged; the call goes through.
    Log,
    /// A wrong or missing caller is refused.
    Enforce,
}

impl MeshGateMode {
    /// `off` / `log` / `enforce` (case-insensitive); anything else is `None`.
    pub fn parse(value: &str) -> Option<Self> {
        match value.trim().to_ascii_lowercase().as_str() {
            "off" => Some(Self::Off),
            "log" => Some(Self::Log),
            "enforce" => Some(Self::Enforce),
            _ => None,
        }
    }
}

/// A mesh token check in flight: the caller, or why it was refused.
pub type MeshVerification<'a> = BoxFuture<'a, Result<MeshCaller, String>>;

/// Turns a mesh token into its caller.
pub trait MeshVerifier: Send + Sync + 'static {
    fn verify<'a>(&'a self, token: &'a str) -> MeshVerification<'a>;
}

impl MeshVerifier for MeshDecoder {
    fn verify<'a>(&'a self, token: &'a str) -> MeshVerification<'a> {
        // The token is never logged; only why it failed.
        Box::pin(async move { self.decode(token).await.map_err(|e| e.to_string()) })
    }
}

/// The callee's check of its mesh callers.
#[derive(Clone)]
pub struct MeshCallerGate {
    mode:      MeshGateMode,
    verifier:  Option<Arc<dyn MeshVerifier>>,
    /// When set, the caller must be in this namespace.
    namespace: Option<String>,
}

impl MeshCallerGate {
    /// `verifier: None` with `Log` / `Enforce`: every caller is unknown —
    /// logged, or refused (fail closed).
    pub fn new(mode: MeshGateMode, verifier: Option<Arc<dyn MeshVerifier>>, namespace: Option<String>) -> Self {
        Self { mode, verifier, namespace }
    }

    /// Nothing checked.
    pub fn off() -> Self {
        Self::new(MeshGateMode::Off, None, None)
    }

    pub fn mode(&self) -> MeshGateMode {
        self.mode
    }

    /// Requires `request` to come from one of `allowed` (app names, e.g.
    /// `account-server`; an overlay's name prefix — `staging-account-server` —
    /// matches too). `rpc` names the call in the log.
    pub async fn require<T>(&self, request: &Request<T>, rpc: &str, allowed: &[&str]) -> Result<(), Status> {
        if self.mode == MeshGateMode::Off {
            return Ok(());
        }
        match self.check(request, allowed).await {
            Ok(()) => Ok(()),
            Err(reason) if self.mode == MeshGateMode::Log => {
                tracing::warn!(rpc, %reason, ?allowed, "mesh caller refused (log mode: let through)");
                Ok(())
            }
            Err(reason) => {
                tracing::warn!(rpc, %reason, ?allowed, "mesh caller refused");
                Err(Status::permission_denied("this service may not call this RPC"))
            }
        }
    }

    async fn check<T>(&self, request: &Request<T>, allowed: &[&str]) -> Result<(), String> {
        let verifier = self.verifier.as_ref().ok_or("no mesh token verifier configured")?;
        let token = request
            .metadata()
            .get(MESH_TOKEN_HEADER)
            .and_then(|v| v.to_str().ok())
            .map(str::trim)
            .filter(|t| !t.is_empty())
            .ok_or("no mesh token")?;
        let caller = verifier.verify(token).await?;
        if let Some(namespace) = &self.namespace
            && &caller.namespace != namespace
        {
            return Err(format!("caller in namespace {:?}", caller.namespace));
        }
        if allowed.iter().any(|app| names(&caller.service_account, app)) {
            Ok(())
        } else {
            Err(format!("caller {:?}", caller.service_account))
        }
    }
}

/// Whether ServiceAccount `account` is app `app`: the same name, or it with
/// an overlay's name prefix (`staging-account-server`).
fn names(account: &str, app: &str) -> bool {
    account == app || account.strip_suffix(app).is_some_and(|prefix| prefix.ends_with('-') && prefix.len() > 1)
}

/// The caller's side: attaches its mesh token to every request of a client
/// (`ServiceClient::with_interceptor(channel, interceptor)`). The token file
/// is re-read at most once a minute (the kubelet rotates it). Without a file
/// configured, nothing is attached.
#[derive(Clone, Default)]
pub struct MeshTokenInterceptor {
    source: Option<Arc<TokenFile>>,
}

struct TokenFile {
    path:   PathBuf,
    /// The last token read (if any) and when the file was last tried.
    cached: RwLock<(Option<MetadataValue<tonic::metadata::Ascii>>, Option<Instant>)>,
}

/// How long a read token is reused.
const TOKEN_REREAD: Duration = Duration::from_secs(60);

impl MeshTokenInterceptor {
    /// From the projected token at `path`.
    pub fn from_file(path: impl Into<PathBuf>) -> Self {
        Self { source: Some(Arc::new(TokenFile { path: path.into(), cached: RwLock::new((None, None)) })) }
    }

    /// From `MESH_TOKEN_FILE`; nothing attached when it is unset.
    pub fn from_env() -> Self {
        match std::env::var("MESH_TOKEN_FILE").ok().filter(|v| !v.trim().is_empty()) {
            Some(path) => Self::from_file(path),
            None => Self::default(),
        }
    }

    /// The token to send: re-read at most once per [`TOKEN_REREAD`]. A failed
    /// read keeps the last good token (still valid for up to its lifetime)
    /// and is retried, and logged, once per window — never on every call.
    fn token(&self) -> Option<MetadataValue<tonic::metadata::Ascii>> {
        let source = self.source.as_ref()?;
        if let Ok(cached) = source.cached.read()
            && cached.1.is_some_and(|tried| tried.elapsed() < TOKEN_REREAD)
        {
            return cached.0.clone();
        }
        let read = std::fs::read_to_string(&source.path)
            .map_err(|e| e.to_string())
            .and_then(|token| MetadataValue::try_from(token.trim()).map_err(|e| e.to_string()));
        let mut cached = source.cached.write().ok()?;
        cached.1 = Some(Instant::now());
        match read {
            Ok(value) => cached.0 = Some(value),
            Err(error) => {
                tracing::warn!(%error, path = %source.path.display(), kept = cached.0.is_some(), "mesh token unreadable");
            }
        }
        cached.0.clone()
    }
}

impl Interceptor for MeshTokenInterceptor {
    fn call(&mut self, mut request: Request<()>) -> Result<Request<()>, Status> {
        if let Some(token) = self.token() {
            request.metadata_mut().insert(MESH_TOKEN_HEADER, token);
        }
        Ok(request)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Tokens are the caller's `namespace/name`.
    struct Plain;

    impl MeshVerifier for Plain {
        fn verify<'a>(&'a self, token: &'a str) -> MeshVerification<'a> {
            Box::pin(async move {
                let (namespace, name) = token.split_once('/').ok_or_else(|| "bad token".to_owned())?;
                Ok(MeshCaller { namespace: namespace.into(), service_account: name.into() })
            })
        }
    }

    fn call(token: Option<&str>) -> Request<()> {
        let mut request = Request::new(());
        if let Some(token) = token {
            request.metadata_mut().insert(MESH_TOKEN_HEADER, token.parse().unwrap());
        }
        request
    }

    fn gate(mode: MeshGateMode) -> MeshCallerGate {
        MeshCallerGate::new(mode, Some(Arc::new(Plain)), Some("core".into()))
    }

    #[tokio::test]
    async fn enforce_lets_the_intended_callers_through_only() {
        let g = gate(MeshGateMode::Enforce);
        let allowed = ["geo-discovery-server"];
        assert!(g.require(&call(Some("core/geo-discovery-server")), "SpendGems", &allowed).await.is_ok());
        assert!(g.require(&call(Some("core/staging-geo-discovery-server")), "SpendGems", &allowed).await.is_ok());
        for refused in [Some("core/account-server"), Some("other/geo-discovery-server"), Some("core/geo-discovery-server-x"), Some("core/xgeo-discovery-server"), Some("garbage"), None] {
            let status = g.require(&call(refused), "SpendGems", &allowed).await.unwrap_err();
            assert_eq!(status.code(), tonic::Code::PermissionDenied, "{refused:?}");
        }
    }

    #[tokio::test]
    async fn log_and_off_let_everyone_through_and_no_verifier_fails_closed() {
        assert!(gate(MeshGateMode::Log).require(&call(None), "SpendGems", &["geo-discovery-server"]).await.is_ok());
        assert!(MeshCallerGate::off().require(&call(None), "SpendGems", &["geo-discovery-server"]).await.is_ok());
        let blind = MeshCallerGate::new(MeshGateMode::Enforce, None, None);
        assert!(blind.require(&call(Some("core/geo-discovery-server")), "SpendGems", &["geo-discovery-server"]).await.is_err());
        assert_eq!(MeshGateMode::parse(" Enforce "), Some(MeshGateMode::Enforce));
        assert_eq!(MeshGateMode::parse("strict"), None);
    }

    #[test]
    fn the_interceptor_attaches_the_token_it_reads() {
        let dir = std::env::temp_dir().join(format!("mesh-token-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("token");
        std::fs::write(&path, "the-token\n").unwrap();
        let mut interceptor = MeshTokenInterceptor::from_file(&path);
        let request = interceptor.call(Request::new(())).unwrap();
        assert_eq!(request.metadata().get(MESH_TOKEN_HEADER).unwrap(), "the-token");
        let mut none = MeshTokenInterceptor::default();
        assert!(none.call(Request::new(())).unwrap().metadata().get(MESH_TOKEN_HEADER).is_none());
        // The file goes (a rotation hiccup): the last good token is kept.
        std::fs::remove_dir_all(&dir).ok();
        if let Some(source) = &interceptor.source {
            source.cached.write().unwrap().1 = None;
        }
        let request = interceptor.call(Request::new(())).unwrap();
        assert_eq!(request.metadata().get(MESH_TOKEN_HEADER).unwrap(), "the-token", "kept");
        let mut missing = MeshTokenInterceptor::from_file(dir.join("never"));
        assert!(missing.call(Request::new(())).unwrap().metadata().get(MESH_TOKEN_HEADER).is_none());
    }
}
