//! The **client edge**: the authenticated, allow-listed gRPC listener a service
//! exposes to the public load balancer.
//!
//! A fleet service has two listeners (see `service-runtime`):
//!
//! * the **mesh** listener (`<SVC>_GRPC_ADDR`) — in-cluster callers only, guarded by
//!   NetworkPolicy, no token check (today's posture, unchanged);
//! * the **edge** listener (`GRPC_EDGE_ADDR`) — what the ALB targets. Every request
//!   goes through [`EdgeLayer`](crate::grpc::layer::EdgeLayer): the method must be
//!   declared in the service's [`EdgePolicy`] (anything else is `UNIMPLEMENTED`),
//!   and unless the rule is [`EdgeAccess::Public`] a valid ES256 edge token must be
//!   presented. The verified caller is then attached to the request as an
//!   [`EdgePrincipal`] extension (and as the `auth-context` task-local principal).
//!
//! # Identity comes from the token, not the request
//!
//! Client-facing contracts carry the acting identity as a request field
//! (`profile_id`, `sender_id`, `owner_id`, …). On the edge that field is a *claim*
//! the caller makes; the handler must bind it to the verified token with
//! [`require_account`] (the field is an account id: must equal `sub`) or
//! [`require_profile`] (the field is a profile id: must be one the account owns,
//! i.e. in the `pids` claim). On the mesh listener there is no principal and both
//! helpers are no-ops, so in-cluster callers keep working unchanged. On the edge, a
//! [`EdgeAccess::Public`] method carries no principal either, and both helpers
//! refuse it: a public method has no identity to bind a field to.
//!
//! # Who is reading
//!
//! Reads that depend on the caller (drafts only for their author, private
//! profiles only for followers) take the reader from [`viewer`], never from a
//! request field: [`Viewer::Internal`] for a trusted mesh caller (unfiltered),
//! [`Viewer::Anonymous`] for an edge caller with no identity, and
//! [`Viewer::Member`] for a verified caller and the profiles its account owns.

use std::sync::Arc;

use auth_context::{edge, CurrentPrincipal, OidcClaims};
use tonic::Status;

/// What a request to an edge-exposed method must present.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EdgeAccess {
    /// No token required. Reserved for the RPCs that *produce* a session (login,
    /// refresh); still rate-limited per method.
    Public,
    /// A valid **member** edge token is required; the verified principal is
    /// attached. A guest token is refused (`PERMISSION_DENIED`): guests only
    /// reach `Permission` rules (`read:public`).
    Authenticated,
    /// A valid edge token carrying this permission (an `auth` `perms` entry) is
    /// required.
    Permission(&'static str),
}

/// One edge-exposed RPC: the full gRPC method path (`/<package.Service>/<Method>`)
/// and the access it requires.
#[derive(Debug, Clone, Copy)]
pub struct EdgeRule {
    pub method: &'static str,
    pub access: EdgeAccess,
}

/// A service's edge allow-list. Methods absent from it are **not exposed** on the
/// edge listener — internal RPCs, staff consoles and anything not yet reviewed stay
/// mesh-only by construction.
pub type EdgePolicy = &'static [EdgeRule];

/// An [`EdgeAccess::Public`] rule.
pub const fn public(method: &'static str) -> EdgeRule {
    EdgeRule { method, access: EdgeAccess::Public }
}

/// An [`EdgeAccess::Authenticated`] rule.
pub const fn authenticated(method: &'static str) -> EdgeRule {
    EdgeRule { method, access: EdgeAccess::Authenticated }
}

/// An [`EdgeAccess::Permission`] rule.
pub const fn permission(method: &'static str, permission: &'static str) -> EdgeRule {
    EdgeRule { method, access: EdgeAccess::Permission(permission) }
}

/// Validates an [`EdgePolicy`]: every method is a `/<service>/<method>` path and
/// no method is declared twice. Run at boot so a typo fails the pod, not a client.
pub fn validate_policy(policy: &[EdgeRule]) -> Result<(), String> {
    let mut seen = std::collections::HashSet::with_capacity(policy.len());
    for rule in policy {
        let m = rule.method;
        let well_formed = m.starts_with('/')
            && m.matches('/').count() == 2
            && m.split('/').skip(1).all(|part| !part.is_empty() && !part.contains(char::is_whitespace));
        if !well_formed {
            return Err(format!("edge policy: `{m}` is not a `/<package.Service>/<Method>` path"));
        }
        if !seen.insert(m) {
            return Err(format!("edge policy: `{m}` is declared twice"));
        }
    }
    Ok(())
}

/// The verified caller of an edge request. Attached by the edge layer as a request
/// extension; read it with [`principal`].
#[derive(Clone)]
pub struct EdgePrincipal(Arc<CurrentPrincipal<OidcClaims>>);

impl EdgePrincipal {
    pub fn new(principal: Arc<CurrentPrincipal<OidcClaims>>) -> Self {
        Self(principal)
    }

    /// The token subject — the caller's **account** id.
    pub fn account_id(&self) -> &str {
        self.0.user_id.as_str()
    }

    /// The caller's session id (`sid` claim), when the token carries one.
    pub fn session_id(&self) -> Option<&str> {
        edge::session_id(&self.0.raw_claims)
    }

    /// The profile ids the caller's account owns (`pids` claim).
    pub fn profile_ids(&self) -> impl Iterator<Item = &str> {
        edge::profile_ids(&self.0.raw_claims)
    }

    /// `true` when `profile_id` is one of the caller's profiles.
    pub fn owns_profile(&self, profile_id: &str) -> bool {
        self.profile_ids().any(|p| p == profile_id)
    }

    /// `true` for a guest token: an anonymous installation that may only read
    /// public content (`read:public`), never act.
    pub fn is_guest(&self) -> bool {
        edge::is_guest(&self.0.raw_claims)
    }

    /// `true` when the token carries `permission`.
    pub fn has_permission(&self, permission: &str) -> bool {
        self.0.has_permission(permission)
    }

    /// The underlying verified principal (raw claims included).
    pub fn inner(&self) -> &CurrentPrincipal<OidcClaims> {
        &self.0
    }

    pub fn into_inner(self) -> Arc<CurrentPrincipal<OidcClaims>> {
        self.0
    }
}

impl std::fmt::Debug for EdgePrincipal {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("EdgePrincipal")
            .field("account_id", &self.account_id())
            .finish_non_exhaustive()
    }
}

/// Marks a request that reached an [`EdgeAccess::Public`] method on the edge
/// listener: it came from a client, but with no verified identity. Without the
/// marker it would be indistinguishable from a trusted mesh call.
#[derive(Debug, Clone, Copy)]
pub struct EdgeAnonymous;

/// The verified edge caller, or `None` when the request came in over the mesh
/// listener (or hit a [`EdgeAccess::Public`] method).
pub fn principal<T>(request: &tonic::Request<T>) -> Option<&EdgePrincipal> {
    request.extensions().get::<EdgePrincipal>()
}

/// The reader of a viewer-aware read, taken from how the request arrived.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Viewer {
    /// A mesh caller: an in-cluster service, trusted like the network it sits
    /// on. Sees everything, as before viewer-aware reads existed.
    Internal,
    /// An edge caller with no verified identity (an [`EdgeAccess::Public`]
    /// method). Sees only what is public.
    Anonymous,
    /// A verified edge caller: its account and the profiles it owns (`pids`).
    Member { account_id: String, profile_ids: Vec<String> },
}

impl Viewer {
    /// `true` when `profile_id` is one of the viewer's own profiles. An internal
    /// caller owns no profile; check [`Viewer::Internal`] first where it should
    /// see everything.
    pub fn owns_profile(&self, profile_id: &str) -> bool {
        match self {
            Self::Member { profile_ids, .. } => profile_ids.iter().any(|p| p == profile_id),
            Self::Internal | Self::Anonymous => false,
        }
    }
}

/// Who is making `request`: see [`Viewer`].
pub fn viewer<T>(request: &tonic::Request<T>) -> Viewer {
    if let Some(p) = principal(request) {
        return Viewer::Member {
            account_id: p.account_id().to_owned(),
            profile_ids: p.profile_ids().map(str::to_owned).collect(),
        };
    }
    if request.extensions().get::<EdgeAnonymous>().is_some() {
        return Viewer::Anonymous;
    }
    Viewer::Internal
}

/// The error for an actor binding on an edge request that has no identity.
fn anonymous_actor() -> Status {
    Status::unauthenticated("this call needs an authenticated caller")
}

fn is_anonymous_edge<T>(request: &tonic::Request<T>) -> bool {
    request.extensions().get::<EdgeAnonymous>().is_some()
}

/// Binds a request's **account**-id actor field to the verified caller: on the
/// edge the field must equal the token subject; over the mesh (no principal) it
/// is accepted as-is. An anonymous edge call (a public method) is refused.
pub fn require_account<T>(request: &tonic::Request<T>, account_id: &str) -> Result<(), Status> {
    match principal(request) {
        None if is_anonymous_edge(request) => Err(anonymous_actor()),
        None => Ok(()),
        Some(p) if p.account_id() == account_id => Ok(()),
        Some(_) => Err(Status::permission_denied(
            "the authenticated account may not act as the requested account",
        )),
    }
}

/// Binds a request's **profile**-id actor field to the verified caller: on the
/// edge the profile must be one the token's account owns (`pids`); over the mesh
/// (no principal) it is accepted as-is. An anonymous edge call is refused.
pub fn require_profile<T>(request: &tonic::Request<T>, profile_id: &str) -> Result<(), Status> {
    match principal(request) {
        None if is_anonymous_edge(request) => Err(anonymous_actor()),
        None => Ok(()),
        Some(p) if p.owns_profile(profile_id) => Ok(()),
        Some(_) => Err(Status::permission_denied(
            "the authenticated account may not act as the requested profile",
        )),
    }
}

/// Requires the verified caller to carry `permission`. Over the mesh (no
/// principal) it is a no-op, like the actor helpers; an anonymous edge call is
/// refused.
pub fn require_permission<T>(request: &tonic::Request<T>, permission: &str) -> Result<(), Status> {
    match principal(request) {
        None if is_anonymous_edge(request) => Err(anonymous_actor()),
        None => Ok(()),
        Some(p) if p.has_permission(permission) => Ok(()),
        Some(_) => Err(Status::permission_denied("missing permission")),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use auth_context::{Permission, PrincipalId};
    use serde_json::json;

    fn principal_with(pids: &[&str], perms: &[&str]) -> EdgePrincipal {
        let mut raw: OidcClaims =
            serde_json::from_value(json!({ "sub": "acct-1", "exp": 4_102_444_800_i64, "sid": "s-1" }))
                .unwrap();
        raw.extra.insert("pids".into(), json!(pids));
        EdgePrincipal::new(Arc::new(CurrentPrincipal {
            user_id: PrincipalId::new("acct-1"),
            tenant_id: None,
            permissions: perms.iter().map(|p| Permission::new(*p)).collect(),
            raw_claims: raw,
        }))
    }

    fn edge_request(p: EdgePrincipal) -> tonic::Request<()> {
        let mut req = tonic::Request::new(());
        req.extensions_mut().insert(p);
        req
    }

    #[test]
    fn validate_accepts_well_formed_unique_paths() {
        validate_policy(&[
            authenticated("/post.v1.PostService/CreatePost"),
            public("/auth.v1.AuthService/Login"),
        ])
        .unwrap();
    }

    #[test]
    fn validate_rejects_malformed_and_duplicate_paths() {
        assert!(validate_policy(&[authenticated("post.v1.PostService/CreatePost")]).is_err());
        assert!(validate_policy(&[authenticated("/post.v1.PostService")]).is_err());
        assert!(validate_policy(&[authenticated("/a/b/c")]).is_err());
        assert!(validate_policy(&[authenticated("/a/b"), public("/a/b")]).is_err());
    }

    #[test]
    fn mesh_requests_have_no_principal_and_pass_every_check() {
        let req = tonic::Request::new(());
        assert!(principal(&req).is_none());
        require_account(&req, "anyone").unwrap();
        require_profile(&req, "anyone").unwrap();
        require_permission(&req, "anything").unwrap();
    }

    #[test]
    fn require_account_binds_to_the_token_subject() {
        let req = edge_request(principal_with(&[], &[]));
        require_account(&req, "acct-1").unwrap();
        let err = require_account(&req, "acct-2").unwrap_err();
        assert_eq!(err.code(), tonic::Code::PermissionDenied);
    }

    #[test]
    fn require_profile_binds_to_the_owned_profiles() {
        let req = edge_request(principal_with(&["p-1", "p-2"], &[]));
        require_profile(&req, "p-2").unwrap();
        let err = require_profile(&req, "p-9").unwrap_err();
        assert_eq!(err.code(), tonic::Code::PermissionDenied);
        // The account id is not a profile id.
        assert!(require_profile(&req, "acct-1").is_err());
    }

    #[test]
    fn require_permission_checks_the_perms_claim() {
        let req = edge_request(principal_with(&[], &["audit:read"]));
        require_permission(&req, "audit:read").unwrap();
        assert!(require_permission(&req, "audit:export").is_err());
    }

    fn anonymous_edge_request() -> tonic::Request<()> {
        let mut req = tonic::Request::new(());
        req.extensions_mut().insert(EdgeAnonymous);
        req
    }

    #[test]
    fn anonymous_edge_requests_fail_every_actor_check() {
        let req = anonymous_edge_request();
        assert!(principal(&req).is_none());
        for err in [
            require_account(&req, "acct-1").unwrap_err(),
            require_profile(&req, "p-1").unwrap_err(),
            require_permission(&req, "audit:read").unwrap_err(),
        ] {
            assert_eq!(err.code(), tonic::Code::Unauthenticated);
        }
    }

    #[test]
    fn viewer_tells_mesh_anonymous_and_member_apart() {
        assert_eq!(viewer(&tonic::Request::new(())), Viewer::Internal);
        assert_eq!(viewer(&anonymous_edge_request()), Viewer::Anonymous);

        let member = viewer(&edge_request(principal_with(&["p-1", "p-2"], &[])));
        assert_eq!(
            member,
            Viewer::Member {
                account_id: "acct-1".into(),
                profile_ids: vec!["p-1".into(), "p-2".into()],
            }
        );
        assert!(member.owns_profile("p-2"));
        assert!(!member.owns_profile("acct-1"));
        assert!(!Viewer::Internal.owns_profile("p-1"));
        assert!(!Viewer::Anonymous.owns_profile("p-1"));
    }

    #[test]
    fn a_guest_principal_is_recognised_by_its_kind_claim() {
        let member = principal_with(&["p-1"], &[]);
        assert!(!member.is_guest());
        let mut raw: OidcClaims = serde_json::from_value(
            json!({ "sub": "guest:g-1", "exp": 4_102_444_800_i64, "kind": "guest" }),
        )
        .unwrap();
        raw.extra.insert("pids".into(), json!([]));
        let guest = EdgePrincipal::new(Arc::new(CurrentPrincipal {
            user_id: PrincipalId::new("guest:g-1"),
            tenant_id: None,
            permissions: vec![Permission::new("read:public")],
            raw_claims: raw,
        }));
        assert!(guest.is_guest());
        // A guest owns no profile, so it can never act as one.
        assert!(!guest.owns_profile("g-1"));
    }

    #[test]
    fn principal_exposes_session_and_profiles() {
        let p = principal_with(&["p-1"], &[]);
        assert_eq!(p.account_id(), "acct-1");
        assert_eq!(p.session_id(), Some("s-1"));
        assert_eq!(p.profile_ids().collect::<Vec<_>>(), vec!["p-1"]);
    }
}
