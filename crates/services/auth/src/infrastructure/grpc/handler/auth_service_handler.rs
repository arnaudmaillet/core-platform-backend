use std::sync::Arc;

use chrono::{DateTime, Utc};
use cqrs::Envelope;
use error::AppError;
use tonic::{Request, Response, Status};
use uuid::Uuid;

use transport::grpc::edge;
use crate::application::command::{
    IssuedSession, LoginCommand, LoginHandler, LogoutAllSessionsCommand, LogoutAllSessionsHandler,
    LogoutCommand, LogoutHandler, RefreshCommand, RefreshHandler, StartGuestSessionCommand,
    StartGuestSessionHandler,
};
use crate::application::port::AuthnGrant;
use crate::application::query::{
    IntrospectHandler, IntrospectQuery, ListSessionsHandler, ListSessionsQuery, SessionSummary,
};
use crate::domain::value_object::{DeviceFingerprint, SessionStatus};
use crate::error::AuthError;

// ── Proto inclusion ───────────────────────────────────────────────────────────
pub use auth_api as proto;

/// gRPC request handler for the `auth.v1` service.
///
/// Each method translates an inbound Protobuf request into an application
/// command/query, invokes the corresponding handler with a fresh correlation id
/// and the wall clock, and maps the result (or [`AuthError`]) back to Protobuf /
/// [`Status`]. The application handlers — not a CQRS bus — are held directly,
/// because the token-returning use-cases do not fit the command bus's `()` return.
#[derive(Clone)]
pub struct AuthServiceHandler {
    login: Arc<LoginHandler>,
    refresh: Arc<RefreshHandler>,
    logout: Arc<LogoutHandler>,
    logout_all: Arc<LogoutAllSessionsHandler>,
    introspect: Arc<IntrospectHandler>,
    list_sessions: Arc<ListSessionsHandler>,
    start_guest: Arc<StartGuestSessionHandler>,
}

impl AuthServiceHandler {
    pub fn new(
        login: Arc<LoginHandler>,
        refresh: Arc<RefreshHandler>,
        logout: Arc<LogoutHandler>,
        logout_all: Arc<LogoutAllSessionsHandler>,
        introspect: Arc<IntrospectHandler>,
        list_sessions: Arc<ListSessionsHandler>,
        start_guest: Arc<StartGuestSessionHandler>,
    ) -> Self {
        Self { login, refresh, logout, logout_all, introspect, list_sessions, start_guest }
    }

    pub async fn start_guest_session(
        &self,
        request: Request<proto::StartGuestSessionRequest>,
    ) -> Result<Response<proto::StartGuestSessionResponse>, Status> {
        let req = request.into_inner();
        let non_empty = |s: String| if s.trim().is_empty() { None } else { Some(s) };
        let cmd = StartGuestSessionCommand {
            device: device_from_proto(req.device),
            attestation: non_empty(req.attestation),
            locale: non_empty(req.locale),
            region_hint: non_empty(req.region_hint),
            current_country: non_empty(req.current_country),
        };

        let issued = self
            .start_guest
            .handle(Envelope::new(Uuid::now_v7(), cmd), Utc::now())
            .await
            .map_err(auth_error_to_status)?;

        Ok(Response::new(proto::StartGuestSessionResponse {
            guest_id: issued.account_id.as_str(),
            tokens: Some(token_pair(&issued)),
        }))
    }

    pub async fn login(
        &self,
        request: Request<proto::LoginRequest>,
    ) -> Result<Response<proto::LoginResponse>, Status> {
        let req = request.into_inner();
        let grant = grant_from_proto(req.credential)?;
        let cmd = LoginCommand { grant, device: device_from_proto(req.device) };

        let issued = self
            .login
            .handle(Envelope::new(Uuid::now_v7(), cmd), Utc::now())
            .await
            .map_err(auth_error_to_status)?;

        Ok(Response::new(proto::LoginResponse {
            account_id: issued.account_id.as_str(),
            tokens: Some(token_pair(&issued)),
            first_link: issued.first_link,
            reactivated: issued.reactivated,
        }))
    }

    pub async fn refresh(
        &self,
        request: Request<proto::RefreshRequest>,
    ) -> Result<Response<proto::RefreshResponse>, Status> {
        let req = request.into_inner();
        let cmd = RefreshCommand {
            refresh_token: req.refresh_token,
            device: device_from_proto(req.device),
        };

        let issued = self
            .refresh
            .handle(Envelope::new(Uuid::now_v7(), cmd), Utc::now())
            .await
            .map_err(auth_error_to_status)?;

        Ok(Response::new(proto::RefreshResponse { tokens: Some(token_pair(&issued)) }))
    }

    pub async fn logout(
        &self,
        request: Request<proto::LogoutRequest>,
    ) -> Result<Response<proto::LogoutResponse>, Status> {
        // Edge: an empty session_id means "my current session" (the token's
        // `sid`), and the session must belong to the caller. Mesh: as supplied.
        let principal = edge::principal(&request);
        let supplied = &request.get_ref().session_id;
        let session_id = if supplied.is_empty() {
            principal.and_then(|p| p.session_id()).map(str::to_owned).unwrap_or_default()
        } else {
            supplied.clone()
        };
        let cmd = LogoutCommand {
            session_id,
            actor: principal.map(|p| p.account_id().to_owned()),
        };
        let out = self
            .logout
            .handle(Envelope::new(Uuid::now_v7(), cmd), Utc::now())
            .await
            .map_err(auth_error_to_status)?;
        Ok(Response::new(proto::LogoutResponse { success: out.success }))
    }

    pub async fn logout_all_sessions(
        &self,
        request: Request<proto::LogoutAllSessionsRequest>,
    ) -> Result<Response<proto::LogoutAllSessionsResponse>, Status> {
        let account_id = edge_account(&request, &request.get_ref().account_id)?;
        let cmd = LogoutAllSessionsCommand { account_id };
        let out = self
            .logout_all
            .handle(Envelope::new(Uuid::now_v7(), cmd), Utc::now())
            .await
            .map_err(auth_error_to_status)?;
        Ok(Response::new(proto::LogoutAllSessionsResponse {
            success: true,
            generation: out.generation,
            sessions_revoked: out.sessions_revoked,
        }))
    }

    pub async fn introspect(
        &self,
        request: Request<proto::IntrospectRequest>,
    ) -> Result<Response<proto::IntrospectResponse>, Status> {
        let query = IntrospectQuery { access_token: request.into_inner().access_token };
        let view = self
            .introspect
            .handle_at(Envelope::new(Uuid::now_v7(), query), Utc::now())
            .await
            .map_err(auth_error_to_status)?;

        Ok(Response::new(proto::IntrospectResponse {
            active: view.active,
            account_id: view.account_id.unwrap_or_default(),
            session_id: view.session_id.unwrap_or_default(),
            generation: view.generation,
            permissions: view.permissions,
            expires_at: view.expires_at.map(to_timestamp),
        }))
    }

    pub async fn list_sessions(
        &self,
        request: Request<proto::ListSessionsRequest>,
    ) -> Result<Response<proto::ListSessionsResponse>, Status> {
        use cqrs::QueryHandler;
        let query = ListSessionsQuery {
            account_id: edge_account(&request, &request.get_ref().account_id)?,
            // Lets the view flag the caller's own session.
            current_session_id: edge::principal(&request)
                .and_then(|p| p.session_id())
                .map(str::to_owned),
        };
        let sessions = self
            .list_sessions
            .handle(Envelope::new(Uuid::now_v7(), query))
            .await
            .map_err(auth_error_to_status)?;

        Ok(Response::new(proto::ListSessionsResponse {
            sessions: sessions.into_iter().map(session_view).collect(),
        }))
    }
}

// ── Mapping helpers ───────────────────────────────────────────────────────────

/// Resolves an account-scoped request's `account_id`: on the edge an empty value
/// means "the calling principal's account", a non-empty one must be the caller's
/// own; over the mesh it is taken as supplied (validation rejects empty later).
fn edge_account<T>(request: &Request<T>, supplied: &str) -> Result<String, Status> {
    match edge::principal(request) {
        Some(p) if supplied.is_empty() => Ok(p.account_id().to_owned()),
        Some(_) => {
            edge::require_account(request, supplied)?;
            Ok(supplied.to_owned())
        }
        None => Ok(supplied.to_owned()),
    }
}

fn grant_from_proto(
    credential: Option<proto::login_request::Credential>,
) -> Result<AuthnGrant, Status> {
    match credential {
        Some(proto::login_request::Credential::AuthorizationCode(g)) => {
            Ok(AuthnGrant::AuthorizationCode {
                code: g.code,
                redirect_uri: g.redirect_uri,
                code_verifier: g.code_verifier,
            })
        }
        Some(proto::login_request::Credential::Password(g)) => {
            Ok(AuthnGrant::Password { username: g.username, password: g.password })
        }
        None => Err(Status::invalid_argument("login requires a credential")),
    }
}

fn device_from_proto(device: Option<proto::DeviceContext>) -> DeviceFingerprint {
    let non_empty = |s: String| if s.is_empty() { None } else { Some(s) };
    match device {
        Some(d) => DeviceFingerprint::new(
            non_empty(d.user_agent),
            non_empty(d.ip_address),
            non_empty(d.device_id),
        ),
        None => DeviceFingerprint::default(),
    }
}

fn token_pair(issued: &IssuedSession) -> proto::TokenPair {
    proto::TokenPair {
        access_token: issued.access_token.clone(),
        refresh_token: issued.refresh_token.clone(),
        token_type: "Bearer".to_owned(),
        expires_in: issued.access_expires_in,
        session_id: issued.session_id.as_str(),
    }
}

fn session_view(summary: SessionSummary) -> proto::SessionView {
    proto::SessionView {
        session_id: summary.session_id,
        status: session_status_to_proto(summary.status),
        generation: summary.generation,
        device: None,
        issued_at: Some(to_timestamp(summary.issued_at)),
        expires_at: Some(to_timestamp(summary.expires_at)),
        absolute_expiry: Some(to_timestamp(summary.absolute_expiry)),
        current: summary.current,
    }
}

fn session_status_to_proto(status: SessionStatus) -> i32 {
    let s = match status {
        SessionStatus::Active => proto::SessionStatus::Active,
        SessionStatus::Revoked => proto::SessionStatus::Revoked,
        SessionStatus::Expired => proto::SessionStatus::Expired,
    };
    s as i32
}

fn to_timestamp(dt: DateTime<Utc>) -> prost_types::Timestamp {
    prost_types::Timestamp { seconds: dt.timestamp(), nanos: dt.timestamp_subsec_nanos() as i32 }
}

/// Maps an [`AuthError`] to a gRPC [`Status`] using its [`AppError`] metadata, so
/// the HTTP semantics defined once in `error.rs` drive the gRPC code too.
pub fn auth_error_to_status(err: AuthError) -> Status {
    let msg = err.to_string();
    let retryable = err.is_retryable();
    match err.http_status().as_u16() {
        401 => Status::unauthenticated(msg),
        403 => Status::permission_denied(msg),
        404 => Status::not_found(msg),
        409 if retryable => Status::aborted(msg),
        409 => Status::already_exists(msg),
        400 | 422 => Status::failed_precondition(msg),
        502 | 503 => Status::unavailable(msg),
        _ => Status::internal(msg),
    }
}
