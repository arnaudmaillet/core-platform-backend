use error::{AppError, Severity};
use http::StatusCode;
use thiserror::Error;

/// Canonical domain and application error type for the auth microservice.
///
/// The `AUT-XXXX` namespace is grouped by concern so a code alone localizes the
/// fault: 1xxx session lifecycle, 2xxx refresh/rotation, 3xxx subject linkage,
/// 4xxx token minting, 5xxx IdP broker, 6xxx account directory, 9xxx domain/parse.
///
/// ## Code catalogue
///
/// | Code     | Variant                      | HTTP | Severity | Retryable |
/// |----------|------------------------------|------|----------|-----------|
/// | AUT-1001 | SessionNotFound              | 404  | Low      | No        |
/// | AUT-1002 | SessionRevoked               | 401  | Low      | No        |
/// | AUT-1003 | SessionExpired               | 401  | Low      | No        |
/// | AUT-1004 | InvalidSessionTransition     | 422  | Medium   | No        |
/// | AUT-2001 | RefreshTokenNotFound         | 401  | Low      | No        |
/// | AUT-2002 | RefreshTokenExpired          | 401  | Low      | No        |
/// | AUT-2003 | RefreshTokenReuseDetected    | 401  | **High** | No        |
/// | AUT-2004 | RefreshTokenAlreadyRotated   | 401  | Medium   | No        |
/// | AUT-3001 | SubjectLinkNotFound          | 404  | Low      | No        |
/// | AUT-3002 | SubjectAlreadyLinked         | 409  | Low      | No        |
/// | AUT-4001 | TokenSigningFailed           | 500  | **High** | No        |
/// | AUT-4002 | SigningKeyUnavailable        | 503  | **High** | **Yes**   |
/// | AUT-4003 | InvalidTokenGeneration       | 401  | Low      | No        |
/// | AUT-5001 | IdpUnavailable               | 503  | High     | **Yes**   |
/// | AUT-5002 | IdpAuthenticationFailed      | 401  | Low      | No        |
/// | AUT-5003 | IdpTokenRejected             | 401  | Low      | No        |
/// | AUT-5004 | ClaimsNormalizationFailed    | 502  | Medium   | No        |
/// | AUT-5005 | CredentialManagementUnavailable | 503 | High   | **Yes**   |
/// | AUT-5006 | PasswordRejected             | 422  | Low      | No        |
/// | AUT-5007 | VerificationMethodUnavailable | 422 | Low      | No        |
/// | AUT-5008 | IdTokenRejected              | 401  | Low      | No        |
/// | AUT-5009 | FederatedProviderNotConfigured | 422 | Medium  | No        |
/// | AUT-5010 | IdTokenWithoutEmail          | 422  | Low      | No        |
/// | AUT-1005 | GuestSessionsDisabled        | 403  | Low      | No        |
/// | AUT-6001 | AccountNotActive             | 403  | Medium   | No        |
/// | AUT-6002 | AccountDirectoryUnavailable  | 503  | High     | **Yes**   |
/// | AUT-6003 | ProfileDirectoryUnavailable  | 503  | Medium   | **Yes**   |
/// | AUT-6004 | NoAccountForIdentity         | 404  | Low      | No        |
/// | AUT-6005 | AgeBelowMinimum              | 422  | Low      | No        |
/// | AUT-6006 | EmailAlreadyRegistered       | 409  | Low      | No        |
/// | AUT-9001 | DomainViolation              | 422  | Medium   | No        |
/// | AUT-9002 | InvalidSessionId             | 422  | Low      | No        |
/// | AUT-9003 | InvalidAccountId             | 422  | Low      | No        |
/// | DB-*     | Storage (delegated)          | var  | var      | var       |
/// | VAL-*    | Validation (delegated)       | 422  | Low      | No        |
///
/// > Migration to a different IdP (Cognito/Okta/custom) reuses this exact
/// > namespace: the broker faults (AUT-5xxx) are normalized, never IdP-specific.
#[derive(Debug, Error)]
#[non_exhaustive]
pub enum AuthError {
    // ── Infrastructure delegates ──────────────────────────────────────────────
    #[error(transparent)]
    Storage(#[from] postgres_storage::StorageError),

    #[error(transparent)]
    Cache(#[from] redis_storage::RedisStorageError),

    #[error(transparent)]
    Validation(#[from] validation::ValidationError),

    // ── Event publication (AUT-7xxx) ──────────────────────────────────────────
    #[error("failed to publish auth event: {0}")]
    EventPublishFailed(String),

    // ── Optimistic concurrency (AUT-8xxx) ─────────────────────────────────────
    #[error("concurrent modification detected; reload and retry")]
    ConcurrentModification,

    // ── Session lifecycle (AUT-1xxx) ──────────────────────────────────────────
    #[error("session not found: {id}")]
    SessionNotFound { id: String },

    #[error("session has been revoked")]
    SessionRevoked,

    #[error("session has expired")]
    SessionExpired,

    #[error("session transition from '{from}' to '{to}' is not permitted")]
    InvalidSessionTransition { from: String, to: String },

    // ── Refresh / rotation (AUT-2xxx) ─────────────────────────────────────────
    #[error("refresh token not found")]
    RefreshTokenNotFound,

    #[error("refresh token has expired")]
    RefreshTokenExpired,

    /// Presenting an already-rotated refresh token signals theft; the handler
    /// revokes the entire session generation in response (OAuth2 BCP).
    #[error("refresh token reuse detected; the session generation has been revoked")]
    RefreshTokenReuseDetected,

    #[error("refresh token has already been rotated")]
    RefreshTokenAlreadyRotated,

    // ── Subject linkage (AUT-3xxx) ────────────────────────────────────────────
    #[error("no account is linked to IdP subject ({iss}, {sub})")]
    SubjectLinkNotFound { iss: String, sub: String },

    #[error("IdP subject ({iss}, {sub}) is already linked to an account")]
    SubjectAlreadyLinked { iss: String, sub: String },

    // ── Token minting (AUT-4xxx) ──────────────────────────────────────────────
    #[error("failed to sign edge token")]
    TokenSigningFailed,

    #[error("no signing key is currently available")]
    SigningKeyUnavailable,

    #[error("edge token generation is stale for this session")]
    InvalidTokenGeneration,

    // ── IdP broker (AUT-5xxx) — normalized, never IdP-specific ────────────────
    #[error("identity provider is unavailable")]
    IdpUnavailable,

    #[error("identity provider rejected the credentials")]
    IdpAuthenticationFailed,

    #[error("identity provider rejected the presented token")]
    IdpTokenRejected,

    #[error("failed to normalize identity-provider claims: {0}")]
    ClaimsNormalizationFailed(String),

    /// The IdP's credential management (setting a password) is unreachable or
    /// not configured (`AUTH_KEYCLOAK_ADMIN_*`).
    #[error("credential management is unavailable")]
    CredentialManagementUnavailable,

    /// The IdP refused the new password under its password policy.
    #[error("the new password was rejected: {reason}")]
    PasswordRejected { reason: String },

    /// A step-up method the account cannot use (e.g. an MFA code with no MFA
    /// enrolled).
    #[error("this verification method is not available for the account")]
    VerificationMethodUnavailable,

    /// An Apple / Google id_token failed verification (signature, issuer,
    /// audience, expiry or nonce). The reason is logged, never returned.
    #[error("the identity token was rejected: {reason}")]
    IdTokenRejected { reason: String },

    /// No client id is configured for this provider (`AUTH_APPLE_AUDIENCES` /
    /// `AUTH_GOOGLE_AUDIENCES`): its sign-in is off.
    #[error("sign-in with {provider} is not configured")]
    FederatedProviderNotConfigured { provider: String },

    /// The id_token carries no email: an account cannot be created from it.
    #[error("the identity token carries no email address")]
    IdTokenWithoutEmail,

    // ── Account directory (AUT-6xxx) ──────────────────────────────────────────
    #[error("account is not active; current status: '{current}'")]
    AccountNotActive { current: String },

    #[error("account directory service is unavailable")]
    AccountDirectoryUnavailable,

    /// The `profile` service could not list the account's profiles. Handlers
    /// degrade to an empty `pids` claim rather than failing the mint.
    #[error("profile directory service is unavailable")]
    ProfileDirectoryUnavailable,

    /// An Apple / Google identity with no account: the client goes on with SignUp.
    #[error("no account for this identity")]
    NoAccountForIdentity,

    /// The date of birth is under the minimum age; nothing was created.
    #[error("the account holder is under the minimum age")]
    AgeBelowMinimum,

    /// The email is already an account's (a concurrent sign-up won).
    #[error("this email address already belongs to an account")]
    EmailAlreadyRegistered,

    // ── Domain invariants & parse errors (AUT-9xxx) ───────────────────────────
    #[error("domain invariant violated on '{field}': {message}")]
    DomainViolation { field: String, message: String },

    #[error("invalid session ID: '{0}'")]
    InvalidSessionId(String),

    #[error("invalid account ID: '{0}'")]
    InvalidAccountId(String),

    /// `StartGuestSession` is switched off (`AUTH_GUEST_SESSIONS_ENABLED`).
    #[error("guest sessions are disabled")]
    GuestSessionsDisabled,
}

impl AppError for AuthError {
    fn error_code(&self) -> &'static str {
        match self {
            AuthError::Storage(e) => e.error_code(),
            AuthError::Cache(e) => e.error_code(),
            AuthError::Validation(e) => e.error_code(),

            AuthError::EventPublishFailed(_) => "AUT-7001",
            AuthError::ConcurrentModification => "AUT-8001",

            AuthError::SessionNotFound { .. } => "AUT-1001",
            AuthError::SessionRevoked => "AUT-1002",
            AuthError::SessionExpired => "AUT-1003",
            AuthError::InvalidSessionTransition { .. } => "AUT-1004",

            AuthError::RefreshTokenNotFound => "AUT-2001",
            AuthError::RefreshTokenExpired => "AUT-2002",
            AuthError::RefreshTokenReuseDetected => "AUT-2003",
            AuthError::RefreshTokenAlreadyRotated => "AUT-2004",

            AuthError::SubjectLinkNotFound { .. } => "AUT-3001",
            AuthError::SubjectAlreadyLinked { .. } => "AUT-3002",

            AuthError::TokenSigningFailed => "AUT-4001",
            AuthError::SigningKeyUnavailable => "AUT-4002",
            AuthError::InvalidTokenGeneration => "AUT-4003",

            AuthError::IdpUnavailable => "AUT-5001",
            AuthError::IdpAuthenticationFailed => "AUT-5002",
            AuthError::IdpTokenRejected => "AUT-5003",
            AuthError::ClaimsNormalizationFailed(_) => "AUT-5004",
            AuthError::CredentialManagementUnavailable => "AUT-5005",
            AuthError::PasswordRejected { .. } => "AUT-5006",
            AuthError::VerificationMethodUnavailable => "AUT-5007",
            AuthError::IdTokenRejected { .. } => "AUT-5008",
            AuthError::FederatedProviderNotConfigured { .. } => "AUT-5009",
            AuthError::IdTokenWithoutEmail => "AUT-5010",

            AuthError::AccountNotActive { .. } => "AUT-6001",
            AuthError::AccountDirectoryUnavailable => "AUT-6002",
            AuthError::ProfileDirectoryUnavailable => "AUT-6003",
            AuthError::NoAccountForIdentity => "AUT-6004",
            AuthError::AgeBelowMinimum => "AUT-6005",
            AuthError::EmailAlreadyRegistered => "AUT-6006",

            AuthError::DomainViolation { .. } => "AUT-9001",
            AuthError::InvalidSessionId(_) => "AUT-9002",
            AuthError::InvalidAccountId(_) => "AUT-9003",
            AuthError::GuestSessionsDisabled => "AUT-1005",
        }
    }

    fn http_status(&self) -> StatusCode {
        match self {
            AuthError::Storage(e) => e.http_status(),
            AuthError::Cache(e) => e.http_status(),
            AuthError::Validation(e) => e.http_status(),

            AuthError::EventPublishFailed(_) => StatusCode::INTERNAL_SERVER_ERROR,
            AuthError::ConcurrentModification => StatusCode::CONFLICT,

            AuthError::SessionNotFound { .. }
            | AuthError::SubjectLinkNotFound { .. }
            | AuthError::NoAccountForIdentity => StatusCode::NOT_FOUND,

            AuthError::SessionRevoked
            | AuthError::SessionExpired
            | AuthError::RefreshTokenNotFound
            | AuthError::RefreshTokenExpired
            | AuthError::RefreshTokenReuseDetected
            | AuthError::RefreshTokenAlreadyRotated
            | AuthError::InvalidTokenGeneration
            | AuthError::IdpAuthenticationFailed
            | AuthError::IdpTokenRejected
            | AuthError::IdTokenRejected { .. } => StatusCode::UNAUTHORIZED,

            AuthError::SubjectAlreadyLinked { .. } | AuthError::EmailAlreadyRegistered => StatusCode::CONFLICT,

            AuthError::AccountNotActive { .. } | AuthError::GuestSessionsDisabled => StatusCode::FORBIDDEN,

            AuthError::TokenSigningFailed => StatusCode::INTERNAL_SERVER_ERROR,

            AuthError::ClaimsNormalizationFailed(_) => StatusCode::BAD_GATEWAY,

            AuthError::SigningKeyUnavailable
            | AuthError::IdpUnavailable
            | AuthError::CredentialManagementUnavailable
            | AuthError::AccountDirectoryUnavailable
            | AuthError::ProfileDirectoryUnavailable => StatusCode::SERVICE_UNAVAILABLE,

            _ => StatusCode::UNPROCESSABLE_ENTITY,
        }
    }

    fn severity(&self) -> Severity {
        match self {
            AuthError::Storage(e) => e.severity(),
            AuthError::Cache(e) => e.severity(),
            AuthError::Validation(e) => e.severity(),

            AuthError::EventPublishFailed(_) => Severity::Medium,
            AuthError::ConcurrentModification => Severity::High,

            AuthError::RefreshTokenReuseDetected
            | AuthError::TokenSigningFailed
            | AuthError::SigningKeyUnavailable
            | AuthError::IdpUnavailable
            | AuthError::CredentialManagementUnavailable
            | AuthError::AccountDirectoryUnavailable => Severity::High,

            AuthError::InvalidSessionTransition { .. }
            | AuthError::RefreshTokenAlreadyRotated
            | AuthError::ClaimsNormalizationFailed(_)
            | AuthError::ProfileDirectoryUnavailable
            | AuthError::AccountNotActive { .. }
            | AuthError::DomainViolation { .. } => Severity::Medium,

            _ => Severity::Low,
        }
    }

    fn is_retryable(&self) -> bool {
        match self {
            AuthError::Storage(e) => e.is_retryable(),
            AuthError::Cache(e) => e.is_retryable(),
            AuthError::ConcurrentModification
            | AuthError::SigningKeyUnavailable
            | AuthError::IdpUnavailable
            | AuthError::CredentialManagementUnavailable
            | AuthError::AccountDirectoryUnavailable
            | AuthError::ProfileDirectoryUnavailable => true,
            _ => false,
        }
    }

    fn category(&self) -> &'static str {
        match self {
            AuthError::Storage(e) => e.category(),
            AuthError::Cache(e) => e.category(),
            AuthError::Validation(e) => e.category(),
            _ => "AUT",
        }
    }

    fn user_facing_message(&self) -> &'static str {
        match self {
            AuthError::Storage(e) => e.user_facing_message(),
            AuthError::Cache(e) => e.user_facing_message(),
            AuthError::Validation(e) => e.user_facing_message(),

            AuthError::EventPublishFailed(_) => "We could not complete that action. Please try again.",

            AuthError::SessionNotFound { .. } => "The session does not exist.",
            AuthError::SessionRevoked => "This session has been signed out.",
            AuthError::SessionExpired => "This session has expired; please sign in again.",
            AuthError::InvalidSessionTransition { .. } => "This session operation is not permitted.",
            AuthError::RefreshTokenNotFound
            | AuthError::RefreshTokenExpired
            | AuthError::RefreshTokenAlreadyRotated => "Your session could not be refreshed; please sign in again.",
            AuthError::RefreshTokenReuseDetected => "A security issue was detected; please sign in again.",
            AuthError::SubjectLinkNotFound { .. } => "No account is linked to this identity.",
            AuthError::SubjectAlreadyLinked { .. } => "This identity is already linked to an account.",
            AuthError::TokenSigningFailed | AuthError::SigningKeyUnavailable => "We could not issue a session right now. Please try again.",
            AuthError::InvalidTokenGeneration => "Your session is no longer valid; please sign in again.",
            AuthError::IdpUnavailable => "The sign-in service is temporarily unavailable.",
            AuthError::IdpAuthenticationFailed => "The credentials provided are incorrect.",
            AuthError::IdpTokenRejected => "Your sign-in could not be verified; please sign in again.",
            AuthError::ClaimsNormalizationFailed(_) => "We could not complete sign-in. Please try again.",
            AuthError::CredentialManagementUnavailable => "Passwords cannot be changed right now. Please try again later.",
            AuthError::PasswordRejected { .. } => "This password does not meet the requirements.",
            AuthError::VerificationMethodUnavailable => "This verification method is not available for your account.",
            AuthError::AccountNotActive { .. } => "This account cannot sign in at this time.",
            AuthError::AccountDirectoryUnavailable => "The account service is temporarily unavailable.",
            AuthError::ProfileDirectoryUnavailable => "The profile service is temporarily unavailable.",
            AuthError::GuestSessionsDisabled => "Browsing without an account is not available right now.",
            AuthError::IdTokenRejected { .. } => "Your sign-in could not be verified; please try again.",
            AuthError::FederatedProviderNotConfigured { .. } => "This sign-in method is not available right now.",
            AuthError::IdTokenWithoutEmail => "This sign-in did not share an email address; please allow it and try again.",
            AuthError::NoAccountForIdentity => "There is no account for this sign-in yet.",
            AuthError::AgeBelowMinimum => "You are not old enough to create an account.",
            AuthError::EmailAlreadyRegistered => "This email address already has an account.",
            _ => "A domain constraint was violated.",
        }
    }
}
