use error::{AppError, Severity};
use http::StatusCode;
use thiserror::Error;

/// Canonical domain and application error type for the moderation microservice.
///
/// The `MOD-XXXX` namespace is grouped by concern so a code alone localizes the
/// fault: 1xxx case lifecycle, 2xxx decision ledger, 3xxx enforcement, 4xxx
/// penalty/strikes/policy, 5xxx appeal, 6xxx report intake, 7xxx Screen gate
/// (Plane C), 8xxx external integrity deps (classifiers / account directory),
/// 9xxx cross-cutting (domain/parse, concurrency, event publication).
///
/// ## Code catalogue
///
/// | Code     | Variant                       | HTTP | Severity | Retryable |
/// |----------|-------------------------------|------|----------|-----------|
/// | MOD-1001 | CaseNotFound                  | 404  | Low      | No        |
/// | MOD-1002 | InvalidCaseTransition         | 422  | Medium   | No        |
/// | MOD-1003 | CaseAlreadyResolved           | 409  | Low      | No        |
/// | MOD-2001 | DecisionNotFound              | 404  | Low      | No        |
/// | MOD-2002 | DecisionImmutable             | 409  | **High** | No        |
/// | MOD-3001 | EnforcementNotFound           | 404  | Low      | No        |
/// | MOD-3002 | EnforcementAlreadyReversed    | 409  | Low      | No        |
/// | MOD-3003 | InvalidEnforcementTransition  | 422  | Medium   | No        |
/// | MOD-4001 | PolicyVersionNotFound         | 404  | Medium   | No        |
/// | MOD-4002 | UnknownPolicyCategory         | 422  | Medium   | No        |
/// | MOD-5001 | AppealNotFound                | 404  | Low      | No        |
/// | MOD-5002 | AppealAlreadyResolved         | 409  | Low      | No        |
/// | MOD-5003 | AppealWindowClosed            | 422  | Low      | No        |
/// | MOD-5004 | NotAppealable                 | 422  | Medium   | No        |
/// | MOD-6001 | ReportNotFound                | 404  | Low      | No        |
/// | MOD-6002 | DuplicateReport               | 409  | Low      | No        |
/// | MOD-6003 | SelfReportRejected            | 422  | Low      | No        |
/// | MOD-6004 | ReportRateLimited             | 429  | Low      | No        |
/// | MOD-6005 | ReportedContentNotFound       | 404  | Low      | No        |
/// | MOD-6006 | UnsupportedReportTarget       | 422  | Low      | No        |
/// | MOD-6007 | ReporterRequired              | 422  | Low      | No        |
/// | MOD-8004 | ContentDirectoryUnavailable   | 503  | Medium   | **Yes**   |
/// | MOD-7001 | ContentBlocked                | 451  | **High** | No        |
/// | MOD-7002 | ScreenUnavailable             | 503  | **High** | **Yes**   |
/// | MOD-7003 | HashCorpusUnavailable         | 503  | **High** | **Yes**   |
/// | MOD-8001 | ClassifierUnavailable         | 503  | Medium   | **Yes**   |
/// | MOD-8002 | SignalRejected                | 422  | Medium   | No        |
/// | MOD-8003 | AccountDirectoryUnavailable   | 503  | High     | **Yes**   |
/// | MOD-9001 | DomainViolation               | 422  | Medium   | No        |
/// | MOD-9002 | InvalidSubjectRef             | 422  | Low      | No        |
/// | MOD-9003 | InvalidIdentifier             | 422  | Low      | No        |
/// | MOD-9004 | ConcurrentModification        | 409  | **High** | **Yes**   |
/// | MOD-9005 | EventPublishFailed            | 500  | Medium   | No        |
/// | DB-*     | Postgres records (delegated)  | var  | var      | var       |
/// | SDB-*    | Scylla history (delegated)    | var  | var      | var       |
/// | RDS-*    | Redis projection (delegated)  | var  | var      | var       |
/// | VAL-*    | Validation (delegated)        | 422  | Low      | No        |
///
/// > **Fail-closed semantics (Plane C):** `ScreenUnavailable` / `ContentBlocked`
/// > are not soft failures for catastrophic-harm categories (CSAM/NCII/TVEC). The
/// > *caller's* per-category fail policy converts an unavailable gate into a hard
/// > block — never an optimistic publish. See the blueprint's fail matrix.
#[derive(Debug, Error)]
#[non_exhaustive]
pub enum ModerationError {
    // ── Storage delegates (the three-store split) ─────────────────────────────
    /// Postgres — the decision/case System of Record (`decisions`, `cases`,
    /// `appeals`, `penalty_ledger`, `policy_versions`).
    #[error(transparent)]
    Records(#[from] postgres_storage::StorageError),

    /// Scylla — the high-volume signal/evidence time-series (per-actor violation
    /// history + retained classifier signals).
    #[error(transparent)]
    History(#[from] scylla_storage::ScyllaStorageError),

    /// Redis — the hot-path enforcement projection + Screen hash corpus.
    #[error(transparent)]
    Cache(#[from] redis_storage::RedisStorageError),

    #[error(transparent)]
    Validation(#[from] validation::ValidationError),

    // ── Case lifecycle (MOD-1xxx) ─────────────────────────────────────────────
    #[error("case not found: {id}")]
    CaseNotFound { id: String },

    #[error("case transition from '{from}' to '{to}' is not permitted")]
    InvalidCaseTransition { from: String, to: String },

    #[error("case has already been resolved")]
    CaseAlreadyResolved,

    // ── Decision ledger (MOD-2xxx) — append-only / WORM ───────────────────────
    #[error("decision not found: {id}")]
    DecisionNotFound { id: String },

    /// The decision ledger is the legal evidence record; it is append-only and a
    /// written decision can never be mutated (a reversal is a *new* decision).
    #[error("decision records are immutable; record a reversal instead")]
    DecisionImmutable,

    // ── Enforcement (MOD-3xxx) ────────────────────────────────────────────────
    #[error("enforcement action not found: {id}")]
    EnforcementNotFound { id: String },

    #[error("enforcement action has already been reversed")]
    EnforcementAlreadyReversed,

    #[error("enforcement transition from '{from}' to '{to}' is not permitted")]
    InvalidEnforcementTransition { from: String, to: String },

    // ── Penalty / strikes / policy (MOD-4xxx) ─────────────────────────────────
    #[error("policy version not found: {version}")]
    PolicyVersionNotFound { version: String },

    #[error("unknown policy category: '{category}'")]
    UnknownPolicyCategory { category: String },

    // ── Appeal (MOD-5xxx) ─────────────────────────────────────────────────────
    #[error("appeal not found: {id}")]
    AppealNotFound { id: String },

    #[error("appeal has already been resolved")]
    AppealAlreadyResolved,

    #[error("the appeal window for this decision has closed")]
    AppealWindowClosed,

    /// Some enforcement categories (e.g. legally-mandated CSAM removals) are not
    /// user-appealable by policy.
    #[error("this decision is not appealable under policy")]
    NotAppealable,

    // ── Report intake (MOD-6xxx) ──────────────────────────────────────────────
    #[error("report not found: {id}")]
    ReportNotFound { id: String },

    /// A duplicate of an already-ingested report (deduplicated by deterministic
    /// key); typically folded into an `Ok` at the application layer.
    #[error("duplicate report; already ingested")]
    DuplicateReport,

    #[error("a reporter may not report their own content")]
    SelfReportRejected,

    /// The reporter hit its report quota (per hour or per day).
    #[error("too many reports; try again later")]
    ReportRateLimited,

    /// The reported content does not exist (or no longer does).
    #[error("reported {entity_type} not found: {id}")]
    ReportedContentNotFound { entity_type: String, id: String },

    /// Clients can report posts, comments and profiles.
    #[error("{entity_type} cannot be reported by a client")]
    UnsupportedReportTarget { entity_type: String },

    /// A client report needs a client principal (member or guest) as reporter.
    #[error("a report needs an authenticated client as its reporter")]
    ReporterRequired,

    /// The services that tell whose content a report targets (post, comment,
    /// profile) could not answer.
    #[error("content directory unavailable")]
    ContentDirectoryUnavailable,

    // ── Screen gate · Plane C (MOD-7xxx) ──────────────────────────────────────
    /// A positive match against the known-bad corpus for a catastrophic-harm
    /// category — the content must never become visible. `451 Unavailable For
    /// Legal Reasons`.
    #[error("content blocked at screening for category '{category}'")]
    ContentBlocked { category: String },

    #[error("the screening gate is unavailable")]
    ScreenUnavailable,

    #[error("the known-bad hash corpus is unavailable")]
    HashCorpusUnavailable,

    // ── External integrity deps (MOD-8xxx) ────────────────────────────────────
    #[error("classifier service is unavailable")]
    ClassifierUnavailable,

    #[error("classifier signal rejected: {reason}")]
    SignalRejected { reason: String },

    #[error("account directory service is unavailable")]
    AccountDirectoryUnavailable,

    // ── Cross-cutting (MOD-9xxx) ──────────────────────────────────────────────
    #[error("domain invariant violated on '{field}': {message}")]
    DomainViolation { field: String, message: String },

    #[error("invalid subject reference: '{0}'")]
    InvalidSubjectRef(String),

    #[error("invalid identifier: '{0}'")]
    InvalidIdentifier(String),

    #[error("concurrent modification detected; reload and retry")]
    ConcurrentModification,

    #[error("failed to publish moderation event: {0}")]
    EventPublishFailed(String),
}

impl AppError for ModerationError {
    fn error_code(&self) -> &'static str {
        match self {
            ModerationError::Records(e) => e.error_code(),
            ModerationError::History(e) => e.error_code(),
            ModerationError::Cache(e) => e.error_code(),
            ModerationError::Validation(e) => e.error_code(),

            ModerationError::CaseNotFound { .. } => "MOD-1001",
            ModerationError::InvalidCaseTransition { .. } => "MOD-1002",
            ModerationError::CaseAlreadyResolved => "MOD-1003",

            ModerationError::DecisionNotFound { .. } => "MOD-2001",
            ModerationError::DecisionImmutable => "MOD-2002",

            ModerationError::EnforcementNotFound { .. } => "MOD-3001",
            ModerationError::EnforcementAlreadyReversed => "MOD-3002",
            ModerationError::InvalidEnforcementTransition { .. } => "MOD-3003",

            ModerationError::PolicyVersionNotFound { .. } => "MOD-4001",
            ModerationError::UnknownPolicyCategory { .. } => "MOD-4002",

            ModerationError::AppealNotFound { .. } => "MOD-5001",
            ModerationError::AppealAlreadyResolved => "MOD-5002",
            ModerationError::AppealWindowClosed => "MOD-5003",
            ModerationError::NotAppealable => "MOD-5004",

            ModerationError::ReportNotFound { .. } => "MOD-6001",
            ModerationError::DuplicateReport => "MOD-6002",
            ModerationError::SelfReportRejected => "MOD-6003",
            ModerationError::ReportRateLimited => "MOD-6004",
            ModerationError::ReportedContentNotFound { .. } => "MOD-6005",
            ModerationError::UnsupportedReportTarget { .. } => "MOD-6006",
            ModerationError::ReporterRequired => "MOD-6007",
            ModerationError::ContentDirectoryUnavailable => "MOD-8004",

            ModerationError::ContentBlocked { .. } => "MOD-7001",
            ModerationError::ScreenUnavailable => "MOD-7002",
            ModerationError::HashCorpusUnavailable => "MOD-7003",

            ModerationError::ClassifierUnavailable => "MOD-8001",
            ModerationError::SignalRejected { .. } => "MOD-8002",
            ModerationError::AccountDirectoryUnavailable => "MOD-8003",

            ModerationError::DomainViolation { .. } => "MOD-9001",
            ModerationError::InvalidSubjectRef(_) => "MOD-9002",
            ModerationError::InvalidIdentifier(_) => "MOD-9003",
            ModerationError::ConcurrentModification => "MOD-9004",
            ModerationError::EventPublishFailed(_) => "MOD-9005",
        }
    }

    fn http_status(&self) -> StatusCode {
        match self {
            ModerationError::Records(e) => e.http_status(),
            ModerationError::History(e) => e.http_status(),
            ModerationError::Cache(e) => e.http_status(),
            ModerationError::Validation(e) => e.http_status(),

            ModerationError::CaseNotFound { .. }
            | ModerationError::DecisionNotFound { .. }
            | ModerationError::EnforcementNotFound { .. }
            | ModerationError::PolicyVersionNotFound { .. }
            | ModerationError::AppealNotFound { .. }
            | ModerationError::ReportNotFound { .. }
            | ModerationError::ReportedContentNotFound { .. } => StatusCode::NOT_FOUND,

            ModerationError::ReportRateLimited => StatusCode::TOO_MANY_REQUESTS,

            ModerationError::CaseAlreadyResolved
            | ModerationError::DecisionImmutable
            | ModerationError::EnforcementAlreadyReversed
            | ModerationError::AppealAlreadyResolved
            | ModerationError::DuplicateReport
            | ModerationError::ConcurrentModification => StatusCode::CONFLICT,

            ModerationError::ContentBlocked { .. } => StatusCode::UNAVAILABLE_FOR_LEGAL_REASONS,

            ModerationError::ScreenUnavailable
            | ModerationError::HashCorpusUnavailable
            | ModerationError::ClassifierUnavailable
            | ModerationError::ContentDirectoryUnavailable
            | ModerationError::AccountDirectoryUnavailable => StatusCode::SERVICE_UNAVAILABLE,

            ModerationError::EventPublishFailed(_) => StatusCode::INTERNAL_SERVER_ERROR,

            _ => StatusCode::UNPROCESSABLE_ENTITY,
        }
    }

    fn severity(&self) -> Severity {
        match self {
            ModerationError::Records(e) => e.severity(),
            ModerationError::History(e) => e.severity(),
            ModerationError::Cache(e) => e.severity(),
            ModerationError::Validation(e) => e.severity(),

            ModerationError::DecisionImmutable
            | ModerationError::ContentBlocked { .. }
            | ModerationError::ScreenUnavailable
            | ModerationError::HashCorpusUnavailable
            | ModerationError::AccountDirectoryUnavailable
            | ModerationError::ConcurrentModification => Severity::High,

            ModerationError::InvalidCaseTransition { .. }
            | ModerationError::InvalidEnforcementTransition { .. }
            | ModerationError::PolicyVersionNotFound { .. }
            | ModerationError::UnknownPolicyCategory { .. }
            | ModerationError::NotAppealable
            | ModerationError::ClassifierUnavailable
            | ModerationError::SignalRejected { .. }
            | ModerationError::DomainViolation { .. }
            | ModerationError::EventPublishFailed(_) => Severity::Medium,

            _ => Severity::Low,
        }
    }

    fn is_retryable(&self) -> bool {
        match self {
            ModerationError::Records(e) => e.is_retryable(),
            ModerationError::History(e) => e.is_retryable(),
            ModerationError::Cache(e) => e.is_retryable(),
            ModerationError::ScreenUnavailable
            | ModerationError::HashCorpusUnavailable
            | ModerationError::ClassifierUnavailable
            | ModerationError::ContentDirectoryUnavailable
            | ModerationError::AccountDirectoryUnavailable
            | ModerationError::ConcurrentModification => true,
            _ => false,
        }
    }

    fn category(&self) -> &'static str {
        match self {
            ModerationError::Records(e) => e.category(),
            ModerationError::History(e) => e.category(),
            ModerationError::Cache(e) => e.category(),
            ModerationError::Validation(e) => e.category(),
            _ => "MOD",
        }
    }

    fn user_facing_message(&self) -> &'static str {
        match self {
            ModerationError::Records(e) => e.user_facing_message(),
            ModerationError::History(e) => e.user_facing_message(),
            ModerationError::Cache(e) => e.user_facing_message(),
            ModerationError::Validation(e) => e.user_facing_message(),

            ModerationError::CaseNotFound { .. }
            | ModerationError::DecisionNotFound { .. }
            | ModerationError::EnforcementNotFound { .. }
            | ModerationError::AppealNotFound { .. }
            | ModerationError::ReportNotFound { .. } => "The requested record does not exist.",
            ModerationError::ReportedContentNotFound { .. } => "The content you reported no longer exists.",
            ModerationError::ReportRateLimited => "You have sent many reports recently. Please try again later.",
            ModerationError::UnsupportedReportTarget { .. } => "This item cannot be reported.",
            ModerationError::ReporterRequired => "Please open the app to send a report.",

            ModerationError::CaseAlreadyResolved
            | ModerationError::AppealAlreadyResolved
            | ModerationError::EnforcementAlreadyReversed => "This item has already been resolved.",

            ModerationError::DecisionImmutable => "This record cannot be changed.",

            ModerationError::AppealWindowClosed => "The window to appeal this decision has closed.",
            ModerationError::NotAppealable => "This decision cannot be appealed.",

            ModerationError::DuplicateReport => "You have already reported this.",
            ModerationError::SelfReportRejected => "You cannot report your own content.",

            ModerationError::ContentBlocked { .. } => "This content cannot be published.",

            ModerationError::ScreenUnavailable
            | ModerationError::HashCorpusUnavailable
            | ModerationError::ClassifierUnavailable
            | ModerationError::ContentDirectoryUnavailable
            | ModerationError::AccountDirectoryUnavailable => {
                "Safety checks are temporarily unavailable. Please try again."
            }

            ModerationError::EventPublishFailed(_) => "We could not complete that action. Please try again.",

            _ => "A domain constraint was violated.",
        }
    }
}
