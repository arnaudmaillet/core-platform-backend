use error::{AppError, Severity};
use http::StatusCode;
use thiserror::Error;

/// Canonical domain and application error type for the search microservice.
///
/// The `SCH-XXXX` namespace is grouped by concern so a code alone localizes the
/// fault: 1xxx query/parse, 2xxx index/upsert, 3xxx projection/transform, 4xxx
/// engine availability (the fail-open core), 5xxx reindex/alias/migration (the
/// Phase-7 ops surface), 8xxx inbound event decode / source mapping, 9xxx
/// cross-cutting (domain/parse, event consumption).
///
/// ## Code catalogue
///
/// | Code     | Variant                  | HTTP | Severity | Retryable |
/// |----------|--------------------------|------|----------|-----------|
/// | SCH-1001 | InvalidQuery             | 422  | Low      | No        |
/// | SCH-1002 | InvalidCursor            | 422  | Low      | No        |
/// | SCH-1003 | UnsupportedEntityType    | 422  | Low      | No        |
/// | SCH-2001 | DocumentNotFound         | 404  | Low      | No        |
/// | SCH-2002 | StaleVersion             | 409  | Low      | No        |
/// | SCH-2003 | BulkIndexFailed          | 500  | Medium   | **Yes**   |
/// | SCH-3001 | ProjectionFailed         | 422  | Medium   | No        |
/// | SCH-3002 | MissingProjectionField   | 422  | Medium   | No        |
/// | SCH-4001 | EngineUnavailable        | 503  | **High** | **Yes**   |
/// | SCH-4002 | EngineTimeout            | 504  | **High** | **Yes**   |
/// | SCH-4003 | IndexNotFound            | 503  | **High** | No        |
/// | SCH-5001 | ReindexFailed            | 500  | Medium   | No        |
/// | SCH-5002 | AliasSwapFailed          | 500  | **High** | No        |
/// | SCH-5003 | IndexMappingConflict     | 409  | Medium   | No        |
/// | SCH-8001 | EventDecodeFailed        | 422  | Medium   | No        |
/// | SCH-8002 | UnknownEventType         | 422  | Low      | No        |
/// | SCH-8003 | UnmappedSource           | 422  | Medium   | No        |
/// | SCH-8004 | SourceNotConverged       | 503  | Medium   | **Yes**   |
/// | SCH-9001 | DomainViolation          | 422  | Medium   | No        |
/// | SCH-9002 | InvalidIdentifier        | 422  | Low      | No        |
/// | SCH-9003 | EventConsumeFailed       | 500  | Medium   | No        |
/// | VAL-*    | Validation (delegated)   | 422  | Low      | No        |
///
/// > **Fail-open semantics.** Search is a derived, best-effort read-model.
/// > `EngineUnavailable` / `EngineTimeout` are *transient* faults: the query path
/// > degrades (empty / partial results), it never blocks an upstream write. The
/// > `is_retryable` flags drive the `run_consumer` retry/DLQ classification on the
/// > ingestion side (Phase 4) — a stale-version write (`SCH-2002`) is a terminal,
/// > harmless skip (external versioning rejected an out-of-order event), whereas a
/// > transient engine fault is retried then dead-lettered.
#[derive(Debug, Error)]
#[non_exhaustive]
pub enum SearchError {
    // ── Delegated ─────────────────────────────────────────────────────────────
    #[error(transparent)]
    Validation(#[from] validation::ValidationError),

    // ── Query / parse (SCH-1xxx) ──────────────────────────────────────────────
    #[error("invalid search query: {reason}")]
    InvalidQuery { reason: String },

    #[error("invalid pagination cursor")]
    InvalidCursor,

    #[error("unsupported entity type: '{entity_type}'")]
    UnsupportedEntityType { entity_type: String },

    // ── Index / upsert (SCH-2xxx) ─────────────────────────────────────────────
    #[error("indexed document not found: {id}")]
    DocumentNotFound { id: String },

    /// An out-of-order or replayed event whose `doc_version` is not newer than the
    /// stored document; rejected by external versioning. Typically folded into an
    /// `Ok` at the consumer so the offset still commits.
    #[error("stale document version; a newer revision is already indexed")]
    StaleVersion,

    #[error("bulk index operation failed: {reason}")]
    BulkIndexFailed { reason: String },

    // ── Projection / transform (SCH-3xxx) ─────────────────────────────────────
    #[error("failed to project '{entity_type}' event into an index document: {reason}")]
    ProjectionFailed { entity_type: String, reason: String },

    #[error("event is missing a field required to build the index document: '{field}'")]
    MissingProjectionField { field: String },

    // ── Engine availability · the fail-open core (SCH-4xxx) ────────────────────
    #[error("the search engine is unavailable")]
    EngineUnavailable,

    #[error("the search engine query timed out")]
    EngineTimeout,

    /// A configured index / alias does not exist — a deployment or migration
    /// fault, not a transient one (no point retrying the same request).
    #[error("search index or alias not found: '{index}'")]
    IndexNotFound { index: String },

    // ── Reindex / alias / migration · Phase-7 ops (SCH-5xxx) ──────────────────
    #[error("reindex job failed: {reason}")]
    ReindexFailed { reason: String },

    #[error("alias swap failed: {reason}")]
    AliasSwapFailed { reason: String },

    #[error("index mapping conflict: {reason}")]
    IndexMappingConflict { reason: String },

    // ── Inbound event decode / source mapping (SCH-8xxx) ──────────────────────
    #[error("failed to decode event from topic '{topic}': {reason}")]
    EventDecodeFailed { topic: String, reason: String },

    /// An event type this consumer does not project; usually folded into an `Ok`
    /// skip rather than dead-lettered.
    #[error("unknown event type: '{event_type}'")]
    UnknownEventType { event_type: String },

    #[error("source event could not be mapped to an indexable entity: {reason}")]
    UnmappedSource { reason: String },

    /// The source service has not caught up with the event yet (a post whose
    /// takedown was lifted still reads restricted from `post`): retried.
    #[error("the source of '{id}' has not caught up with the event yet")]
    SourceNotConverged { id: String },

    // ── Cross-cutting (SCH-9xxx) ──────────────────────────────────────────────
    #[error("domain invariant violated on '{field}': {message}")]
    DomainViolation { field: String, message: String },

    #[error("invalid identifier: '{0}'")]
    InvalidIdentifier(String),

    #[error("failed to consume event: {0}")]
    EventConsumeFailed(String),
}

impl AppError for SearchError {
    fn error_code(&self) -> &'static str {
        match self {
            SearchError::Validation(e) => e.error_code(),

            SearchError::InvalidQuery { .. } => "SCH-1001",
            SearchError::InvalidCursor => "SCH-1002",
            SearchError::UnsupportedEntityType { .. } => "SCH-1003",

            SearchError::DocumentNotFound { .. } => "SCH-2001",
            SearchError::StaleVersion => "SCH-2002",
            SearchError::BulkIndexFailed { .. } => "SCH-2003",

            SearchError::ProjectionFailed { .. } => "SCH-3001",
            SearchError::MissingProjectionField { .. } => "SCH-3002",

            SearchError::EngineUnavailable => "SCH-4001",
            SearchError::EngineTimeout => "SCH-4002",
            SearchError::IndexNotFound { .. } => "SCH-4003",

            SearchError::ReindexFailed { .. } => "SCH-5001",
            SearchError::AliasSwapFailed { .. } => "SCH-5002",
            SearchError::IndexMappingConflict { .. } => "SCH-5003",

            SearchError::EventDecodeFailed { .. } => "SCH-8001",
            SearchError::UnknownEventType { .. } => "SCH-8002",
            SearchError::UnmappedSource { .. } => "SCH-8003",
            SearchError::SourceNotConverged { .. } => "SCH-8004",

            SearchError::DomainViolation { .. } => "SCH-9001",
            SearchError::InvalidIdentifier(_) => "SCH-9002",
            SearchError::EventConsumeFailed(_) => "SCH-9003",
        }
    }

    fn http_status(&self) -> StatusCode {
        match self {
            SearchError::Validation(e) => e.http_status(),

            SearchError::DocumentNotFound { .. } => StatusCode::NOT_FOUND,

            SearchError::StaleVersion | SearchError::IndexMappingConflict { .. } => {
                StatusCode::CONFLICT
            }

            SearchError::EngineUnavailable
            | SearchError::IndexNotFound { .. }
            | SearchError::SourceNotConverged { .. } => StatusCode::SERVICE_UNAVAILABLE,

            SearchError::EngineTimeout => StatusCode::GATEWAY_TIMEOUT,

            SearchError::BulkIndexFailed { .. }
            | SearchError::ReindexFailed { .. }
            | SearchError::AliasSwapFailed { .. }
            | SearchError::EventConsumeFailed(_) => StatusCode::INTERNAL_SERVER_ERROR,

            _ => StatusCode::UNPROCESSABLE_ENTITY,
        }
    }

    fn severity(&self) -> Severity {
        match self {
            SearchError::Validation(e) => e.severity(),

            SearchError::EngineUnavailable
            | SearchError::EngineTimeout
            | SearchError::IndexNotFound { .. }
            | SearchError::AliasSwapFailed { .. } => Severity::High,

            SearchError::BulkIndexFailed { .. }
            | SearchError::ProjectionFailed { .. }
            | SearchError::MissingProjectionField { .. }
            | SearchError::ReindexFailed { .. }
            | SearchError::IndexMappingConflict { .. }
            | SearchError::EventDecodeFailed { .. }
            | SearchError::UnmappedSource { .. }
            | SearchError::SourceNotConverged { .. }
            | SearchError::DomainViolation { .. }
            | SearchError::EventConsumeFailed(_) => Severity::Medium,

            _ => Severity::Low,
        }
    }

    fn is_retryable(&self) -> bool {
        match self {
            SearchError::Validation(e) => e.is_retryable(),
            SearchError::EngineUnavailable
            | SearchError::EngineTimeout
            | SearchError::BulkIndexFailed { .. }
            | SearchError::SourceNotConverged { .. } => true,
            _ => false,
        }
    }

    fn category(&self) -> &'static str {
        match self {
            SearchError::Validation(e) => e.category(),
            _ => "SCH",
        }
    }

    fn user_facing_message(&self) -> &'static str {
        match self {
            SearchError::Validation(e) => e.user_facing_message(),

            SearchError::InvalidQuery { .. }
            | SearchError::InvalidCursor
            | SearchError::UnsupportedEntityType { .. } => "Your search could not be processed.",

            SearchError::EngineUnavailable
            | SearchError::EngineTimeout
            | SearchError::IndexNotFound { .. } => {
                "Search is temporarily unavailable. Please try again."
            }

            SearchError::DocumentNotFound { .. } => "The requested item does not exist.",

            _ => "An internal search error occurred.",
        }
    }
}
