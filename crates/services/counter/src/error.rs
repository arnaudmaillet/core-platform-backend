use error::{AppError, Severity};
use http::StatusCode;
use thiserror::Error;

/// Canonical domain and application error type for the counter-analytics
/// microservice.
///
/// The `CTR-XXXX` namespace is grouped by concern so a code alone localizes the
/// fault: 1xxx read/query, 2xxx aggregation/window, 3xxx flush/write-behind,
/// 4xxx store availability (the fail-open core), 5xxx reconciliation/drift (the
/// Phase-7 correctness surface), 8xxx inbound event decode / source mapping,
/// 9xxx cross-cutting (domain/parse, event consumption).
///
/// ## Code catalogue
///
/// | Code     | Variant                  | HTTP | Severity | Retryable |
/// |----------|--------------------------|------|----------|-----------|
/// | CTR-1001 | InvalidCounterQuery      | 422  | Low      | No        |
/// | CTR-1002 | UnsupportedMetric        | 422  | Low      | No        |
/// | CTR-1003 | InvalidTimeRange         | 422  | Low      | No        |
/// | CTR-1004 | InvalidTrendingScope     | 422  | Low      | No        |
/// | CTR-2001 | WindowAggregationFailed  | 500  | Medium   | No        |
/// | CTR-2002 | InvalidDelta             | 422  | Medium   | No        |
/// | CTR-2003 | ProbabilisticError       | 500  | Medium   | No        |
/// | CTR-3001 | FlushFailed              | 500  | **High** | **Yes**   |
/// | CTR-3002 | CacheWriteFailed         | 500  | Medium   | **Yes**   |
/// | CTR-3003 | SignalPublishFailed      | 500  | Medium   | **Yes**   |
/// | CTR-4001 | HotStoreUnavailable      | 503  | **High** | **Yes**   |
/// | CTR-4002 | LedgerUnavailable        | 503  | **High** | **Yes**   |
/// | CTR-4003 | TimeSeriesUnavailable    | 503  | **High** | **Yes**   |
/// | CTR-4004 | StoreTimeout             | 504  | **High** | **Yes**   |
/// | CTR-5001 | ReconciliationFailed     | 500  | Medium   | No        |
/// | CTR-5002 | DriftThresholdExceeded   | 500  | **High** | No        |
/// | CTR-5003 | SourceReplayFailed       | 500  | Medium   | **Yes**   |
/// | CTR-8001 | EventDecodeFailed        | 422  | Medium   | No        |
/// | CTR-8002 | UnknownEventType         | 422  | Low      | No        |
/// | CTR-8003 | UnmappedMetric           | 422  | Medium   | No        |
/// | CTR-9001 | DomainViolation          | 422  | Medium   | No        |
/// | CTR-9002 | InvalidIdentifier        | 422  | Low      | No        |
/// | CTR-9003 | EventConsumeFailed       | 500  | Medium   | No        |
/// | VAL-*    | Validation (delegated)   | 422  | Low      | No        |
///
/// > **Fail-open semantics.** Counter-analytics is a derived, best-effort
/// > read-model. The `4xxx` store faults are *transient*: on the **read** path
/// > they degrade to a stale-but-served snapshot (never a 5xx that blocks a feed
/// > render); on the **ingestion** path their `is_retryable` flags drive the
/// > `run_consumer` retry/DLQ classification — a redelivered event re-flushes the
/// > same idempotent window, a poison event (`CTR-8001`) is dead-lettered, and an
/// > unmapped/unknown event (`CTR-8002`) is a harmless skip folded into `Ok` so
/// > the offset still commits. Counts are eventually consistent and periodically
/// > reconciled (`5xxx`); they are never in a synchronous write path.
#[derive(Debug, Error)]
#[non_exhaustive]
pub enum CounterError {
    // ── Delegated ─────────────────────────────────────────────────────────────
    #[error(transparent)]
    Validation(#[from] validation::ValidationError),

    // ── Read / query (CTR-1xxx) ───────────────────────────────────────────────
    #[error("invalid counter query: {reason}")]
    InvalidCounterQuery { reason: String },

    #[error("unsupported metric: '{metric}'")]
    UnsupportedMetric { metric: String },

    #[error("invalid time-series range or granularity: {reason}")]
    InvalidTimeRange { reason: String },

    #[error("invalid trending scope: '{scope}'")]
    InvalidTrendingScope { scope: String },

    // ── Aggregation / window (CTR-2xxx) ───────────────────────────────────────
    #[error("failed to aggregate counter window: {reason}")]
    WindowAggregationFailed { reason: String },

    #[error("invalid counter delta: {reason}")]
    InvalidDelta { reason: String },

    /// A HyperLogLog / Count-Min-Sketch operation failed (encode/decode/merge).
    #[error("probabilistic structure operation failed: {reason}")]
    ProbabilisticError { reason: String },

    // ── Flush / write-behind (CTR-3xxx) ───────────────────────────────────────
    /// A batched, window-keyed flush to the durable tier (Postgres ledger / Scylla
    /// time-series) failed. Retryable: the flush is idempotent on `(entity, metric,
    /// window_id)`, so a replay re-applies the same window without double-counting.
    #[error("durable flush failed: {reason}")]
    FlushFailed { reason: String },

    #[error("hot-store write failed: {reason}")]
    CacheWriteFailed { reason: String },

    #[error("failed to publish the popularity signal: {reason}")]
    SignalPublishFailed { reason: String },

    // ── Store availability · the fail-open core (CTR-4xxx) ────────────────────
    #[error("the hot counter store (Redis) is unavailable")]
    HotStoreUnavailable,

    #[error("the warm counter ledger (Postgres) is unavailable")]
    LedgerUnavailable,

    #[error("the cold time-series store (Scylla) is unavailable")]
    TimeSeriesUnavailable,

    #[error("a counter store operation timed out")]
    StoreTimeout,

    // ── Reconciliation / drift · Phase-7 correctness (CTR-5xxx) ───────────────
    #[error("reconciliation run failed: {reason}")]
    ReconciliationFailed { reason: String },

    /// The drift between the fast approximate hot count and the authoritative
    /// replayed total exceeded tolerance — a correctness alarm, not a transient
    /// fault.
    #[error("counter drift exceeded tolerance for '{metric}': {detail}")]
    DriftThresholdExceeded { metric: String, detail: String },

    /// Could not query the owning source-of-record to rebuild an exact count;
    /// retryable (the SoR may be briefly unavailable).
    #[error("source replay for reconciliation failed: {reason}")]
    SourceReplayFailed { reason: String },

    // ── Inbound event decode / source mapping (CTR-8xxx) ──────────────────────
    #[error("failed to decode event from topic '{topic}': {reason}")]
    EventDecodeFailed { topic: String, reason: String },

    /// An event type this consumer does not aggregate; usually folded into an
    /// `Ok` skip rather than dead-lettered.
    #[error("unknown event type: '{event_type}'")]
    UnknownEventType { event_type: String },

    #[error("event could not be mapped to a counter metric: {reason}")]
    UnmappedMetric { reason: String },

    // ── Cross-cutting (CTR-9xxx) ──────────────────────────────────────────────
    #[error("domain invariant violated on '{field}': {message}")]
    DomainViolation { field: String, message: String },

    #[error("invalid identifier: '{0}'")]
    InvalidIdentifier(String),

    #[error("failed to consume event: {0}")]
    EventConsumeFailed(String),
}

impl AppError for CounterError {
    fn error_code(&self) -> &'static str {
        match self {
            CounterError::Validation(e) => e.error_code(),

            CounterError::InvalidCounterQuery { .. } => "CTR-1001",
            CounterError::UnsupportedMetric { .. } => "CTR-1002",
            CounterError::InvalidTimeRange { .. } => "CTR-1003",
            CounterError::InvalidTrendingScope { .. } => "CTR-1004",

            CounterError::WindowAggregationFailed { .. } => "CTR-2001",
            CounterError::InvalidDelta { .. } => "CTR-2002",
            CounterError::ProbabilisticError { .. } => "CTR-2003",

            CounterError::FlushFailed { .. } => "CTR-3001",
            CounterError::CacheWriteFailed { .. } => "CTR-3002",
            CounterError::SignalPublishFailed { .. } => "CTR-3003",

            CounterError::HotStoreUnavailable => "CTR-4001",
            CounterError::LedgerUnavailable => "CTR-4002",
            CounterError::TimeSeriesUnavailable => "CTR-4003",
            CounterError::StoreTimeout => "CTR-4004",

            CounterError::ReconciliationFailed { .. } => "CTR-5001",
            CounterError::DriftThresholdExceeded { .. } => "CTR-5002",
            CounterError::SourceReplayFailed { .. } => "CTR-5003",

            CounterError::EventDecodeFailed { .. } => "CTR-8001",
            CounterError::UnknownEventType { .. } => "CTR-8002",
            CounterError::UnmappedMetric { .. } => "CTR-8003",

            CounterError::DomainViolation { .. } => "CTR-9001",
            CounterError::InvalidIdentifier(_) => "CTR-9002",
            CounterError::EventConsumeFailed(_) => "CTR-9003",
        }
    }

    fn http_status(&self) -> StatusCode {
        match self {
            CounterError::Validation(e) => e.http_status(),

            CounterError::HotStoreUnavailable
            | CounterError::LedgerUnavailable
            | CounterError::TimeSeriesUnavailable => StatusCode::SERVICE_UNAVAILABLE,

            CounterError::StoreTimeout => StatusCode::GATEWAY_TIMEOUT,

            CounterError::WindowAggregationFailed { .. }
            | CounterError::ProbabilisticError { .. }
            | CounterError::FlushFailed { .. }
            | CounterError::CacheWriteFailed { .. }
            | CounterError::SignalPublishFailed { .. }
            | CounterError::ReconciliationFailed { .. }
            | CounterError::DriftThresholdExceeded { .. }
            | CounterError::SourceReplayFailed { .. }
            | CounterError::EventConsumeFailed(_) => StatusCode::INTERNAL_SERVER_ERROR,

            _ => StatusCode::UNPROCESSABLE_ENTITY,
        }
    }

    fn severity(&self) -> Severity {
        match self {
            CounterError::Validation(e) => e.severity(),

            CounterError::FlushFailed { .. }
            | CounterError::HotStoreUnavailable
            | CounterError::LedgerUnavailable
            | CounterError::TimeSeriesUnavailable
            | CounterError::StoreTimeout
            | CounterError::DriftThresholdExceeded { .. } => Severity::High,

            CounterError::WindowAggregationFailed { .. }
            | CounterError::InvalidDelta { .. }
            | CounterError::ProbabilisticError { .. }
            | CounterError::CacheWriteFailed { .. }
            | CounterError::SignalPublishFailed { .. }
            | CounterError::ReconciliationFailed { .. }
            | CounterError::SourceReplayFailed { .. }
            | CounterError::EventDecodeFailed { .. }
            | CounterError::UnmappedMetric { .. }
            | CounterError::DomainViolation { .. }
            | CounterError::EventConsumeFailed(_) => Severity::Medium,

            _ => Severity::Low,
        }
    }

    fn is_retryable(&self) -> bool {
        match self {
            CounterError::Validation(e) => e.is_retryable(),
            CounterError::FlushFailed { .. }
            | CounterError::CacheWriteFailed { .. }
            | CounterError::SignalPublishFailed { .. }
            | CounterError::HotStoreUnavailable
            | CounterError::LedgerUnavailable
            | CounterError::TimeSeriesUnavailable
            | CounterError::StoreTimeout
            | CounterError::SourceReplayFailed { .. } => true,
            _ => false,
        }
    }

    fn category(&self) -> &'static str {
        match self {
            CounterError::Validation(e) => e.category(),
            _ => "CTR",
        }
    }

    fn user_facing_message(&self) -> &'static str {
        match self {
            CounterError::Validation(e) => e.user_facing_message(),

            CounterError::InvalidCounterQuery { .. }
            | CounterError::UnsupportedMetric { .. }
            | CounterError::InvalidTimeRange { .. }
            | CounterError::InvalidTrendingScope { .. } => {
                "Your counter request could not be processed."
            }

            CounterError::HotStoreUnavailable
            | CounterError::LedgerUnavailable
            | CounterError::TimeSeriesUnavailable
            | CounterError::StoreTimeout => {
                "Counts are temporarily unavailable. Please try again."
            }

            _ => "An internal counter error occurred.",
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Every variant must carry a stable, correctly-prefixed `CTR-XXXX` code and
    /// agree with the documented retry classification that drives `run_consumer`.
    #[test]
    fn codes_are_stable_and_prefixed() {
        let store_down = CounterError::HotStoreUnavailable;
        assert_eq!(store_down.error_code(), "CTR-4001");
        assert!(store_down.is_retryable());
        assert_eq!(store_down.http_status(), StatusCode::SERVICE_UNAVAILABLE);
        assert_eq!(store_down.severity(), Severity::High);

        let poison = CounterError::EventDecodeFailed {
            topic: "view.v1.events".into(),
            reason: "bad frame".into(),
        };
        assert_eq!(poison.error_code(), "CTR-8001");
        assert!(!poison.is_retryable()); // poison → DLQ, never an infinite retry

        let skip = CounterError::UnknownEventType {
            event_type: "post.archived".into(),
        };
        assert_eq!(skip.error_code(), "CTR-8002");
        assert!(!skip.is_retryable()); // folded into Ok at the consumer

        assert_eq!(store_down.category(), "CTR");
    }
}
