use error::{AppError, Severity};
use http::StatusCode;
use thiserror::Error;

/// Canonical domain and application error type for the audit & compliance
/// microservice.
///
/// The `AUD-XXXX` namespace is grouped by concern so a code alone localizes the
/// fault: 1xxx event intake / contract validation, 2xxx **ledger integrity**
/// (the tamper-evidence core — hash chain, sequence gaps, checkpoint/witness
/// divergence), 3xxx audit-read authorization (the query/export surface is
/// privileged and itself audited), 4xxx storage-plane availability (the
/// durability core: append-only ledger, WORM archive, key vault, durable-commit
/// deadline), 5xxx crypto-shred / key lifecycle (the GDPR erasure pattern), 6xxx
/// retention / legal hold, 8xxx the async ingestion (`run_consumer`) surface, and
/// 9xxx cross-cutting (domain/parse).
///
/// ## Code catalogue
///
/// | Code     | Variant                       | HTTP | Severity | Retryable |
/// |----------|-------------------------------|------|----------|-----------|
/// | AUD-1001 | MalformedAuditEvent           | 400  | Medium   | No        |
/// | AUD-1002 | MissingLawfulBasis            | 422  | Medium   | No        |
/// | AUD-1003 | UnknownEventCategory          | 422  | Low      | No        |
/// | AUD-1004 | DuplicateEvent                | 409  | Low      | No        |
/// | AUD-2001 | ChainHashMismatch             | 500  | **High** | No        |
/// | AUD-2002 | SequenceGap                   | 500  | **High** | No        |
/// | AUD-2003 | ChainHeadConflict             | 409  | Medium   | **Yes**   |
/// | AUD-2004 | CheckpointVerificationFailed  | 500  | **High** | No        |
/// | AUD-2005 | AnchorWitnessUnavailable      | 503  | **High** | **Yes**   |
/// | AUD-3001 | QueryForbidden                | 403  | **High** | No        |
/// | AUD-3002 | ExportForbidden               | 403  | **High** | No        |
/// | AUD-3003 | SubjectScopeViolation         | 403  | **High** | No        |
/// | AUD-3004 | CallerUnauthenticated         | 401  | **High** | No        |
/// | AUD-3005 | RecordForbidden               | 403  | **High** | No        |
/// | AUD-3006 | CallerCheckUnavailable        | 503  | **High** | **Yes**   |
/// | AUD-4001 | LedgerStoreUnavailable        | 503  | **High** | **Yes**   |
/// | AUD-4002 | ArchiveUnavailable            | 503  | **High** | **Yes**   |
/// | AUD-4003 | KeyVaultUnavailable           | 503  | **High** | **Yes**   |
/// | AUD-4004 | DurabilityNotConfirmed        | 504  | **High** | **Yes**   |
/// | AUD-5001 | SubjectKeyNotFound            | 404  | Medium   | No        |
/// | AUD-5002 | ShredBlockedByLegalHold       | 409  | **High** | No        |
/// | AUD-5003 | CryptoShredFailed             | 500  | **High** | **Yes**   |
/// | AUD-5004 | PiiEnvelopeUndecryptable      | 410  | Low      | No        |
/// | AUD-6001 | RetentionFloorViolation       | 422  | **High** | No        |
/// | AUD-6002 | LegalHoldActive               | 409  | Medium   | No        |
/// | AUD-6003 | RetentionPolicyNotFound       | 422  | Medium   | No        |
/// | AUD-8001 | EventDecodeFailed             | 422  | Medium   | No        |
/// | AUD-8002 | UnrecordableEvent             | 422  | Low      | No        |
/// | AUD-8003 | EventConsumeFailed            | 500  | Medium   | **Yes**   |
/// | AUD-9001 | DomainViolation               | 422  | Medium   | No        |
/// | AUD-9002 | InvalidIdentifier             | 422  | Low      | No        |
/// | VAL-*    | Validation (delegated)        | 422  | Low      | No        |
///
/// > **Split posture: fail-open at producers, fail-closed on durability.** The
/// > async ingestion lane is **fail-open for the business mesh** — a producer
/// > never blocks on audit; Kafka is the durable buffer, so a write spike becomes
/// > consumer *lag*, not producer backpressure. On the worker, the `4xxx`
/// > storage-plane faults and `AUD-8003` are *transient* and retryable, driving
/// > `run_consumer` retry/DLQ so no committed offset ever advances past an
/// > un-persisted event (zero loss). But the **synchronous** `RecordPrivileged`
/// > lane is **fail-closed**: `AUD-4004` (durability not confirmed within the
/// > deadline) denies the privileged action rather than letting it proceed
/// > unrecorded. The `2xxx` **integrity** faults are deliberately **not
/// > retryable** — a hash mismatch, a sequence gap, or a witness divergence is a
/// > tampering/truncation signal that must *alarm*, never silently retry; and the
/// > `3xxx` authorization denials are security-relevant (the audit-read surface is
/// > need-to-know, and every read is itself an audit event). `AUD-5004` is the
/// > *expected* post-erasure state — the per-subject DEK was crypto-shredded, so a
/// > PII envelope is permanently undecryptable while the record and its chain
/// > remain intact and verifiable.
#[derive(Debug, Error)]
#[non_exhaustive]
pub enum AuditError {
    // ── Delegated ─────────────────────────────────────────────────────────────
    #[error(transparent)]
    Validation(#[from] validation::ValidationError),

    // ── Event intake / contract validation (AUD-1xxx) ─────────────────────────
    #[error("malformed audit event: {reason}")]
    MalformedAuditEvent { reason: String },

    /// A compliance event arrived without the lawful-basis / category metadata the
    /// contract requires — it cannot be classified for retention or reporting.
    #[error("audit event is missing its required lawful basis / category")]
    MissingLawfulBasis,

    #[error("unknown audit event category: '{category}'")]
    UnknownEventCategory { category: String },

    /// Idempotency: an event with this deterministic id is already chained. A
    /// harmless skip — folded into `Ok` so the offset still commits.
    #[error("duplicate audit event '{event_id}' already recorded")]
    DuplicateEvent { event_id: String },

    // ── Ledger integrity · the tamper-evidence core (AUD-2xxx) ────────────────
    /// A recomputed record hash does not match the stored hash — a tampering
    /// indicator. Must alarm, never retry.
    #[error("ledger hash chain mismatch at sequence {sequence}")]
    ChainHashMismatch { sequence: u64 },

    /// The monotonic per-partition sequence has a hole — a truncation indicator.
    /// Must alarm, never retry.
    #[error("ledger sequence gap detected in partition '{partition}'")]
    SequenceGap { partition: String },

    /// A concurrent append raced the partition chain head; re-read the head and
    /// re-chain. The only retryable integrity fault.
    #[error("ledger chain head conflict in partition '{partition}'")]
    ChainHeadConflict { partition: String },

    /// A signed Merkle checkpoint does not match the externally-anchored witness —
    /// an operator-level tampering indicator (the storage operator is assumed
    /// potentially hostile). Must alarm, never retry.
    #[error("checkpoint verification failed against the external witness")]
    CheckpointVerificationFailed,

    /// The external anchor / trusted-timestamp authority is unreachable; the
    /// periodic checkpoint cannot be witnessed yet.
    #[error("the external anchor/witness is unavailable")]
    AnchorWitnessUnavailable,

    // ── Audit-read authorization · the privileged read surface (AUD-3xxx) ─────
    /// The caller lacks audit-read authority. Need-to-know + separation of duties;
    /// security-relevant, and this denial is itself recorded.
    #[error("audit query is forbidden for this caller")]
    QueryForbidden,

    #[error("audit export is forbidden for this caller")]
    ExportForbidden,

    /// A query/export crossed a tenant or subject boundary it is not scoped to.
    #[error("audit access crosses a subject/tenant scope it is not authorized for")]
    SubjectScopeViolation,

    /// The caller presented no verifiable identity (missing, malformed, expired,
    /// or unverifiable Bearer token). The privileged surface fails CLOSED —
    /// including when the verification backend (JWKS) is not yet configured.
    #[error("audit caller is not authenticated")]
    CallerUnauthenticated,

    /// The caller is authenticated but lacks the authority to record on the
    /// synchronous privileged lane.
    #[error("recording on the privileged audit lane is forbidden for this caller")]
    RecordForbidden,

    /// The caller's token verified, but whether `auth` revoked it since could
    /// not be read (#903). The privileged surface fails CLOSED; retry.
    #[error("the audit caller's token revocation state is unavailable")]
    CallerCheckUnavailable,

    // ── Storage-plane availability · the durability core (AUD-4xxx) ───────────
    /// The append-only ledger store (Postgres, UPDATE/DELETE revoked) is
    /// unreachable — the record cannot be durably committed.
    #[error("the append-only ledger store is unavailable")]
    LedgerStoreUnavailable,

    /// The WORM archive (S3/MinIO Object Lock, compliance mode) is unreachable.
    #[error("the WORM archive is unavailable")]
    ArchiveUnavailable,

    /// The KMS/HSM signer or per-subject DEK vault is unreachable.
    #[error("the key vault is unavailable")]
    KeyVaultUnavailable,

    /// Durability was not confirmed (persisted + chained) within the deadline. On
    /// the **synchronous** break-glass lane this fails *closed* — the privileged
    /// action is denied rather than performed unrecorded.
    #[error("audit durability was not confirmed within the deadline")]
    DurabilityNotConfirmed,

    // ── Crypto-shred / key lifecycle · the GDPR erasure pattern (AUD-5xxx) ─────
    /// No per-subject DEK exists (already shredded, or never minted).
    #[error("no encryption key found for subject '{subject}'")]
    SubjectKeyNotFound { subject: String },

    /// Erasure was requested for a subject under an active legal hold; lawful
    /// retention (GDPR Art. 17(3)) overrides the erasure request.
    #[error("crypto-shred blocked: subject '{subject}' is under an active legal hold")]
    ShredBlockedByLegalHold { subject: String },

    /// The per-subject DEK destruction did not complete; erasure is not yet
    /// guaranteed and must be retried.
    #[error("crypto-shred failed for subject '{subject}'")]
    CryptoShredFailed { subject: String },

    /// A PII envelope is permanently undecryptable because its per-subject DEK was
    /// crypto-shredded — the *expected* post-erasure state. The record and its
    /// hash chain remain intact and verifiable.
    #[error("PII envelope is undecryptable (subject erased via crypto-shred)")]
    PiiEnvelopeUndecryptable,

    // ── Retention / legal hold (AUD-6xxx) ─────────────────────────────────────
    /// An operation would expire/remove a record before its retention floor.
    #[error("operation violates the retention floor for this record")]
    RetentionFloorViolation,

    /// A mutation/expiry is blocked by an active legal hold.
    #[error("operation blocked by an active legal hold")]
    LegalHoldActive,

    #[error("no retention policy resolved for category '{category}'")]
    RetentionPolicyNotFound { category: String },

    // ── Async ingestion · the run_consumer surface (AUD-8xxx) ─────────────────
    #[error("failed to decode audit event from topic '{topic}': {reason}")]
    EventDecodeFailed { topic: String, reason: String },

    /// The event carries nothing recordable (no compliance-relevant content); a
    /// harmless skip folded into `Ok` so the offset still commits.
    #[error("event carries nothing recordable: {reason}")]
    UnrecordableEvent { reason: String },

    /// A transient failure consuming/recording an event; retried by `run_consumer`.
    #[error("failed to consume audit event: {0}")]
    EventConsumeFailed(String),

    // ── Cross-cutting (AUD-9xxx) ──────────────────────────────────────────────
    #[error("domain invariant violated on '{field}': {message}")]
    DomainViolation { field: String, message: String },

    #[error("invalid identifier: '{0}'")]
    InvalidIdentifier(String),
}

impl AppError for AuditError {
    fn error_code(&self) -> &'static str {
        match self {
            AuditError::Validation(e) => e.error_code(),

            AuditError::MalformedAuditEvent { .. } => "AUD-1001",
            AuditError::MissingLawfulBasis => "AUD-1002",
            AuditError::UnknownEventCategory { .. } => "AUD-1003",
            AuditError::DuplicateEvent { .. } => "AUD-1004",

            AuditError::ChainHashMismatch { .. } => "AUD-2001",
            AuditError::SequenceGap { .. } => "AUD-2002",
            AuditError::ChainHeadConflict { .. } => "AUD-2003",
            AuditError::CheckpointVerificationFailed => "AUD-2004",
            AuditError::AnchorWitnessUnavailable => "AUD-2005",

            AuditError::QueryForbidden => "AUD-3001",
            AuditError::ExportForbidden => "AUD-3002",
            AuditError::SubjectScopeViolation => "AUD-3003",
            AuditError::CallerUnauthenticated => "AUD-3004",
            AuditError::RecordForbidden => "AUD-3005",
            AuditError::CallerCheckUnavailable => "AUD-3006",

            AuditError::LedgerStoreUnavailable => "AUD-4001",
            AuditError::ArchiveUnavailable => "AUD-4002",
            AuditError::KeyVaultUnavailable => "AUD-4003",
            AuditError::DurabilityNotConfirmed => "AUD-4004",

            AuditError::SubjectKeyNotFound { .. } => "AUD-5001",
            AuditError::ShredBlockedByLegalHold { .. } => "AUD-5002",
            AuditError::CryptoShredFailed { .. } => "AUD-5003",
            AuditError::PiiEnvelopeUndecryptable => "AUD-5004",

            AuditError::RetentionFloorViolation => "AUD-6001",
            AuditError::LegalHoldActive => "AUD-6002",
            AuditError::RetentionPolicyNotFound { .. } => "AUD-6003",

            AuditError::EventDecodeFailed { .. } => "AUD-8001",
            AuditError::UnrecordableEvent { .. } => "AUD-8002",
            AuditError::EventConsumeFailed(_) => "AUD-8003",

            AuditError::DomainViolation { .. } => "AUD-9001",
            AuditError::InvalidIdentifier(_) => "AUD-9002",
        }
    }

    fn http_status(&self) -> StatusCode {
        match self {
            AuditError::Validation(e) => e.http_status(),

            AuditError::MalformedAuditEvent { .. } => StatusCode::BAD_REQUEST,

            AuditError::QueryForbidden
            | AuditError::ExportForbidden
            | AuditError::SubjectScopeViolation
            | AuditError::RecordForbidden => StatusCode::FORBIDDEN,

            AuditError::CallerUnauthenticated => StatusCode::UNAUTHORIZED,

            AuditError::DuplicateEvent { .. }
            | AuditError::ChainHeadConflict { .. }
            | AuditError::ShredBlockedByLegalHold { .. }
            | AuditError::LegalHoldActive => StatusCode::CONFLICT,

            AuditError::LedgerStoreUnavailable
            | AuditError::ArchiveUnavailable
            | AuditError::KeyVaultUnavailable
            | AuditError::AnchorWitnessUnavailable
            | AuditError::CallerCheckUnavailable => StatusCode::SERVICE_UNAVAILABLE,

            AuditError::DurabilityNotConfirmed => StatusCode::GATEWAY_TIMEOUT,

            AuditError::SubjectKeyNotFound { .. } => StatusCode::NOT_FOUND,
            AuditError::PiiEnvelopeUndecryptable => StatusCode::GONE,

            AuditError::ChainHashMismatch { .. }
            | AuditError::SequenceGap { .. }
            | AuditError::CheckpointVerificationFailed
            | AuditError::CryptoShredFailed { .. }
            | AuditError::EventConsumeFailed(_) => StatusCode::INTERNAL_SERVER_ERROR,

            _ => StatusCode::UNPROCESSABLE_ENTITY,
        }
    }

    fn severity(&self) -> Severity {
        match self {
            AuditError::Validation(e) => e.severity(),

            // Tamper / truncation / divergence indicators, authz denials, the
            // durability core, and the lawful-retention override are all High.
            AuditError::ChainHashMismatch { .. }
            | AuditError::SequenceGap { .. }
            | AuditError::CheckpointVerificationFailed
            | AuditError::AnchorWitnessUnavailable
            | AuditError::QueryForbidden
            | AuditError::ExportForbidden
            | AuditError::SubjectScopeViolation
            | AuditError::CallerUnauthenticated
            | AuditError::RecordForbidden
            | AuditError::CallerCheckUnavailable
            | AuditError::LedgerStoreUnavailable
            | AuditError::ArchiveUnavailable
            | AuditError::KeyVaultUnavailable
            | AuditError::DurabilityNotConfirmed
            | AuditError::ShredBlockedByLegalHold { .. }
            | AuditError::CryptoShredFailed { .. }
            | AuditError::RetentionFloorViolation => Severity::High,

            AuditError::MalformedAuditEvent { .. }
            | AuditError::MissingLawfulBasis
            | AuditError::ChainHeadConflict { .. }
            | AuditError::SubjectKeyNotFound { .. }
            | AuditError::LegalHoldActive
            | AuditError::RetentionPolicyNotFound { .. }
            | AuditError::EventDecodeFailed { .. }
            | AuditError::EventConsumeFailed(_)
            | AuditError::DomainViolation { .. } => Severity::Medium,

            _ => Severity::Low,
        }
    }

    fn is_retryable(&self) -> bool {
        match self {
            AuditError::Validation(e) => e.is_retryable(),
            // Storage-plane availability + the durable-commit deadline + a raced
            // chain head + an incomplete shred drive retry. Integrity faults
            // (2001/2002/2004) deliberately do NOT — they must alarm, not retry.
            AuditError::LedgerStoreUnavailable
            | AuditError::ArchiveUnavailable
            | AuditError::KeyVaultUnavailable
            | AuditError::AnchorWitnessUnavailable
            | AuditError::DurabilityNotConfirmed
            | AuditError::CallerCheckUnavailable
            | AuditError::ChainHeadConflict { .. }
            | AuditError::CryptoShredFailed { .. }
            | AuditError::EventConsumeFailed(_) => true,
            _ => false,
        }
    }

    fn category(&self) -> &'static str {
        match self {
            AuditError::Validation(e) => e.category(),
            _ => "AUD",
        }
    }

    fn user_facing_message(&self) -> &'static str {
        match self {
            AuditError::Validation(e) => e.user_facing_message(),

            AuditError::QueryForbidden
            | AuditError::ExportForbidden
            | AuditError::SubjectScopeViolation
            | AuditError::CallerUnauthenticated
            | AuditError::RecordForbidden => {
                "You are not authorized to access audit records."
            }

            AuditError::LedgerStoreUnavailable
            | AuditError::ArchiveUnavailable
            | AuditError::KeyVaultUnavailable
            | AuditError::AnchorWitnessUnavailable
            | AuditError::CallerCheckUnavailable
            | AuditError::DurabilityNotConfirmed => {
                "The compliance audit plane is temporarily unavailable."
            }

            AuditError::ChainHashMismatch { .. }
            | AuditError::SequenceGap { .. }
            | AuditError::CheckpointVerificationFailed => {
                "An audit integrity check failed and has been escalated."
            }

            _ => "An audit processing error occurred.",
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Every variant must carry a stable, correctly-prefixed `AUD-XXXX` code and
    /// agree with the documented split posture — fail-open/retryable on the
    /// storage-plane lane that drives `run_consumer`, fail-closed and
    /// non-retryable on the integrity and authorization classes.
    #[test]
    fn codes_are_stable_and_prefixed() {
        // Storage-plane availability → retryable, drives consumer retry / no loss.
        let ledger_down = AuditError::LedgerStoreUnavailable;
        assert_eq!(ledger_down.error_code(), "AUD-4001");
        assert!(ledger_down.is_retryable());
        assert_eq!(ledger_down.http_status(), StatusCode::SERVICE_UNAVAILABLE);
        assert_eq!(ledger_down.severity(), Severity::High);
        assert_eq!(ledger_down.category(), "AUD");

        // The synchronous break-glass lane fails CLOSED: durable commit not
        // confirmed → deny the action (the caller retries / aborts).
        let not_durable = AuditError::DurabilityNotConfirmed;
        assert_eq!(not_durable.error_code(), "AUD-4004");
        assert!(not_durable.is_retryable());
        assert_eq!(not_durable.http_status(), StatusCode::GATEWAY_TIMEOUT);

        // Tampering indicator → High severity, MUST NOT retry (must alarm).
        let tamper = AuditError::ChainHashMismatch { sequence: 42 };
        assert_eq!(tamper.error_code(), "AUD-2001");
        assert!(!tamper.is_retryable());
        assert_eq!(tamper.severity(), Severity::High);

        // Truncation indicator → High severity, MUST NOT retry.
        let gap = AuditError::SequenceGap {
            partition: "tenant-7".into(),
        };
        assert_eq!(gap.error_code(), "AUD-2002");
        assert!(!gap.is_retryable());

        // Audit-read denial is security-relevant and itself audited.
        let forbidden = AuditError::QueryForbidden;
        assert_eq!(forbidden.error_code(), "AUD-3001");
        assert_eq!(forbidden.http_status(), StatusCode::FORBIDDEN);
        assert_eq!(forbidden.severity(), Severity::High);
        assert!(!forbidden.is_retryable());

        // No verifiable caller identity → 401, fail-closed, never retried by the
        // transport (the caller must obtain a token, not hammer the gate).
        let unauthenticated = AuditError::CallerUnauthenticated;
        assert_eq!(unauthenticated.error_code(), "AUD-3004");
        assert_eq!(unauthenticated.http_status(), StatusCode::UNAUTHORIZED);
        assert_eq!(unauthenticated.severity(), Severity::High);
        assert!(!unauthenticated.is_retryable());

        // Authenticated but not authorized to record on the privileged lane.
        let record_denied = AuditError::RecordForbidden;
        assert_eq!(record_denied.error_code(), "AUD-3005");
        assert_eq!(record_denied.http_status(), StatusCode::FORBIDDEN);
        assert_eq!(record_denied.severity(), Severity::High);
        assert!(!record_denied.is_retryable());

        // GDPR erasure vs audit: lawful retention (Art. 17(3)) overrides erasure.
        let held = AuditError::ShredBlockedByLegalHold {
            subject: "7f3a".into(),
        };
        assert_eq!(held.error_code(), "AUD-5002");
        assert!(!held.is_retryable());

        // Expected post-erasure state: DEK shredded → ciphertext unreadable, but
        // the record + chain remain. Not an error to retry.
        let shredded = AuditError::PiiEnvelopeUndecryptable;
        assert_eq!(shredded.error_code(), "AUD-5004");
        assert_eq!(shredded.http_status(), StatusCode::GONE);
        assert!(!shredded.is_retryable());

        // Poison upstream event → DLQ, never an infinite retry.
        let poison = AuditError::EventDecodeFailed {
            topic: "moderation.v1.events".into(),
            reason: "bad frame".into(),
        };
        assert_eq!(poison.error_code(), "AUD-8001");
        assert!(!poison.is_retryable());

        // Nothing recordable → folded into an Ok skip at the consumer.
        let skip = AuditError::UnrecordableEvent {
            reason: "no compliance-relevant content".into(),
        };
        assert_eq!(skip.error_code(), "AUD-8002");
        assert!(!skip.is_retryable());
    }
}
