use error::{AppError, Severity};
use http::StatusCode;
use thiserror::Error;

/// Canonical domain and application error type for the media microservice.
///
/// The `MED-XXXX` namespace is grouped by concern so a code alone localizes the
/// fault: 1xxx upload brokerage / ticket (Plane A), 2xxx asset metadata SoR,
/// 3xxx rendition / transformation pipeline (Plane B), 4xxx object-store adapter
/// (the byte plane — transient infra), 5xxx CDN / delivery / signing (Plane C),
/// 6xxx content validation / probe (the trust-but-verify gate on finalize), 7xxx
/// compliance / Screen (the fail-closed sub-plane), 8xxx inbound event decode /
/// source mapping, 9xxx cross-cutting (domain/parse, concurrency, event I/O).
///
/// ## Code catalogue
///
/// | Code     | Variant                  | HTTP | Severity | Retryable |
/// |----------|--------------------------|------|----------|-----------|
/// | MED-1001 | InvalidMediaKind         | 422  | Low      | No        |
/// | MED-1002 | UnsupportedMimeType      | 415  | Low      | No        |
/// | MED-1003 | UploadSizeExceeded       | 413  | Low      | No        |
/// | MED-1004 | UploadTicketExpired      | 410  | Low      | No        |
/// | MED-1005 | UploadNotFinalized       | 409  | Low      | No        |
/// | MED-1006 | PrivateDocumentsDisabled | 503  | Low      | No        |
/// | MED-2001 | AssetNotFound            | 404  | Low      | No        |
/// | MED-2002 | InvalidStateTransition   | 409  | Medium   | No        |
/// | MED-2003 | ConcurrentModification   | 409  | **High** | **Yes**   |
/// | MED-2004 | AssetAlreadyExists       | 409  | Low      | No        |
/// | MED-3001 | TranscodeFailed          | 500  | Medium   | **Yes**   |
/// | MED-3002 | UnsupportedCodec         | 422  | Medium   | No        |
/// | MED-3003 | RenditionNotFound        | 404  | Low      | No        |
/// | MED-3004 | ProcessingFailed         | 500  | Medium   | **Yes**   |
/// | MED-4001 | ObjectStoreUnavailable   | 503  | **High** | **Yes**   |
/// | MED-4002 | ObjectStoreTimeout       | 504  | **High** | **Yes**   |
/// | MED-4003 | PresignFailed            | 500  | Medium   | **Yes**   |
/// | MED-4004 | ObjectNotFound           | 404  | Low      | No        |
/// | MED-5001 | DeliverySigningFailed    | 500  | Medium   | **Yes**   |
/// | MED-5002 | CdnInvalidationFailed    | 500  | **High** | **Yes**   |
/// | MED-5003 | ManifestBuildFailed      | 500  | Medium   | No        |
/// | MED-6001 | ContentTypeMismatch      | 422  | Medium   | No        |
/// | MED-6002 | CorruptMedia             | 422  | Medium   | No        |
/// | MED-6003 | DimensionLimitExceeded   | 422  | Low      | No        |
/// | MED-6004 | MalwareDetected          | 422  | **High** | No        |
/// | MED-7001 | AssetQuarantined         | 451  | **High** | No        |
/// | MED-7002 | ScreenUnavailable        | 503  | **High** | **Yes**   |
/// | MED-7003 | LegalHoldActive          | 451  | **High** | No        |
/// | MED-8001 | EventDecodeFailed        | 422  | Medium   | No        |
/// | MED-8002 | UnknownEventType         | 422  | Low      | No        |
/// | MED-8003 | UnmappedSource           | 422  | Medium   | No        |
/// | MED-9001 | DomainViolation          | 422  | Medium   | No        |
/// | MED-9002 | InvalidIdentifier        | 422  | Low      | No        |
/// | MED-9003 | EventPublishFailed       | 500  | Medium   | No        |
/// | MED-9004 | EventConsumeFailed       | 500  | Medium   | No        |
/// | DB-*     | Postgres metadata (deleg.) | var | var      | var       |
/// | RDS-*    | Redis delivery cache (deleg.) | var | var   | var       |
/// | VAL-*    | Validation (delegated)   | 422  | Low      | No        |
///
/// > **Mixed failure posture.** Media runs two planes with opposite stances. The
/// > *delivery* plane is **fail-open**: an object-store or CDN fault degrades a
/// > render (placeholder / blurhash), it never blocks an upstream write or login.
/// > The *compliance* gate (MED-7xxx) is **fail-closed**: `ScreenUnavailable` is a
/// > transient fault the *caller* converts into a hard block for CSAM-class media
/// > — never an optimistic publish — and `AssetQuarantined` / `LegalHoldActive`
/// > (451) revoke delivery and override deletion. The `is_retryable` flags drive
/// > the `run_consumer` retry/DLQ classification on the ingestion side: a content
/// > probe failure (MED-6xxx) is terminal (→ DLQ / Failed), whereas a transient
/// > object-store or transcode-worker fault is retried then dead-lettered.
#[derive(Debug, Error)]
#[non_exhaustive]
pub enum MediaError {
    // ── Storage delegates (the byte/metadata split) ───────────────────────────
    /// Postgres — the asset metadata System of Record (`assets`, `renditions`,
    /// `upload_tickets`, compliance flags). Object storage holds the canonical
    /// *bytes*; this store holds the canonical *truth about* them.
    #[error(transparent)]
    Storage(#[from] postgres_storage::StorageError),

    /// Redis — the hot-path delivery-resolution cache + upload-ticket reservations
    /// + content-hash dedup lookups.
    #[error(transparent)]
    Cache(#[from] redis_storage::RedisStorageError),

    #[error(transparent)]
    Validation(#[from] validation::ValidationError),

    // ── Upload brokerage / ticket · Plane A (MED-1xxx) ────────────────────────
    #[error("unsupported media kind: '{kind}'")]
    InvalidMediaKind { kind: String },

    #[error("unsupported MIME type: '{mime}'")]
    UnsupportedMimeType { mime: String },

    #[error("upload size {actual} bytes exceeds the limit of {limit} bytes")]
    UploadSizeExceeded { limit: u64, actual: u64 },

    /// The pre-signed upload ticket's validity window has elapsed; the client must
    /// request a fresh ticket rather than retry against the expired capability.
    #[error("upload ticket has expired")]
    UploadTicketExpired,

    /// A commit / finalize was attempted before the bytes actually landed in the
    /// object store (no object present at the reserved key).
    #[error("upload has not been finalized: the object is not present in the store")]
    UploadNotFinalized,

    /// Private documents (#777) are not enabled on this deployment
    /// (`MEDIA_PRIVATE_DOCUMENTS_ENABLED`): their bytes would have no private
    /// home yet.
    #[error("private documents are not enabled")]
    PrivateDocumentsDisabled,

    // ── Asset metadata SoR (MED-2xxx) ─────────────────────────────────────────
    #[error("asset not found: {id}")]
    AssetNotFound { id: String },

    #[error("asset state transition from '{from}' to '{to}' is not permitted")]
    InvalidStateTransition { from: String, to: String },

    /// An optimistic-lock conflict on the asset row; another writer advanced the
    /// version concurrently. Safe to retry against the fresh row.
    #[error("concurrent modification: the asset was updated by another writer")]
    ConcurrentModification,

    #[error("asset already exists: {id}")]
    AssetAlreadyExists { id: String },

    // ── Rendition / transformation pipeline · Plane B (MED-3xxx) ──────────────
    #[error("transcode failed: {reason}")]
    TranscodeFailed { reason: String },

    #[error("unsupported codec: '{codec}'")]
    UnsupportedCodec { codec: String },

    #[error("rendition not found: '{spec}'")]
    RenditionNotFound { spec: String },

    #[error("processing pipeline failed: {reason}")]
    ProcessingFailed { reason: String },

    // ── Object-store adapter · the byte plane (MED-4xxx) ──────────────────────
    #[error("object storage is unavailable")]
    ObjectStoreUnavailable,

    #[error("object storage operation timed out")]
    ObjectStoreTimeout,

    #[error("failed to pre-sign an object-store URL: {reason}")]
    PresignFailed { reason: String },

    #[error("object not found in store: '{key}'")]
    ObjectNotFound { key: String },

    // ── CDN / delivery / signing · Plane C (MED-5xxx) ─────────────────────────
    #[error("failed to mint a signed delivery URL: {reason}")]
    DeliverySigningFailed { reason: String },

    #[error("CDN cache invalidation failed: {reason}")]
    CdnInvalidationFailed { reason: String },

    #[error("failed to build the delivery manifest: {reason}")]
    ManifestBuildFailed { reason: String },

    // ── Content validation / probe (MED-6xxx) ─────────────────────────────────
    /// The verified magic bytes do not match the client-declared MIME — the upload
    /// lied about its content type. Never trust the declared type over the bytes.
    #[error("declared content type '{declared}' does not match the actual content '{actual}'")]
    ContentTypeMismatch { declared: String, actual: String },

    #[error("media is corrupt or undecodable: {reason}")]
    CorruptMedia { reason: String },

    /// A decode-bomb guard: declared/actual dimensions exceed the safe ceiling
    /// (pixel count, frame count, or duration).
    #[error("media dimensions exceed the permitted limit")]
    DimensionLimitExceeded,

    #[error("malware detected in the uploaded object")]
    MalwareDetected,

    // ── Compliance / Screen · the fail-closed sub-plane (MED-7xxx) ────────────
    /// The asset is under a moderation takedown: delivery is revoked. 451 mirrors
    /// moderation's `ContentBlocked`.
    #[error("asset is quarantined and cannot be delivered")]
    AssetQuarantined,

    /// The pre-publish moderation Screen gate is unavailable. For CSAM-class media
    /// the *caller* converts this into a hard block (fail-closed) — the asset never
    /// goes public on uncertainty.
    #[error("the moderation screen gate is unavailable")]
    ScreenUnavailable,

    /// A legal hold (e.g. CSAM evidence preservation) is active; the asset's bytes
    /// cannot be hard-deleted, overriding an owner GDPR-erasure request.
    #[error("a legal hold is active; the asset cannot be deleted")]
    LegalHoldActive,

    // ── Inbound event decode / source mapping (MED-8xxx) ──────────────────────
    #[error("failed to decode event from topic '{topic}': {reason}")]
    EventDecodeFailed { topic: String, reason: String },

    /// An event type this consumer does not handle; usually folded into an `Ok`
    /// skip rather than dead-lettered.
    #[error("unknown event type: '{event_type}'")]
    UnknownEventType { event_type: String },

    #[error("source event could not be mapped to a media action: {reason}")]
    UnmappedSource { reason: String },

    // ── Cross-cutting (MED-9xxx) ──────────────────────────────────────────────
    #[error("domain invariant violated on '{field}': {message}")]
    DomainViolation { field: String, message: String },

    #[error("invalid identifier: '{0}'")]
    InvalidIdentifier(String),

    #[error("failed to publish event: {0}")]
    EventPublishFailed(String),

    #[error("failed to consume event: {0}")]
    EventConsumeFailed(String),
}

impl AppError for MediaError {
    fn error_code(&self) -> &'static str {
        match self {
            MediaError::Storage(e) => e.error_code(),
            MediaError::Cache(e) => e.error_code(),
            MediaError::Validation(e) => e.error_code(),

            MediaError::InvalidMediaKind { .. } => "MED-1001",
            MediaError::UnsupportedMimeType { .. } => "MED-1002",
            MediaError::UploadSizeExceeded { .. } => "MED-1003",
            MediaError::UploadTicketExpired => "MED-1004",
            MediaError::UploadNotFinalized => "MED-1005",
            MediaError::PrivateDocumentsDisabled => "MED-1006",

            MediaError::AssetNotFound { .. } => "MED-2001",
            MediaError::InvalidStateTransition { .. } => "MED-2002",
            MediaError::ConcurrentModification => "MED-2003",
            MediaError::AssetAlreadyExists { .. } => "MED-2004",

            MediaError::TranscodeFailed { .. } => "MED-3001",
            MediaError::UnsupportedCodec { .. } => "MED-3002",
            MediaError::RenditionNotFound { .. } => "MED-3003",
            MediaError::ProcessingFailed { .. } => "MED-3004",

            MediaError::ObjectStoreUnavailable => "MED-4001",
            MediaError::ObjectStoreTimeout => "MED-4002",
            MediaError::PresignFailed { .. } => "MED-4003",
            MediaError::ObjectNotFound { .. } => "MED-4004",

            MediaError::DeliverySigningFailed { .. } => "MED-5001",
            MediaError::CdnInvalidationFailed { .. } => "MED-5002",
            MediaError::ManifestBuildFailed { .. } => "MED-5003",

            MediaError::ContentTypeMismatch { .. } => "MED-6001",
            MediaError::CorruptMedia { .. } => "MED-6002",
            MediaError::DimensionLimitExceeded => "MED-6003",
            MediaError::MalwareDetected => "MED-6004",

            MediaError::AssetQuarantined => "MED-7001",
            MediaError::ScreenUnavailable => "MED-7002",
            MediaError::LegalHoldActive => "MED-7003",

            MediaError::EventDecodeFailed { .. } => "MED-8001",
            MediaError::UnknownEventType { .. } => "MED-8002",
            MediaError::UnmappedSource { .. } => "MED-8003",

            MediaError::DomainViolation { .. } => "MED-9001",
            MediaError::InvalidIdentifier(_) => "MED-9002",
            MediaError::EventPublishFailed(_) => "MED-9003",
            MediaError::EventConsumeFailed(_) => "MED-9004",
        }
    }

    fn http_status(&self) -> StatusCode {
        match self {
            MediaError::Storage(e) => e.http_status(),
            MediaError::Cache(e) => e.http_status(),
            MediaError::Validation(e) => e.http_status(),

            MediaError::AssetNotFound { .. }
            | MediaError::RenditionNotFound { .. }
            | MediaError::ObjectNotFound { .. } => StatusCode::NOT_FOUND,

            MediaError::UploadNotFinalized
            | MediaError::InvalidStateTransition { .. }
            | MediaError::ConcurrentModification
            | MediaError::AssetAlreadyExists { .. } => StatusCode::CONFLICT,

            MediaError::UploadTicketExpired => StatusCode::GONE,
            MediaError::UploadSizeExceeded { .. } => StatusCode::PAYLOAD_TOO_LARGE,
            MediaError::UnsupportedMimeType { .. } => StatusCode::UNSUPPORTED_MEDIA_TYPE,

            MediaError::ObjectStoreUnavailable
            | MediaError::ScreenUnavailable
            | MediaError::PrivateDocumentsDisabled => StatusCode::SERVICE_UNAVAILABLE,
            MediaError::ObjectStoreTimeout => StatusCode::GATEWAY_TIMEOUT,

            MediaError::AssetQuarantined | MediaError::LegalHoldActive => {
                StatusCode::UNAVAILABLE_FOR_LEGAL_REASONS
            }

            MediaError::TranscodeFailed { .. }
            | MediaError::ProcessingFailed { .. }
            | MediaError::PresignFailed { .. }
            | MediaError::DeliverySigningFailed { .. }
            | MediaError::CdnInvalidationFailed { .. }
            | MediaError::ManifestBuildFailed { .. }
            | MediaError::EventPublishFailed(_)
            | MediaError::EventConsumeFailed(_) => StatusCode::INTERNAL_SERVER_ERROR,

            _ => StatusCode::UNPROCESSABLE_ENTITY,
        }
    }

    fn severity(&self) -> Severity {
        match self {
            MediaError::Storage(e) => e.severity(),
            MediaError::Cache(e) => e.severity(),
            MediaError::Validation(e) => e.severity(),

            MediaError::ConcurrentModification
            | MediaError::ObjectStoreUnavailable
            | MediaError::ObjectStoreTimeout
            | MediaError::CdnInvalidationFailed { .. }
            | MediaError::MalwareDetected
            | MediaError::AssetQuarantined
            | MediaError::ScreenUnavailable
            | MediaError::LegalHoldActive => Severity::High,

            MediaError::InvalidStateTransition { .. }
            | MediaError::TranscodeFailed { .. }
            | MediaError::UnsupportedCodec { .. }
            | MediaError::ProcessingFailed { .. }
            | MediaError::PresignFailed { .. }
            | MediaError::DeliverySigningFailed { .. }
            | MediaError::ManifestBuildFailed { .. }
            | MediaError::ContentTypeMismatch { .. }
            | MediaError::CorruptMedia { .. }
            | MediaError::EventDecodeFailed { .. }
            | MediaError::UnmappedSource { .. }
            | MediaError::DomainViolation { .. }
            | MediaError::EventPublishFailed(_)
            | MediaError::EventConsumeFailed(_) => Severity::Medium,

            _ => Severity::Low,
        }
    }

    fn is_retryable(&self) -> bool {
        match self {
            MediaError::Storage(e) => e.is_retryable(),
            MediaError::Cache(e) => e.is_retryable(),
            MediaError::Validation(e) => e.is_retryable(),

            MediaError::ConcurrentModification
            | MediaError::TranscodeFailed { .. }
            | MediaError::ProcessingFailed { .. }
            | MediaError::ObjectStoreUnavailable
            | MediaError::ObjectStoreTimeout
            | MediaError::PresignFailed { .. }
            | MediaError::DeliverySigningFailed { .. }
            | MediaError::CdnInvalidationFailed { .. }
            | MediaError::ScreenUnavailable => true,

            _ => false,
        }
    }

    fn category(&self) -> &'static str {
        match self {
            MediaError::Storage(e) => e.category(),
            MediaError::Cache(e) => e.category(),
            MediaError::Validation(e) => e.category(),
            _ => "MED",
        }
    }

    fn user_facing_message(&self) -> &'static str {
        match self {
            MediaError::Storage(e) => e.user_facing_message(),
            MediaError::Cache(e) => e.user_facing_message(),
            MediaError::Validation(e) => e.user_facing_message(),

            MediaError::InvalidMediaKind { .. }
            | MediaError::UnsupportedMimeType { .. }
            | MediaError::UploadSizeExceeded { .. }
            | MediaError::ContentTypeMismatch { .. }
            | MediaError::CorruptMedia { .. }
            | MediaError::DimensionLimitExceeded
            | MediaError::UnsupportedCodec { .. } => "This file could not be accepted.",

            MediaError::UploadTicketExpired | MediaError::UploadNotFinalized => {
                "Your upload could not be completed. Please try again."
            }

            MediaError::MalwareDetected
            | MediaError::AssetQuarantined
            | MediaError::LegalHoldActive => "This content is not available.",

            MediaError::AssetNotFound { .. }
            | MediaError::RenditionNotFound { .. }
            | MediaError::ObjectNotFound { .. } => "The requested media does not exist.",

            MediaError::ObjectStoreUnavailable
            | MediaError::ObjectStoreTimeout
            | MediaError::ScreenUnavailable => {
                "Media is temporarily unavailable. Please try again."
            }

            _ => "An internal media error occurred.",
        }
    }
}
