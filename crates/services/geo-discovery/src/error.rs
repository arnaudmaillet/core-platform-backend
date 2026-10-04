use error::{AppError, Severity};
use http::StatusCode;
use thiserror::Error;

#[derive(Debug, Error)]
#[non_exhaustive]
pub enum GeoDiscoveryError {
    #[error(transparent)]
    Scylla(#[from] scylla_storage::ScyllaStorageError),

    #[error(transparent)]
    Redis(#[from] redis_storage::RedisStorageError),

    #[error(transparent)]
    Validation(#[from] validation::ValidationError),

    // ── GEO-1xxx: Coordinate / H3 encoding errors ─────────────────────────────
    #[error("invalid coordinate: lat={lat}, lng={lng} — must be within WGS-84 bounds")]
    InvalidCoordinate { lat: f64, lng: f64 },

    #[error("invalid H3 cell index: {0}")]
    InvalidH3Index(i64),

    // ── GEO-2xxx: Viewport / zoom validation ──────────────────────────────────
    #[error("invalid viewport: sw ({sw_lat},{sw_lng}) must be strictly below ne ({ne_lat},{ne_lng})")]
    InvalidViewport { sw_lat: f64, sw_lng: f64, ne_lat: f64, ne_lng: f64 },

    #[error("invalid zoom level {0}: must be in [0, 15]")]
    InvalidZoomLevel(i32),

    // ── GEO-4xxx: Redis spatial index errors ──────────────────────────────────
    #[error("Lua script returned an unexpected value from the spatial index")]
    SpatialLuaReturnInvalid,

    // ── GEO-5xxx: Redis card store errors ─────────────────────────────────────
    #[error("failed to serialize map card for post {post_id}: {message}")]
    CardSerializationFailed { post_id: String, message: String },

    #[error("failed to deserialize map card for post {post_id}: {message}")]
    CardDeserializationFailed { post_id: String, message: String },

    // ── GEO-9xxx: ID parsing / domain violations ──────────────────────────────
    #[error("invalid post ID: '{0}'")]
    InvalidPostId(String),

    #[error("invalid author ID: '{0}'")]
    InvalidAuthorId(String),

    #[error("domain violation on field '{field}': {message}")]
    DomainViolation { field: String, message: String },

    // ── GEO-6xxx: dependencies ────────────────────────────────────────────────
    /// social-graph's `CheckAccess` could not answer. Client reads fail closed
    /// rather than risk showing a private, blocked or hidden author's post.
    #[error("audience check unavailable: {reason}")]
    AccessCheckUnavailable { reason: String },
}

impl AppError for GeoDiscoveryError {
    fn error_code(&self) -> &'static str {
        match self {
            Self::Scylla(e)     => e.error_code(),
            Self::Redis(e)      => e.error_code(),
            Self::Validation(e) => e.error_code(),

            Self::InvalidCoordinate { .. } => "GEO-1001",
            Self::InvalidH3Index(_)        => "GEO-1002",

            Self::InvalidViewport { .. }  => "GEO-2001",
            Self::InvalidZoomLevel(_)     => "GEO-2002",

            Self::SpatialLuaReturnInvalid          => "GEO-4001",

            Self::CardSerializationFailed { .. }   => "GEO-5001",
            Self::CardDeserializationFailed { .. } => "GEO-5002",

            Self::InvalidPostId(_)    => "GEO-9001",
            Self::InvalidAuthorId(_)  => "GEO-9002",
            Self::DomainViolation { .. } => "GEO-9003",
            Self::AccessCheckUnavailable { .. } => "GEO-6001",
        }
    }

    fn http_status(&self) -> StatusCode {
        match self {
            Self::Scylla(e)     => e.http_status(),
            Self::Redis(e)      => e.http_status(),
            Self::Validation(e) => e.http_status(),

            Self::InvalidCoordinate { .. }
            | Self::InvalidViewport { .. }
            | Self::InvalidZoomLevel(_)
            | Self::InvalidPostId(_)
            | Self::InvalidAuthorId(_)
            | Self::DomainViolation { .. } => StatusCode::UNPROCESSABLE_ENTITY,

            Self::InvalidH3Index(_) => StatusCode::UNPROCESSABLE_ENTITY,

            Self::SpatialLuaReturnInvalid
            | Self::CardSerializationFailed { .. }
            | Self::CardDeserializationFailed { .. } => StatusCode::INTERNAL_SERVER_ERROR,

            Self::AccessCheckUnavailable { .. } => StatusCode::SERVICE_UNAVAILABLE,
        }
    }

    fn severity(&self) -> Severity {
        match self {
            Self::Scylla(e) => e.severity(),
            Self::Redis(e)  => e.severity(),

            Self::SpatialLuaReturnInvalid
            | Self::CardSerializationFailed { .. }
            | Self::CardDeserializationFailed { .. } => Severity::High,

            Self::Validation(e)          => e.severity(),
            Self::InvalidCoordinate { .. }
            | Self::InvalidViewport { .. }
            | Self::DomainViolation { .. } => Severity::Medium,

            Self::InvalidH3Index(_)
            | Self::InvalidZoomLevel(_)
            | Self::InvalidPostId(_)
            | Self::InvalidAuthorId(_) => Severity::Low,

            Self::AccessCheckUnavailable { .. } => Severity::Medium,
        }
    }

    fn is_retryable(&self) -> bool {
        match self {
            Self::Scylla(e) => e.is_retryable(),
            Self::Redis(e)  => e.is_retryable(),
            Self::AccessCheckUnavailable { .. } => true,
            _               => false,
        }
    }

    fn category(&self) -> &'static str {
        match self {
            Self::Scylla(e)     => e.category(),
            Self::Redis(e)      => e.category(),
            Self::Validation(e) => e.category(),
            _                   => "GEO",
        }
    }

    fn user_facing_message(&self) -> &'static str {
        match self {
            Self::Scylla(_)
            | Self::Redis(_)
            | Self::SpatialLuaReturnInvalid
            | Self::CardSerializationFailed { .. }
            | Self::CardDeserializationFailed { .. } =>
                "An internal error occurred. Please try again later.",

            Self::InvalidCoordinate { .. } =>
                "The provided coordinates are outside valid WGS-84 bounds.",

            Self::InvalidH3Index(_) => "The provided spatial index is not valid.",

            Self::InvalidViewport { .. } =>
                "The provided map viewport is not valid.",

            Self::InvalidZoomLevel(_) => "The provided zoom level is not valid.",

            Self::InvalidPostId(_)    => "The provided post ID is not valid.",
            Self::InvalidAuthorId(_)  => "The provided author ID is not valid.",
            Self::DomainViolation { .. } =>
                "The request contains an invalid domain value.",

            Self::Validation(e) => e.user_facing_message(),

            Self::AccessCheckUnavailable { .. } =>
                "The map is temporarily unavailable. Please try again later.",
        }
    }
}

/// Classifies failures for the Kafka consumer runner: transient storage/cache
/// faults are retried with backoff, data/invariant errors are dead-lettered.
impl transport::kafka::consumer::ClassifyError for GeoDiscoveryError {
    fn is_retryable(&self) -> bool {
        <Self as AppError>::is_retryable(self)
    }
}
