//! Redis adapters — the hot-path enforcement projection (Plane B) and the
//! known-bad screen corpus (Plane C), and the client report quotas.

pub mod keys;
pub mod redis_classification_debounce;
pub mod redis_enforcement_projection;
pub mod redis_report_rate_limiter;
pub mod redis_screen_corpus;

pub use redis_classification_debounce::DebouncedClassifierGateway;
pub use redis_enforcement_projection::RedisEnforcementProjection;
pub use redis_report_rate_limiter::RedisReportRateLimiter;
pub use redis_screen_corpus::RedisScreenCorpus;
