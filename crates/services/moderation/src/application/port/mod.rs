//! Outbound ports — the only contracts the application layer holds against the
//! outside world. Concrete adapters (Postgres, Scylla, Redis, the `account` gRPC
//! client, the classifier gateway, the Kafka publisher) live in `infrastructure`
//! (Phase 4) and are injected at the composition root. Each is an `async_trait` so
//! it can be held as `Arc<dyn …>`; in-memory fakes back the unit tests.

pub mod account_directory;
pub mod classifier_gateway;
pub mod enforcement_projection;
pub mod event_publisher;
pub mod report_rate_limiter;
pub mod repositories;
pub mod screen_corpus;
pub mod subject_resolver;

pub use account_directory::AccountDirectory;
pub use classifier_gateway::ClassifierGateway;
pub use enforcement_projection::EnforcementProjection;
pub use event_publisher::EventPublisher;
pub use repositories::{
    AppealCursor, AppealRepository, CaseRepository, DecisionRepository, EnforcementRepository, FiledReport,
    PenaltyRepository, ReportCursor, ReportRepository,
};
pub use report_rate_limiter::ReportRateLimiter;
pub use screen_corpus::{ContentHash, CorpusMatch, ScreenCorpus};
pub use subject_resolver::SubjectResolver;
