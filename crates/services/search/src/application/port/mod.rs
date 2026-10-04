//! Outbound ports — the only contracts the application layer holds against the
//! outside world. Concrete adapters (the OpenSearch index, the inbound event-decode
//! layer) live in `infrastructure` (Phase 4) and are injected at the composition
//! root. Each is an `async_trait` so it can be held as `Arc<dyn …>`; in-memory fakes
//! back the unit tests.

pub mod audience_gate;
pub mod backfill;
pub mod index_admin;
pub mod search_index;

pub use audience_gate::AudienceGate;
pub use backfill::BackfillSource;
pub use index_admin::IndexAdmin;
pub use search_index::{SearchIndex, WriteOutcome};
