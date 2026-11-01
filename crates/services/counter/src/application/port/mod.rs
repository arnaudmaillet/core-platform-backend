//! Outbound ports — the only contracts the application layer holds against the
//! outside world. Concrete adapters (Redis hot store, Postgres ledger, Scylla
//! time-series, Kafka signal publisher, plus the inbound event-decode layer) live
//! in `infrastructure` (Phase 4) and are injected at the composition root. Each is
//! an `async_trait` so it can be held as `Arc<dyn …>`; in-memory fakes back the
//! unit tests.
//!
//! The three storage ports mirror the three tiers: [`CounterStore`] (hot Redis,
//! the only one on the sub-ms read path), [`CounterLedger`] (warm Postgres, the
//! auditable totals + idempotency), [`TimeSeriesStore`] (cold Scylla, history).
//! [`SignalPublisher`] is the single outbound stream.

pub mod counter_ledger;
pub mod counter_store;
pub mod reconciliation_source;
pub mod signal_publisher;
pub mod time_series;

pub use counter_ledger::{CounterLedger, FlushOutcome, reconcile_cursor};
pub use counter_store::CounterStore;
pub use reconciliation_source::ReconciliationSource;
pub use signal_publisher::SignalPublisher;
pub use time_series::TimeSeriesStore;
