//! Infrastructure adapters — the concrete implementations of the application
//! ports, plus the inbound event-decode layer.
//!
//! * [`redis_counter_store`] — hot tier (fred, Lua-issued HLL/sorted-set ops)
//! * [`pg_counter_ledger`] — warm tier (sqlx, idempotent window-keyed UPSERT)
//! * [`scylla_time_series`] — cold tier (Scylla TWCS counter rollups)
//! * [`kafka_signal_publisher`] — the `counter.v1.popularity` producer
//! * [`decode`] — counter-owned wire DTOs + the pure wire→`Observation` mappers
//!
//! Per the integration-test standard, the storage/transport adapters are
//! compile-checked here; their live behaviour is exercised by the gated suite in
//! Phase 6. The decode layer is pure and unit-tested in place.

pub mod consumer;
pub mod decode;
pub mod grpc;
pub mod kafka_signal_publisher;
pub mod pg_counter_ledger;
pub mod reconcile;
pub mod redis_counter_store;
pub mod scylla_time_series;

pub use kafka_signal_publisher::{KafkaSignalPublisher, PopularityEvent};
pub use pg_counter_ledger::PgCounterLedger;
pub use redis_counter_store::RedisCounterStore;
pub use scylla_time_series::ScyllaTimeSeriesStore;
