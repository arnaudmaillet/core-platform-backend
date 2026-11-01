//! Read-side use cases. Each implements [`cqrs::QueryHandler`] and rides the query
//! bus. All three are fail-open: a transient store outage degrades the response
//! (ledger fallback / empty results), it never errors the caller's page.

pub mod get_counters;
pub mod get_time_series;
pub mod get_trending;

pub use get_counters::{BatchGetHandler, RunBatchGet};
pub use get_time_series::{RunTimeSeries, TimeSeriesHandler};
pub use get_trending::{RunTrending, TrendingHandler};
