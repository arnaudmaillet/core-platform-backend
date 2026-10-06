//! Scenario groups for the timeline live suite. Each maps to one of the axes the
//! testing standard targets: concurrency, temporal partitioning, cache
//! invalidation, and stream/async-task lifetimes.

mod fanout_ordering;
mod discovery;
mod following_cache;
mod interests;
mod mutes;
mod vip_routing;
mod warmup_lifecycle;
