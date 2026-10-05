//! Infrastructure layer — outbound adapters implementing the application ports.
//!
//! [`persistence`] holds the ScyllaDB adapters (time-bucketed message log,
//! member roster, audience subscription tables) and [`cache`] holds the Redis
//! Cluster adapters (hot-tail cache, presence/typing, read-receipts, and the
//! audience-shard routing registry), all implementing the application ports. The
//! [`routing`] is the sharded Pub/Sub backbone (`SPUBLISH`/`SSUBSCRIBE` channel
//! scheme, the per-pod subscription manager + reaper, and the message-fork
//! orchestrator that drives both planes). The dual gRPC server-streaming
//! registries (Member vs Audience planes) and the Kafka workers arrive in later
//! phases.

pub mod cache;
pub mod client;
pub mod event;
pub mod grpc;
pub mod persistence;
pub mod routing;
pub mod streaming;
pub mod worker;
