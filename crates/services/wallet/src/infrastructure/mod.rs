//! Adapters: Postgres (the ledger), gRPC (the edge and the mesh), Kafka (the
//! account-erasure consumer).

pub mod consumer;
pub mod grpc;
pub mod persistence;
