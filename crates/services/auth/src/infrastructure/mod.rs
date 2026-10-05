//! Infrastructure adapters — the concrete implementations of the application
//! ports. This is the only layer that names a real backend (Keycloak, Postgres,
//! Redis Cluster, Kafka, the `account` gRPC service) or a token format (ES256).
//! The composition root (Phase 5) selects and injects them.

pub mod attest;
pub mod cache;
pub mod consumer;
pub mod directory;
pub mod event;
pub mod grpc;
pub mod http;
pub mod idp;
pub mod mfa;
pub mod notify;
pub mod persistence;
pub mod token;
