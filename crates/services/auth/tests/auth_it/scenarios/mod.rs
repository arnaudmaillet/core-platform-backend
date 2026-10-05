//! Live scenarios driving the real Postgres + Redis graph through the gRPC handler.

pub mod credentials;
pub mod federated_nonce;
pub mod global_logout;
pub mod guest_session;
pub mod lifecycle;
pub mod persistence_roundtrip;
pub mod refresh_reuse;
pub mod outbox_relay;
pub mod verification_codes;
