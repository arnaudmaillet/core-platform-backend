//! Outbound gRPC clients to peer services.

pub mod grpc_interaction_gate;
pub mod grpc_media;

pub use grpc_interaction_gate::GrpcInteractionGate;
pub use grpc_media::{GrpcMedia, MediaDelivery};
