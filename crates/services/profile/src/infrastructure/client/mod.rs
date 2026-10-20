//! Outbound mesh clients.

pub mod grpc_private_documents;

pub use grpc_private_documents::{GrpcPrivateDocuments, MediaDelivery};
pub mod grpc_gem_wallet;

pub use grpc_gem_wallet::GrpcGemWallet;
