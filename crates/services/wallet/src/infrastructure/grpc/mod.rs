//! The `wallet.v1` gRPC surface.

pub mod handler;
pub mod server;

pub use handler::{proto, WalletServiceHandler};
pub use proto::wallet_service_server::WalletServiceServer;
pub use server::FILE_DESCRIPTOR_SET;
