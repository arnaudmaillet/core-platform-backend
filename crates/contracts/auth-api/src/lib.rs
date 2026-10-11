//! Generated gRPC contract for `auth.v1` (server + client stubs + descriptor),
//! compiled from the shared contracts/proto IDL. Consumers depend on this crate
//! instead of recompiling the .proto files.
//!
//! Contract rule: `auth.v1` exposes only normalized identity types
//! (`account_id`, `session_id`, `generation`, `permissions`) — never a
//! provider-specific field.
tonic::include_proto!("auth.v1");

/// Encoded protobuf descriptor set for gRPC server reflection.
pub const FILE_DESCRIPTOR_SET: &[u8] = tonic::include_file_descriptor_set!("auth_descriptor");
