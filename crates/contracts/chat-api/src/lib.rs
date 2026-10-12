//! Generated gRPC contract for `chat.v1` (server + client stubs + descriptor),
//! compiled from the shared contracts/proto IDL. Consumers depend on this crate
//! instead of recompiling the .proto files.

// Generated: `ChatEvent.event` carries a `MessageView`, which grew with its
// media delivery (#919, #921); boxing it would change the generated API.
#![allow(clippy::large_enum_variant)]

tonic::include_proto!("chat.v1");

/// Encoded protobuf descriptor set for gRPC server reflection.
pub const FILE_DESCRIPTOR_SET: &[u8] =
    tonic::include_file_descriptor_set!("chat_descriptor");
