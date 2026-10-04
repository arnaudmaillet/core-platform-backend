//! Account-directory adapter — the small synchronous slice moderation needs from
//! `account` (confirming an actor exists before an actor-level enforcement), and
//! the post / comment / profile lookups that resolve a client report's subject.

pub mod grpc_account_directory;
pub mod grpc_subject_resolver;

pub use grpc_account_directory::GrpcAccountDirectory;
pub use grpc_subject_resolver::GrpcSubjectResolver;
