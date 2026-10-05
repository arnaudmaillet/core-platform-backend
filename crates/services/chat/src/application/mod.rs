//! Application layer — CQRS command/query handlers and the ports (traits) they
//! depend on.
//!
//! [`port`] defines the hexagonal boundary the infrastructure adapters
//! implement (repositories now; cache/streaming/routing in later phases). The
//! command/query handlers (`SendMessage`, `CreateConversation`,
//! `ToggleVisibility`, `JoinAsMember`, `Subscribe`, `GetHistory`, …) live in
//! [`command`] and [`query`]; [`access`] holds the existence-concealing
//! answer the member-only paths give a non-member.

pub mod access;
pub mod command;
pub mod port;
pub mod query;
