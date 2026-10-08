//! Live, container-backed integration suite for the engagement microservice.
//!
//! The whole binary is gated behind the `integration-engagement` feature so the
//! default `cargo test -p engagement` stays hermetic and Docker-free. Run the
//! live suite explicitly:
//!
//! ```text
//! cargo test -p engagement --features integration-engagement -- --nocapture
//! ```
//!
//! Engagement is Redis-primary: the likes (#665) and the view/share/comment
//! counters are single atomic Redis round-trips, with ScyllaDB durability
//! handled by background workers. The suite therefore boots only an ephemeral
//! **Redis** container (no ScyllaDB, no Kafka) and drives the hot path through
//! the production composition root ([`engagement::app::App`]):
//!
//! - **likes** — a stake's total is applied once, monotonically.
//! - **concurrent view counter** — concurrent view records sum exactly, proving
//!   the atomic Redis increment.
//!
//! All cross-component synchronisation polls observable state with a deadline
//! (`await_until`); there are no fixed sleeps.
#![cfg(feature = "integration-engagement")]

mod engagement_it;
