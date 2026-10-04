//! PostgreSQL/CockroachDB adapters for the durable auth ports.
//!
//! Writes that carry an `account_id` route through
//! [`TransactionManager::run_on_shard`] keyed on it, so a person's auth state
//! co-locates on one shard. Lookups by `session_id` / `token_hash` / subject use
//! the single pool — correct for SingleNode and CockroachDB; true app-sharding of
//! those would need a secondary index (deferred, as in `account`).
//!
//! ## Optimistic locking
//! The in-memory aggregate `version` mirrors the persisted value. A new aggregate
//! (`version == 0`) is `INSERT`ed at version 0; a mutation bumps the in-memory
//! version, and the adapter writes `SET version = $new WHERE version = $new - 1`,
//! mapping a zero-row update to [`AuthError::ConcurrentModification`].

pub mod model;
pub mod pg_guest_registry;
pub mod pg_refresh_token_repository;
pub mod pg_session_repository;
pub mod pg_subject_link_repository;

pub use pg_guest_registry::PgGuestRegistry;
pub use pg_refresh_token_repository::PgRefreshTokenRepository;
pub use pg_session_repository::PgSessionRepository;
pub use pg_subject_link_repository::PgSubjectLinkRepository;
