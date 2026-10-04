//! PostgreSQL adapters — the decision/case system of record.
//!
//! Like `account`/`auth`, writes use a single pool via [`TransactionManager`];
//! true `account_id` sharding is deferred (every table carries `actor_id` so it
//! can be added without a schema change). Upserts are last-write-wins on the
//! aggregate `version`, which is retained for a future optimistic-lock tightening.

pub mod model;
pub mod pg_appeal_repository;
pub mod pg_case_repository;
pub mod pg_decision_repository;
pub mod pg_enforcement_repository;
pub mod pg_penalty_repository;
pub mod pg_report_repository;

pub use pg_appeal_repository::PgAppealRepository;
pub use pg_case_repository::PgCaseRepository;
pub use pg_decision_repository::PgDecisionRepository;
pub use pg_enforcement_repository::PgEnforcementRepository;
pub use pg_penalty_repository::PgPenaltyRepository;
pub use pg_report_repository::PgReportRepository;

use postgres_storage::StorageError;

use crate::error::ModerationError;

/// Maps a raw sqlx error into the moderation error namespace (delegating the code
/// to the shared `DB-*` storage taxonomy).
pub(crate) fn storage_err(e: sqlx::Error) -> ModerationError {
    ModerationError::Records(StorageError::from(e))
}
