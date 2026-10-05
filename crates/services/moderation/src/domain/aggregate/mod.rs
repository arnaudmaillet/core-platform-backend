//! Aggregates for the moderation domain — the consistency boundaries that own
//! their invariants and emit domain events.
//!
//! * [`Case`] — the review-work root (deterministic id, signal accrual, lifecycle).
//! * [`Decision`] — the append-only ledger record (immutable; the legal evidence).
//! * [`EnforcementAction`] — the executable consequence (lifecycle + per-subject
//!   version).
//! * [`PenaltyLedger`] — the graduated-enforcement engine (strike decay → action).
//! * [`Appeal`] — the challenge lifecycle.
//! * [`Report`] — the user-report intake record (deterministic id, dedup).

pub mod appeal;
pub mod case;
pub mod decision;
pub mod enforcement;
pub mod penalty_ledger;
pub mod report;

pub use appeal::{Appeal, Appellant};
pub use case::{Case, CaseOpenParams};
pub use decision::{Decision, DecisionAuthor, DecisionParams};
pub use enforcement::{EnforcementAction, EnforcementParams};
pub use penalty_ledger::PenaltyLedger;
pub use report::Report;
