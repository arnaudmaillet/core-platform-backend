//! Durable persistence ports for the moderation aggregates. Concrete adapters
//! (Postgres for the decision/case SoR, Scylla for history) live in
//! `infrastructure` (Phase 4) and are injected as `Arc<dyn …>` at the composition
//! root.

use async_trait::async_trait;

use chrono::{DateTime, Utc};

use crate::domain::aggregate::{Appeal, Case, Decision, EnforcementAction, PenaltyLedger, Report};
use crate::domain::value_object::{
    ActorId, AppealId, CaseId, CaseStatus, DecisionId, EnforcementId, EnforcementVersion, ReportId,
    ReporterKind, SubjectRef,
};
use crate::error::ModerationError;

/// Persistence for the [`Case`] aggregate. `save` upserts with optimistic-lock
/// semantics on the aggregate `version`; the deterministic [`CaseId`] makes a
/// redelivered open idempotent.
#[async_trait]
pub trait CaseRepository: Send + Sync + 'static {
    async fn save(&self, case: &Case) -> Result<(), ModerationError>;

    async fn find_by_id(&self, id: &CaseId) -> Result<Option<Case>, ModerationError>;

    /// The review queue (paged), optionally filtered by status. `None` status ⇒
    /// all open work.
    async fn list_queue(
        &self,
        queue: &str,
        status: Option<CaseStatus>,
        limit: usize,
    ) -> Result<Vec<Case>, ModerationError>;
}

/// Persistence for the **append-only** decision ledger. There is deliberately no
/// `update`: a decision is never mutated, only superseded by a new (reversal) one.
#[async_trait]
pub trait DecisionRepository: Send + Sync + 'static {
    async fn append(&self, decision: &Decision) -> Result<(), ModerationError>;

    async fn find_by_id(&self, id: &DecisionId) -> Result<Option<Decision>, ModerationError>;
}

/// Persistence for the [`EnforcementAction`] aggregate.
#[async_trait]
pub trait EnforcementRepository: Send + Sync + 'static {
    async fn save(&self, enforcement: &EnforcementAction) -> Result<(), ModerationError>;

    async fn find_by_id(
        &self,
        id: &EnforcementId,
    ) -> Result<Option<EnforcementAction>, ModerationError>;

    /// The next monotonic enforcement version for a subject (max existing + 1, or
    /// [`EnforcementVersion::INITIAL`] when none exists). The monotonicity is what
    /// keeps a stale reversal from racing ahead of a newer re-application.
    async fn next_version(
        &self,
        subject: &SubjectRef,
    ) -> Result<EnforcementVersion, ModerationError>;

    /// Currently-active enforcements for an actor (for `GetEnforcementState`).
    async fn list_active_for_actor(
        &self,
        actor_id: &ActorId,
    ) -> Result<Vec<EnforcementAction>, ModerationError>;
}

/// Persistence for the [`PenaltyLedger`] aggregate. `load` returns an empty ledger
/// for an actor with no history.
#[async_trait]
pub trait PenaltyRepository: Send + Sync + 'static {
    async fn load(&self, actor_id: &ActorId) -> Result<PenaltyLedger, ModerationError>;

    async fn save(&self, ledger: &PenaltyLedger) -> Result<(), ModerationError>;
}

/// Persistence for the [`Appeal`] aggregate.
#[async_trait]
pub trait AppealRepository: Send + Sync + 'static {
    async fn save(&self, appeal: &Appeal) -> Result<(), ModerationError>;

    async fn find_by_id(&self, id: &AppealId) -> Result<Option<Appeal>, ModerationError>;
}

/// A position in a reporter's newest-first report list: the last report of the
/// previous page.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ReportCursor {
    pub reported_at: DateTime<Utc>,
    pub id: ReportId,
}

/// A recorded report with the current status of the review case it fed
/// (`None` while that case is not persisted).
#[derive(Debug, Clone)]
pub struct FiledReport {
    pub report: Report,
    pub case_status: Option<CaseStatus>,
}

/// Persistence for the [`Report`] aggregate — the reporter's own record of what
/// they reported (DSA Art. 16(5)), separate from the evidence the case keeps.
#[async_trait]
pub trait ReportRepository: Send + Sync + 'static {
    /// Records a report. Idempotent on its deterministic id: the same reporter
    /// reporting the same subject again keeps the first record.
    async fn record(&self, report: &Report) -> Result<(), ModerationError>;

    /// A reporter's reports, newest first, strictly after `after`, joined with
    /// the status of each report's case.
    async fn list_for_reporter(
        &self,
        kind: ReporterKind,
        reporter_id: &ActorId,
        after: Option<ReportCursor>,
        limit: usize,
    ) -> Result<Vec<FiledReport>, ModerationError>;
}
