use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::domain::event::{CaseOpened, CaseResolved, DomainEvent};
use crate::domain::value_object::{
    ActionType, CaseId, CaseStatus, DecisionId, PolicyCategory, Signal, SubjectRef,
};
use crate::error::ModerationError;

/// Most signals a case keeps from one source (`report`, `guest_report`, a
/// classifier…).
pub const MAX_SIGNALS_PER_SOURCE: usize = 25;

/// Parameters to open a case.
#[derive(Debug, Clone)]
pub struct CaseOpenParams {
    pub subject: SubjectRef,
    pub category: PolicyCategory,
    pub queue: String,
    pub priority: String,
    pub opened_at: DateTime<Utc>,
    pub correlation_id: Uuid,
}

/// The **Case** aggregate root — a unit of review work about a subject.
///
/// Its id is **deterministic** ([`CaseId::for_subject`]), so opening a case for the
/// same subject twice (e.g. a redelivered content event) yields the same case and
/// upserts rather than duplicating — the idempotency the Consumer Runtime Standard
/// requires. A case accrues [`Signal`]s, gets triaged/assigned, and is finally
/// resolved (actioned or dismissed) — which records a [`Decision`] elsewhere and
/// emits [`CaseResolved`]. An appeal can reopen an actioned case.
///
/// [`Decision`]: crate::domain::aggregate::Decision
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Case {
    id: CaseId,
    subject: SubjectRef,
    status: CaseStatus,
    category: PolicyCategory,
    queue: String,
    priority: String,
    assignee: Option<String>,
    signals: Vec<Signal>,
    opened_at: DateTime<Utc>,
    version: i64,

    #[serde(skip)]
    pending_events: Vec<DomainEvent>,
}

impl Case {
    /// Opens a new `Open` case, emitting [`CaseOpened`].
    pub fn open(params: CaseOpenParams) -> Self {
        let id = CaseId::for_subject(&params.subject);
        let event = DomainEvent::CaseOpened(CaseOpened {
            case_id: id,
            subject: params.subject.clone(),
            actor_id: params.subject.actor_id(),
            category: params.category,
            occurred_at: params.opened_at,
            correlation_id: params.correlation_id,
        });
        Self {
            id,
            subject: params.subject,
            status: CaseStatus::Open,
            category: params.category,
            queue: params.queue,
            priority: params.priority,
            assignee: None,
            signals: Vec::new(),
            opened_at: params.opened_at,
            version: 0,
            pending_events: vec![event],
        }
    }

    /// Reconstructs from storage (no events emitted).
    #[allow(clippy::too_many_arguments)]
    pub fn reconstitute(
        id: CaseId,
        subject: SubjectRef,
        status: CaseStatus,
        category: PolicyCategory,
        queue: String,
        priority: String,
        assignee: Option<String>,
        signals: Vec<Signal>,
        opened_at: DateTime<Utc>,
        version: i64,
    ) -> Self {
        Self {
            id,
            subject,
            status,
            category,
            queue,
            priority,
            assignee,
            signals,
            opened_at,
            version,
            pending_events: Vec::new(),
        }
    }

    // ─── Queries ─────────────────────────────────────────────────────────────

    pub fn id(&self) -> CaseId {
        self.id
    }

    pub fn subject(&self) -> &SubjectRef {
        &self.subject
    }

    pub fn status(&self) -> CaseStatus {
        self.status
    }

    pub fn category(&self) -> PolicyCategory {
        self.category
    }

    pub fn queue(&self) -> &str {
        &self.queue
    }

    pub fn priority(&self) -> &str {
        &self.priority
    }

    pub fn assignee(&self) -> Option<&str> {
        self.assignee.as_deref()
    }

    pub fn signals(&self) -> &[Signal] {
        &self.signals
    }

    pub fn opened_at(&self) -> DateTime<Utc> {
        self.opened_at
    }

    pub fn version(&self) -> i64 {
        self.version
    }

    // ─── Commands ────────────────────────────────────────────────────────────

    /// Appends an evidence signal; `Ok(false)` when the case already holds
    /// [`MAX_SIGNALS_PER_SOURCE`] signals from that source (more of the same adds no
    /// evidence, and a report flood must not grow the case without bound — the
    /// reports themselves are still recorded). Rejected once the case is resolved.
    pub fn add_signal(&mut self, signal: Signal) -> Result<bool, ModerationError> {
        self.ensure_not_resolved()?;
        let same_source = self.signals.iter().filter(|s| s.source() == signal.source()).count();
        if same_source >= MAX_SIGNALS_PER_SOURCE {
            return Ok(false);
        }
        self.signals.push(signal);
        self.touch();
        Ok(true)
    }

    /// Assigns a reviewer, moving an `Open` case to `Triaged`.
    pub fn assign(&mut self, reviewer_id: impl Into<String>) -> Result<(), ModerationError> {
        self.ensure_not_resolved()?;
        self.assignee = Some(reviewer_id.into());
        if self.status == CaseStatus::Open {
            self.transition_to(CaseStatus::Triaged)?;
        }
        self.touch();
        Ok(())
    }

    /// Resolves the case under `decision_id`: `Actioned` when the action enforces,
    /// `Dismissed` when it is `NoAction`. Emits [`CaseResolved`].
    pub fn resolve(
        &mut self,
        decision_id: DecisionId,
        action: ActionType,
        now: DateTime<Utc>,
        correlation_id: Uuid,
    ) -> Result<(), ModerationError> {
        self.ensure_not_resolved()?;
        let target = if action.is_enforced() {
            CaseStatus::Actioned
        } else {
            CaseStatus::Dismissed
        };
        self.transition_to(target)?;
        self.touch();
        self.pending_events.push(DomainEvent::CaseResolved(CaseResolved {
            case_id: self.id,
            decision_id,
            actor_id: self.subject.actor_id(),
            action,
            category: self.category,
            occurred_at: now,
            correlation_id,
        }));
        Ok(())
    }

    /// Marks an actioned case as under appeal.
    pub fn mark_appealed(&mut self) -> Result<(), ModerationError> {
        self.transition_to(CaseStatus::Appealed)?;
        self.touch();
        Ok(())
    }

    /// Closes an appeal: `Dismissed` if overturned, back to `Actioned` if upheld.
    pub fn close_appeal(&mut self, overturned: bool) -> Result<(), ModerationError> {
        let target = if overturned {
            CaseStatus::Dismissed
        } else {
            CaseStatus::Actioned
        };
        self.transition_to(target)?;
        self.touch();
        Ok(())
    }

    /// Drains accumulated events for the unit-of-work to publish.
    pub fn drain_events(&mut self) -> Vec<DomainEvent> {
        std::mem::take(&mut self.pending_events)
    }

    // ─── Internals ───────────────────────────────────────────────────────────

    fn ensure_not_resolved(&self) -> Result<(), ModerationError> {
        if self.status.is_resolved() {
            return Err(ModerationError::CaseAlreadyResolved);
        }
        Ok(())
    }

    fn transition_to(&mut self, next: CaseStatus) -> Result<(), ModerationError> {
        if !self.status.can_transition_to(next) {
            return Err(ModerationError::InvalidCaseTransition {
                from: self.status.to_string(),
                to: next.to_string(),
            });
        }
        self.status = next;
        Ok(())
    }

    fn touch(&mut self) {
        self.version += 1;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::domain::value_object::{ActorId, Confidence, EntityType};

    fn t0() -> DateTime<Utc> {
        DateTime::parse_from_rfc3339("2026-06-25T12:00:00Z").unwrap().with_timezone(&Utc)
    }

    fn subject() -> SubjectRef {
        SubjectRef::new(EntityType::Post, "p1", ActorId::from_uuid(Uuid::from_u128(1)), "feed").unwrap()
    }

    fn params() -> CaseOpenParams {
        CaseOpenParams {
            subject: subject(),
            category: PolicyCategory::Harassment,
            queue: "default".into(),
            priority: "normal".into(),
            opened_at: t0(),
            correlation_id: Uuid::now_v7(),
        }
    }

    fn sig() -> Signal {
        Signal::new("report", PolicyCategory::Harassment, Confidence::new(0.7).unwrap(), t0()).unwrap()
    }

    #[test]
    fn open_is_deterministic_and_emits_event() {
        let mut c = Case::open(params());
        assert_eq!(c.id(), CaseId::for_subject(&subject()));
        assert_eq!(c.status(), CaseStatus::Open);
        let events = c.drain_events();
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].event_type(), "moderation.case_opened");
    }

    #[test]
    fn assign_triages_an_open_case() {
        let mut c = Case::open(params());
        c.assign("rev-1").unwrap();
        assert_eq!(c.status(), CaseStatus::Triaged);
        assert_eq!(c.assignee(), Some("rev-1"));
    }

    #[test]
    fn resolve_with_enforcing_action_actions_the_case() {
        let mut c = Case::open(params());
        c.drain_events();
        c.resolve(DecisionId::new(), ActionType::RemoveContent, t0(), Uuid::now_v7()).unwrap();
        assert_eq!(c.status(), CaseStatus::Actioned);
        let events = c.drain_events();
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].event_type(), "moderation.case_resolved");
    }

    #[test]
    fn resolve_with_no_action_dismisses() {
        let mut c = Case::open(params());
        c.resolve(DecisionId::new(), ActionType::NoAction, t0(), Uuid::now_v7()).unwrap();
        assert_eq!(c.status(), CaseStatus::Dismissed);
    }

    #[test]
    fn cannot_resolve_twice() {
        let mut c = Case::open(params());
        c.resolve(DecisionId::new(), ActionType::Warn, t0(), Uuid::now_v7()).unwrap();
        assert!(matches!(
            c.resolve(DecisionId::new(), ActionType::Ban, t0(), Uuid::now_v7()).unwrap_err(),
            ModerationError::CaseAlreadyResolved
        ));
    }

    #[test]
    fn cannot_add_signal_after_resolution() {
        let mut c = Case::open(params());
        c.resolve(DecisionId::new(), ActionType::Warn, t0(), Uuid::now_v7()).unwrap();
        assert!(matches!(
            c.add_signal(sig()).unwrap_err(),
            ModerationError::CaseAlreadyResolved
        ));
    }

    #[test]
    fn appeal_reopens_then_closes() {
        let mut c = Case::open(params());
        c.resolve(DecisionId::new(), ActionType::RemoveContent, t0(), Uuid::now_v7()).unwrap();
        c.mark_appealed().unwrap();
        assert_eq!(c.status(), CaseStatus::Appealed);
        // upheld ⇒ back to actioned
        c.close_appeal(false).unwrap();
        assert_eq!(c.status(), CaseStatus::Actioned);
    }

    #[test]
    fn overturned_appeal_dismisses() {
        let mut c = Case::open(params());
        c.resolve(DecisionId::new(), ActionType::RemoveContent, t0(), Uuid::now_v7()).unwrap();
        c.mark_appealed().unwrap();
        c.close_appeal(true).unwrap();
        assert_eq!(c.status(), CaseStatus::Dismissed);
    }

    #[test]
    fn signals_accrue_while_open() {
        let mut c = Case::open(params());
        assert!(c.add_signal(sig()).unwrap());
        assert!(c.add_signal(sig()).unwrap());
        assert_eq!(c.signals().len(), 2);
        assert_eq!(c.version(), 2);
    }

    #[test]
    fn signals_from_one_source_are_capped() {
        let mut c = Case::open(params());
        for _ in 0..MAX_SIGNALS_PER_SOURCE {
            assert!(c.add_signal(sig()).unwrap());
        }
        assert!(!c.add_signal(sig()).unwrap(), "the cap is reached");
        assert_eq!(c.signals().len(), MAX_SIGNALS_PER_SOURCE);
    }
}
