use chrono::{DateTime, Duration, Utc};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::domain::event::{AppealResolved, DomainEvent};
use crate::domain::value_object::{ActorId, AppealId, AppealStatus, DecisionId};
use crate::error::ModerationError;

/// How long after a decision it may be appealed: DSA Art. 20(1) requires at
/// least six months. Past it, `FileAppeal` answers `AppealWindowClosed`.
pub const APPEAL_WINDOW: Duration = Duration::days(183);

/// When a decision taken at `decided_at` stops being appealable.
pub fn appealable_until(decided_at: DateTime<Utc>) -> DateTime<Utc> {
    decided_at + APPEAL_WINDOW
}

/// Who appeals a decision (DSA Art. 20(1) gives the complaint to both).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Appellant {
    /// The account the decision was taken against.
    Sanctioned,
    /// A member who reported the content and disagrees with the outcome (a
    /// dismissal, or an action they find too light). An overturn sends the case
    /// back to review; it never reverses an enforcement.
    Reporter,
}

impl Appellant {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Sanctioned => "sanctioned",
            Self::Reporter => "reporter",
        }
    }
}

impl TryFrom<&str> for Appellant {
    type Error = ModerationError;

    fn try_from(s: &str) -> Result<Self, Self::Error> {
        match s {
            "sanctioned" => Ok(Self::Sanctioned),
            "reporter" => Ok(Self::Reporter),
            other => Err(ModerationError::DomainViolation {
                field: "appeal.appellant".into(),
                message: format!("unknown appellant '{other}'"),
            }),
        }
    }
}

/// The **Appeal** aggregate — a challenge to a [`Decision`].
///
/// Whether a category is appealable at all is checked by the application layer
/// against the decision's category (legally-mandated removals are not appealable);
/// this aggregate owns the lifecycle once an appeal is admitted. Resolving an
/// appeal emits [`AppealResolved`]; an overturn additionally drives a reversal
/// decision + enforcement reversal in the application layer.
///
/// [`Decision`]: crate::domain::aggregate::Decision
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Appeal {
    id: AppealId,
    decision_id: DecisionId,
    /// The appellant's account.
    actor_id: ActorId,
    appellant: Appellant,
    statement: String,
    status: AppealStatus,
    filed_at: DateTime<Utc>,
    resolved_at: Option<DateTime<Utc>>,
    /// The reviewer's reasons once resolved, told to the appellant (DSA Art.
    /// 20(4)–(5)).
    outcome: Option<String>,

    #[serde(skip)]
    pending_events: Vec<DomainEvent>,
}

impl Appeal {
    /// Files an appeal. Rejects an empty statement — the appellant must state a
    /// case.
    pub fn file(
        decision_id: DecisionId,
        actor_id: ActorId,
        appellant: Appellant,
        statement: impl Into<String>,
        filed_at: DateTime<Utc>,
    ) -> Result<Self, ModerationError> {
        let statement = statement.into();
        if statement.trim().is_empty() {
            return Err(ModerationError::DomainViolation {
                field: "appeal.statement".into(),
                message: "an appeal must include a statement".into(),
            });
        }
        Ok(Self {
            id: AppealId::new(),
            decision_id,
            actor_id,
            appellant,
            statement,
            status: AppealStatus::Filed,
            filed_at,
            resolved_at: None,
            outcome: None,
            pending_events: Vec::new(),
        })
    }

    /// Reconstructs from storage (no events emitted).
    #[allow(clippy::too_many_arguments)]
    pub fn reconstitute(
        id: AppealId,
        decision_id: DecisionId,
        actor_id: ActorId,
        appellant: Appellant,
        statement: String,
        status: AppealStatus,
        filed_at: DateTime<Utc>,
        resolved_at: Option<DateTime<Utc>>,
        outcome: Option<String>,
    ) -> Self {
        Self {
            id,
            decision_id,
            actor_id,
            appellant,
            statement,
            status,
            filed_at,
            resolved_at,
            outcome,
            pending_events: Vec::new(),
        }
    }

    // ─── Queries ─────────────────────────────────────────────────────────────

    pub fn id(&self) -> AppealId {
        self.id
    }

    pub fn decision_id(&self) -> DecisionId {
        self.decision_id
    }

    pub fn actor_id(&self) -> ActorId {
        self.actor_id
    }

    pub fn appellant(&self) -> Appellant {
        self.appellant
    }

    pub fn statement(&self) -> &str {
        &self.statement
    }

    pub fn status(&self) -> AppealStatus {
        self.status
    }

    pub fn filed_at(&self) -> DateTime<Utc> {
        self.filed_at
    }

    pub fn resolved_at(&self) -> Option<DateTime<Utc>> {
        self.resolved_at
    }

    /// The reviewer's reasons, once resolved.
    pub fn outcome(&self) -> Option<&str> {
        self.outcome.as_deref()
    }

    // ─── Commands ────────────────────────────────────────────────────────────

    /// Moves a filed appeal into review.
    pub fn start_review(&mut self) -> Result<(), ModerationError> {
        self.transition_to(AppealStatus::UnderReview)
    }

    /// Resolves the appeal: `overturn = true` overturns the decision, `false`
    /// upholds it, for the reasons in `rationale` (told to the appellant).
    /// Emits [`AppealResolved`].
    pub fn resolve(
        &mut self,
        overturn: bool,
        rationale: impl Into<String>,
        now: DateTime<Utc>,
        correlation_id: Uuid,
    ) -> Result<(), ModerationError> {
        let rationale = rationale.into();
        if rationale.trim().is_empty() {
            return Err(ModerationError::DomainViolation {
                field: "appeal.rationale".into(),
                message: "a resolved appeal must state its reasons".into(),
            });
        }
        let target = if overturn {
            AppealStatus::Overturned
        } else {
            AppealStatus::Upheld
        };
        self.transition_to(target)?;
        self.resolved_at = Some(now);
        self.outcome = Some(rationale);
        self.pending_events.push(DomainEvent::AppealResolved(AppealResolved {
            appeal_id: self.id,
            decision_id: self.decision_id,
            actor_id: self.actor_id,
            by_reporter: self.appellant == Appellant::Reporter,
            overturned: overturn,
            occurred_at: now,
            correlation_id,
        }));
        Ok(())
    }

    /// Drains accumulated events for the unit-of-work to publish.
    pub fn drain_events(&mut self) -> Vec<DomainEvent> {
        std::mem::take(&mut self.pending_events)
    }

    fn transition_to(&mut self, next: AppealStatus) -> Result<(), ModerationError> {
        if self.status.is_terminal() {
            return Err(ModerationError::AppealAlreadyResolved);
        }
        if !self.status.can_transition_to(next) {
            return Err(ModerationError::DomainViolation {
                field: "appeal.status".into(),
                message: format!("illegal appeal transition {} → {}", self.status, next),
            });
        }
        self.status = next;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn t0() -> DateTime<Utc> {
        DateTime::parse_from_rfc3339("2026-06-25T12:00:00Z").unwrap().with_timezone(&Utc)
    }

    fn appeal() -> Appeal {
        Appeal::file(
            DecisionId::new(),
            ActorId::from_uuid(Uuid::from_u128(1)),
            Appellant::Sanctioned,
            "this was not harassment",
            t0(),
        )
        .unwrap()
    }

    #[test]
    fn file_requires_statement() {
        assert!(matches!(
            Appeal::file(DecisionId::new(), ActorId::from_uuid(Uuid::nil()), Appellant::Sanctioned, "   ", t0()).unwrap_err(),
            ModerationError::DomainViolation { .. }
        ));
    }

    #[test]
    fn overturn_emits_event() {
        let mut a = appeal();
        a.start_review().unwrap();
        a.resolve(true, "the post was satire", t0(), Uuid::now_v7()).unwrap();
        assert_eq!(a.status(), AppealStatus::Overturned);
        assert_eq!(a.outcome(), Some("the post was satire"));
        let events = a.drain_events();
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].event_type(), "moderation.appeal_resolved");
    }

    #[test]
    fn uphold_directly_from_filed_is_allowed() {
        let mut a = appeal();
        a.resolve(false, "it was harassment", t0(), Uuid::now_v7()).unwrap();
        assert_eq!(a.status(), AppealStatus::Upheld);
    }

    #[test]
    fn cannot_resolve_twice() {
        let mut a = appeal();
        a.resolve(false, "stands", t0(), Uuid::now_v7()).unwrap();
        assert!(matches!(
            a.resolve(true, "changed my mind", t0(), Uuid::now_v7()).unwrap_err(),
            ModerationError::AppealAlreadyResolved
        ));
    }

    #[test]
    fn a_resolution_states_its_reasons() {
        let mut a = appeal();
        assert!(matches!(
            a.resolve(false, "  ", t0(), Uuid::now_v7()).unwrap_err(),
            ModerationError::DomainViolation { .. }
        ));
        assert_eq!(a.status(), AppealStatus::Filed, "nothing changed");
    }

    #[test]
    fn a_decision_is_appealable_for_six_months_at_least() {
        assert!(APPEAL_WINDOW >= Duration::days(182));
        assert_eq!(appealable_until(t0()), t0() + Duration::days(183));
    }
}
