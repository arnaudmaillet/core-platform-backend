use std::sync::Arc;

use chrono::{DateTime, Utc};
use cqrs::Envelope;

use crate::application::port::{
    AppealRepository, CaseRepository, DecisionRepository, EnforcementProjection,
    EnforcementRepository, EventPublisher,
};
use crate::domain::aggregate::appeal::appealable_until;
use crate::domain::aggregate::{Appeal, Decision, DecisionAuthor, DecisionParams};
use crate::domain::value_object::{ActionType, ActorId, AppealId, CaseId, DecisionId};
use crate::error::ModerationError;

// ─── FileAppeal ───────────────────────────────────────────────────────────────

#[derive(Debug, Clone)]
pub struct FileAppealCommand {
    pub decision_id: DecisionId,
    pub actor_id: ActorId,
    pub statement: String,
}

pub struct FileAppealHandler {
    decisions: Arc<dyn DecisionRepository>,
    appeals: Arc<dyn AppealRepository>,
    cases: Arc<dyn CaseRepository>,
}

impl FileAppealHandler {
    pub fn new(
        decisions: Arc<dyn DecisionRepository>,
        appeals: Arc<dyn AppealRepository>,
        cases: Arc<dyn CaseRepository>,
    ) -> Self {
        Self { decisions, appeals, cases }
    }

    pub async fn handle(
        &self,
        envelope: Envelope<FileAppealCommand>,
        now: DateTime<Utc>,
    ) -> Result<Appeal, ModerationError> {
        let cmd = envelope.payload;
        let decision = self
            .decisions
            .find_by_id(&cmd.decision_id)
            .await?
            .ok_or(ModerationError::DecisionNotFound { id: cmd.decision_id.as_str() })?;
        // Only the sanctioned account appeals its decision; anyone else learns
        // nothing, not even that the decision exists.
        if decision.subject().actor_id() != cmd.actor_id {
            return Err(ModerationError::DecisionNotFound { id: cmd.decision_id.as_str() });
        }

        // Some categories (legally-mandated CSAM removals) are not appealable.
        if !decision.category().is_appealable() {
            return Err(ModerationError::NotAppealable);
        }

        // Already appealed: the same appeal, whatever its state (idempotent, like
        // SubmitReport) — never a second one, never a case transition again.
        if let Some(existing) = self.appeals.find_for(&cmd.decision_id, &cmd.actor_id).await? {
            return Ok(existing);
        }
        // DSA Art. 20(1): at least six months from the decision.
        if now > appealable_until(decision.decided_at()) {
            return Err(ModerationError::AppealWindowClosed);
        }

        let appeal = Appeal::file(cmd.decision_id, cmd.actor_id, cmd.statement, now)?;
        let stored = self.appeals.file(&appeal).await?;
        if stored.id() != appeal.id() {
            // A concurrent file of the same appeal won the race.
            return Ok(stored);
        }

        // Move the subject's case into the Appealed state (best-effort: the case may
        // have been opened on a different surface or already cleaned up).
        let case_id = CaseId::for_subject(decision.subject());
        if let Some(mut case) = self.cases.find_by_id(&case_id).await? {
            case.mark_appealed()?;
            self.cases.save(&case).await?;
        }
        Ok(appeal)
    }
}

// ─── ResolveAppeal ────────────────────────────────────────────────────────────

#[derive(Debug, Clone)]
pub struct ResolveAppealCommand {
    pub appeal_id: AppealId,
    pub overturn: bool,
    pub rationale: String,
    pub reviewer_id: String,
}

/// The resolved appeal and, on an overturn, the reversal decision recorded.
#[derive(Debug, Clone)]
pub struct ResolveAppealOutcome {
    pub appeal: Appeal,
    pub reversal: Option<Decision>,
}

/// Resolves an appeal. On overturn it records a reversal decision (a new
/// append-only entry referencing the original), reverses the active enforcement
/// the original decision created (clearing the hot-path projection for actor-level
/// ones), closes the case, and publishes `EnforcementReversed` + `AppealResolved`.
pub struct ResolveAppealHandler {
    appeals: Arc<dyn AppealRepository>,
    decisions: Arc<dyn DecisionRepository>,
    enforcements: Arc<dyn EnforcementRepository>,
    cases: Arc<dyn CaseRepository>,
    projection: Arc<dyn EnforcementProjection>,
    publisher: Arc<dyn EventPublisher>,
}

impl ResolveAppealHandler {
    pub fn new(
        appeals: Arc<dyn AppealRepository>,
        decisions: Arc<dyn DecisionRepository>,
        enforcements: Arc<dyn EnforcementRepository>,
        cases: Arc<dyn CaseRepository>,
        projection: Arc<dyn EnforcementProjection>,
        publisher: Arc<dyn EventPublisher>,
    ) -> Self {
        Self { appeals, decisions, enforcements, cases, projection, publisher }
    }

    pub async fn handle(
        &self,
        envelope: Envelope<ResolveAppealCommand>,
        now: DateTime<Utc>,
    ) -> Result<ResolveAppealOutcome, ModerationError> {
        let cmd = envelope.payload;
        let correlation_id = envelope.correlation_id;

        let mut appeal = self
            .appeals
            .find_by_id(&cmd.appeal_id)
            .await?
            .ok_or(ModerationError::AppealNotFound { id: cmd.appeal_id.as_str() })?;
        let original = self
            .decisions
            .find_by_id(&appeal.decision_id())
            .await?
            .ok_or(ModerationError::DecisionNotFound { id: appeal.decision_id().as_str() })?;

        appeal.resolve(cmd.overturn, cmd.rationale.clone(), now, correlation_id)?;
        self.appeals.save(&appeal).await?;

        let mut reversal = None;
        if cmd.overturn {
            // 1. Record the reversal decision (append-only; references the original).
            let rev = Decision::record_reversal(
                DecisionParams {
                    subject: original.subject().clone(),
                    action: ActionType::NoAction,
                    category: original.category(),
                    policy_version: original.policy_version().clone(),
                    rationale: cmd.rationale,
                    author: DecisionAuthor::Reviewer(cmd.reviewer_id),
                    decided_at: now,
                },
                original.id(),
            )?;
            self.decisions.append(&rev).await?;

            // 1b. Publish the compliance-evidence event (the audit feed) — the
            // reversal carries `reverses` linking it to the decision it supersedes.
            self.publisher
                .publish(&super::decision_recorded(&rev, correlation_id))
                .await?;

            // 2. Reverse the active enforcement(s) the original decision created.
            let actor = original.subject().actor_id();
            for mut enf in self.enforcements.list_active_for_actor(&actor).await? {
                if enf.decision_id() == original.id() && enf.is_active(now) {
                    enf.reverse(now, correlation_id)?;
                    self.enforcements.save(&enf).await?;
                    if enf.action().is_actor_level() {
                        self.projection.clear_actor_restriction(&actor, enf.version()).await?;
                    }
                    self.publish_all(enf.drain_events()).await?;
                }
            }
            reversal = Some(rev);
        }

        // 3. Close the case (overturned ⇒ dismissed; upheld ⇒ back to actioned).
        let case_id = CaseId::for_subject(original.subject());
        if let Some(mut case) = self.cases.find_by_id(&case_id).await? {
            case.close_appeal(cmd.overturn)?;
            self.cases.save(&case).await?;
        }

        self.publish_all(appeal.drain_events()).await?;
        Ok(ResolveAppealOutcome { appeal, reversal })
    }

    async fn publish_all(
        &self,
        events: Vec<crate::domain::event::DomainEvent>,
    ) -> Result<(), ModerationError> {
        for event in &events {
            self.publisher.publish(event).await?;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::application::command::{DecideCaseCommand, OpenCaseCommand};
    use crate::application::fakes::{t0, Fixture};
    use crate::domain::value_object::{EntityType, PolicyCategory, SubjectRef};
    use uuid::Uuid;

    fn subject() -> SubjectRef {
        SubjectRef::new(EntityType::Post, "p1", ActorId::from_uuid(Uuid::from_u128(1)), "feed").unwrap()
    }

    /// Drives a full open→decide(Suspend) so there is a real decision + active
    /// actor-level enforcement to appeal against. Returns the decision id.
    async fn actioned_decision(fx: &Fixture, category: PolicyCategory) -> DecisionId {
        let open = Envelope::new(
            Uuid::now_v7(),
            OpenCaseCommand { subject: subject(), category, queue: "q".into(), priority: "p".into() },
        );
        let case = fx.open_case_handler().handle(open, t0()).await.unwrap().case;
        let decide = Envelope::new(
            Uuid::now_v7(),
            DecideCaseCommand {
                case_id: case.id(),
                action: ActionType::Suspend,
                category,
                rationale: "violation".into(),
                reviewer_id: "rev-1".into(),
                policy_version: "2026.06.1".into(),
            },
        );
        fx.decide_handler().handle(decide, t0()).await.unwrap().decision.id()
    }

    #[tokio::test]
    async fn overturned_appeal_reverses_enforcement_and_clears_projection() {
        let fx = Fixture::new();
        let decision_id = actioned_decision(&fx, PolicyCategory::Harassment).await;
        assert!(fx.projection.is_actor_restricted(&subject().actor_id()).await.unwrap());

        // File then overturn.
        let file = Envelope::new(
            Uuid::now_v7(),
            FileAppealCommand { decision_id, actor_id: subject().actor_id(), statement: "unfair".into() },
        );
        let appeal = fx.file_appeal_handler().handle(file, t0()).await.unwrap();
        fx.publisher.clear();

        let resolve = Envelope::new(
            Uuid::now_v7(),
            ResolveAppealCommand {
                appeal_id: appeal.id(),
                overturn: true,
                rationale: "reviewer erred".into(),
                reviewer_id: "rev-2".into(),
            },
        );
        let out = fx.resolve_appeal_handler().handle(resolve, t0()).await.unwrap();

        assert!(out.reversal.is_some());
        assert_eq!(out.reversal.unwrap().reverses(), Some(decision_id));
        // Projection cleared; DecisionRecorded (the reversal), then
        // EnforcementReversed, then AppealResolved emitted.
        assert!(!fx.projection.is_actor_restricted(&subject().actor_id()).await.unwrap());
        assert_eq!(
            fx.publisher.event_types(),
            vec![
                "moderation.decision_recorded",
                "moderation.enforcement_reversed",
                "moderation.appeal_resolved"
            ]
        );
    }

    #[tokio::test]
    async fn upheld_appeal_keeps_enforcement() {
        let fx = Fixture::new();
        let decision_id = actioned_decision(&fx, PolicyCategory::Harassment).await;
        let file = Envelope::new(
            Uuid::now_v7(),
            FileAppealCommand { decision_id, actor_id: subject().actor_id(), statement: "unfair".into() },
        );
        let appeal = fx.file_appeal_handler().handle(file, t0()).await.unwrap();

        let resolve = Envelope::new(
            Uuid::now_v7(),
            ResolveAppealCommand {
                appeal_id: appeal.id(),
                overturn: false,
                rationale: "decision stands".into(),
                reviewer_id: "rev-2".into(),
            },
        );
        let out = fx.resolve_appeal_handler().handle(resolve, t0()).await.unwrap();
        assert!(out.reversal.is_none());
        assert!(fx.projection.is_actor_restricted(&subject().actor_id()).await.unwrap());
    }

    #[tokio::test]
    async fn csam_decision_is_not_appealable() {
        let fx = Fixture::new();
        let decision_id = actioned_decision(&fx, PolicyCategory::Csam).await;
        let file = Envelope::new(
            Uuid::now_v7(),
            FileAppealCommand { decision_id, actor_id: subject().actor_id(), statement: "x".into() },
        );
        let err = fx.file_appeal_handler().handle(file, t0()).await.unwrap_err();
        assert!(matches!(err, ModerationError::NotAppealable));
    }

    #[tokio::test]
    async fn a_second_appeal_on_the_same_decision_returns_the_first() {
        let fx = Fixture::new();
        let decision_id = actioned_decision(&fx, PolicyCategory::Harassment).await;
        let file = |statement: &str| {
            Envelope::new(
                Uuid::now_v7(),
                FileAppealCommand { decision_id, actor_id: subject().actor_id(), statement: statement.into() },
            )
        };
        let first = fx.file_appeal_handler().handle(file("unfair"), t0()).await.unwrap();
        let again = fx.file_appeal_handler().handle(file("really unfair"), t0()).await.unwrap();
        assert_eq!(again.id(), first.id());
        assert_eq!(again.statement(), "unfair", "the first stands");

        // Even once resolved: told the outcome, not a new appeal.
        let resolve = Envelope::new(
            Uuid::now_v7(),
            ResolveAppealCommand {
                appeal_id: first.id(),
                overturn: false,
                rationale: "the decision stands".into(),
                reviewer_id: "rev-2".into(),
            },
        );
        fx.resolve_appeal_handler().handle(resolve, t0()).await.unwrap();
        let after = fx.file_appeal_handler().handle(file("one more"), t0()).await.unwrap();
        assert_eq!(after.id(), first.id());
        assert_eq!(after.outcome(), Some("the decision stands"));
    }

    #[tokio::test]
    async fn an_appeal_after_the_window_is_refused() {
        let fx = Fixture::new();
        let decision_id = actioned_decision(&fx, PolicyCategory::Harassment).await;
        let file = Envelope::new(
            Uuid::now_v7(),
            FileAppealCommand { decision_id, actor_id: subject().actor_id(), statement: "late".into() },
        );
        let late = t0() + crate::domain::aggregate::appeal::APPEAL_WINDOW + chrono::Duration::seconds(1);
        let err = fx.file_appeal_handler().handle(file.clone(), late).await.unwrap_err();
        assert!(matches!(err, ModerationError::AppealWindowClosed));

        // On its last day it is still accepted.
        let last_day = t0() + crate::domain::aggregate::appeal::APPEAL_WINDOW;
        fx.file_appeal_handler().handle(file, last_day).await.expect("within the window");
    }
}
