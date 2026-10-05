use std::sync::Arc;

use chrono::{DateTime, Utc};
use cqrs::Envelope;

use crate::application::port::{
    AppealRepository, CaseRepository, DecisionRepository, EnforcementProjection,
    EnforcementRepository, EventPublisher, ReportRepository, SubjectResolver,
};
use crate::domain::aggregate::appeal::appealable_until;
use crate::domain::aggregate::{Appeal, Appellant, Decision, DecisionAuthor, DecisionParams};
use crate::domain::event::DomainEvent;
use crate::domain::value_object::{ActionType, ActorId, AppealId, CaseId, CaseStatus, DecisionId, ReporterKind};
use crate::error::ModerationError;

// ─── FileAppeal ───────────────────────────────────────────────────────────────

#[derive(Debug, Clone)]
pub struct FileAppealCommand {
    pub decision_id: DecisionId,
    /// The appellant's account: the sanctioned account, or a member who
    /// reported the content before the decision (DSA Art. 20(1)).
    pub actor_id: ActorId,
    pub statement: String,
}

pub struct FileAppealHandler {
    decisions: Arc<dyn DecisionRepository>,
    appeals: Arc<dyn AppealRepository>,
    cases: Arc<dyn CaseRepository>,
    reports: Arc<dyn ReportRepository>,
}

impl FileAppealHandler {
    pub fn new(
        decisions: Arc<dyn DecisionRepository>,
        appeals: Arc<dyn AppealRepository>,
        cases: Arc<dyn CaseRepository>,
        reports: Arc<dyn ReportRepository>,
    ) -> Self {
        Self { decisions, appeals, cases, reports }
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
        // The sanctioned account, or a member who reported that content before
        // the decision (its complainant, DSA Art. 20(1)); anyone else learns
        // nothing, not even that the decision exists.
        let appellant = if decision.subject().actor_id() == cmd.actor_id {
            Appellant::Sanctioned
        } else if self
            .reports
            .reported_before(ReporterKind::Member, &cmd.actor_id, decision.subject(), decision.decided_at())
            .await?
        {
            Appellant::Reporter
        } else {
            return Err(ModerationError::DecisionNotFound { id: cmd.decision_id.as_str() });
        };

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

        let appeal = Appeal::file(cmd.decision_id, cmd.actor_id, appellant, cmd.statement, now)?;
        let stored = self.appeals.file(&appeal).await?;
        if stored.id() != appeal.id() {
            // A concurrent file of the same appeal won the race.
            return Ok(stored);
        }

        // The sanctioned account's appeal moves the subject's actioned case into
        // the Appealed state (best-effort: the case may have been opened on a
        // different surface, cleaned up, or already be back under review after a
        // reporter's appeal). A reporter's leaves the case as decided until it
        // is resolved.
        if appellant == Appellant::Sanctioned {
            let case_id = CaseId::for_subject(decision.subject());
            if let Some(mut case) = self.cases.find_by_id(&case_id).await?
                && case.status() == CaseStatus::Actioned
            {
                case.mark_appealed()?;
                self.cases.save(&case).await?;
            }
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
/// `AppealResolved` names the appellant account's profiles, which
/// `notification` tells of the outcome (#744): they are read first, so an
/// unreachable profile directory resolves nothing (retry later).
pub struct ResolveAppealHandler {
    appeals: Arc<dyn AppealRepository>,
    decisions: Arc<dyn DecisionRepository>,
    enforcements: Arc<dyn EnforcementRepository>,
    cases: Arc<dyn CaseRepository>,
    projection: Arc<dyn EnforcementProjection>,
    subjects: Arc<dyn SubjectResolver>,
    publisher: Arc<dyn EventPublisher>,
}

impl ResolveAppealHandler {
    pub fn new(
        appeals: Arc<dyn AppealRepository>,
        decisions: Arc<dyn DecisionRepository>,
        enforcements: Arc<dyn EnforcementRepository>,
        cases: Arc<dyn CaseRepository>,
        projection: Arc<dyn EnforcementProjection>,
        subjects: Arc<dyn SubjectResolver>,
        publisher: Arc<dyn EventPublisher>,
    ) -> Self {
        Self { appeals, decisions, enforcements, cases, projection, subjects, publisher }
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

        // Who is told of the outcome, before anything is written.
        let profile_ids = self.subjects.profiles_of(&appeal.actor_id()).await?;
        appeal.resolve(cmd.overturn, cmd.rationale.clone(), now, correlation_id)?;
        self.appeals.save(&appeal).await?;

        // A reporter's appeal never reverses anything: upheld, the decision
        // stands; overturned, the case goes back to review for a new decision.
        if appeal.appellant() == Appellant::Reporter {
            if cmd.overturn {
                let case_id = CaseId::for_subject(original.subject());
                if let Some(mut case) = self.cases.find_by_id(&case_id).await?
                    && case.status().is_resolved_or_appealed()
                {
                    case.reopen_for_review()?;
                    self.cases.save(&case).await?;
                }
            }
            self.publish_all(addressed(appeal.drain_events(), &profile_ids)).await?;
            return Ok(ResolveAppealOutcome { appeal, reversal: None });
        }

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

        // 3. Close the case (overturned ⇒ dismissed; upheld ⇒ back to actioned)
        // — when the appeal holds it; a case already back under review (a
        // reporter's appeal overturned meanwhile) stays there.
        let case_id = CaseId::for_subject(original.subject());
        if let Some(mut case) = self.cases.find_by_id(&case_id).await?
            && case.status() == CaseStatus::Appealed
        {
            case.close_appeal(cmd.overturn)?;
            self.cases.save(&case).await?;
        }

        self.publish_all(addressed(appeal.drain_events(), &profile_ids)).await?;
        Ok(ResolveAppealOutcome { appeal, reversal })
    }

    async fn publish_all(
        &self,
        events: Vec<DomainEvent>,
    ) -> Result<(), ModerationError> {
        for event in &events {
            self.publisher.publish(event).await?;
        }
        Ok(())
    }
}

/// `events` with every `AppealResolved` naming `profile_ids` as its recipients.
fn addressed(mut events: Vec<DomainEvent>, profile_ids: &[String]) -> Vec<DomainEvent> {
    for event in &mut events {
        if let DomainEvent::AppealResolved(resolved) = event {
            resolved.profile_ids = profile_ids.to_vec();
        }
    }
    events
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::application::command::{DecideCaseCommand, OpenCaseCommand};
    use crate::application::fakes::{t0, Fixture};
    use crate::domain::value_object::{EntityType, PolicyCategory, SubjectRef};
    use crate::application::port::{CaseRepository, ReportRepository};
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

    /// #744: the outcome names the appellant account's profiles, which
    /// notification tells — read before anything is written.
    #[tokio::test]
    async fn the_outcome_names_the_appellants_profiles_or_resolves_nothing() {
        let fx = Fixture::new();
        let decision_id = actioned_decision(&fx, PolicyCategory::Harassment).await;
        let file = Envelope::new(
            Uuid::now_v7(),
            FileAppealCommand { decision_id, actor_id: subject().actor_id(), statement: "unfair".into() },
        );
        let appeal = fx.file_appeal_handler().handle(file, t0()).await.unwrap();

        // The profile directory is down: nothing is resolved, the appeal waits.
        fx.subjects.profiles_down.store(true, std::sync::atomic::Ordering::SeqCst);
        let err = fx.resolve_appeal_handler().handle(resolve(appeal.id(), false), t0()).await.unwrap_err();
        assert!(matches!(err, ModerationError::ContentDirectoryUnavailable), "{err:?}");
        let stored = fx.appeals.find_by_id(&appeal.id()).await.unwrap().unwrap();
        assert!(!stored.status().is_terminal(), "the appeal is still open");

        fx.subjects.profiles_down.store(false, std::sync::atomic::Ordering::SeqCst);
        fx.subjects.with_profiles(subject().actor_id(), &["pro-1", "pro-2"]);
        fx.publisher.clear();
        fx.resolve_appeal_handler().handle(resolve(appeal.id(), false), t0()).await.unwrap();
        let Some(DomainEvent::AppealResolved(resolved)) = fx.publisher.events().pop() else {
            panic!("AppealResolved last");
        };
        assert_eq!(resolved.profile_ids, vec!["pro-1".to_owned(), "pro-2".to_owned()]);
        assert!(!resolved.overturned && !resolved.by_reporter);
    }

    /// Events from before the field read as naming nobody.
    #[test]
    fn an_older_appeal_resolved_names_no_profile() {
        let json = serde_json::json!({
            "type": "appeal_resolved",
            "appeal_id": Uuid::now_v7(),
            "decision_id": Uuid::now_v7(),
            "actor_id": Uuid::now_v7(),
            "overturned": true,
            "occurred_at": t0(),
            "correlation_id": Uuid::now_v7(),
        });
        let Ok(DomainEvent::AppealResolved(e)) = serde_json::from_value::<DomainEvent>(json) else {
            panic!("decodes");
        };
        assert!(e.profile_ids.is_empty() && !e.by_reporter);
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

    /// Opens the subject's case and decides it with `action` at t0.
    async fn decided(fx: &Fixture, action: ActionType) -> DecisionId {
        let open = Envelope::new(
            Uuid::now_v7(),
            OpenCaseCommand {
                subject: subject(),
                category: PolicyCategory::Harassment,
                queue: "q".into(),
                priority: "p".into(),
            },
        );
        let case = fx.open_case_handler().handle(open, t0()).await.unwrap().case;
        let decide = Envelope::new(
            Uuid::now_v7(),
            DecideCaseCommand {
                case_id: case.id(),
                action,
                category: PolicyCategory::Harassment,
                rationale: "reviewed".into(),
                reviewer_id: "rev-1".into(),
                policy_version: "2026.06.1".into(),
            },
        );
        fx.decide_handler().handle(decide, t0()).await.unwrap().decision.id()
    }

    fn reporter() -> ActorId {
        ActorId::from_uuid(Uuid::from_u128(2))
    }

    async fn reported(fx: &Fixture, who: ActorId, at: chrono::DateTime<Utc>) {
        let report = crate::domain::aggregate::Report::file(
            who,
            ReporterKind::Member,
            subject(),
            PolicyCategory::Harassment,
            "abusive",
            at,
        )
        .unwrap();
        fx.reports.record(&report).await.unwrap();
    }

    fn file_as(who: ActorId, decision_id: DecisionId) -> Envelope<FileAppealCommand> {
        Envelope::new(Uuid::now_v7(), FileAppealCommand { decision_id, actor_id: who, statement: "it was abusive".into() })
    }

    fn resolve(appeal_id: AppealId, overturn: bool) -> Envelope<ResolveAppealCommand> {
        Envelope::new(
            Uuid::now_v7(),
            ResolveAppealCommand { appeal_id, overturn, rationale: "looked again".into(), reviewer_id: "rev-2".into() },
        )
    }

    #[tokio::test]
    async fn a_reporter_appeals_a_dismissal_and_an_overturn_sends_the_case_back_to_review() {
        let fx = Fixture::new();
        reported(&fx, reporter(), t0() - chrono::Duration::hours(1)).await;
        let decision_id = decided(&fx, ActionType::NoAction).await;
        let case_id = CaseId::for_subject(&subject());

        let appeal = fx.file_appeal_handler().handle(file_as(reporter(), decision_id), t0()).await.unwrap();
        assert_eq!(appeal.appellant(), Appellant::Reporter);
        assert_eq!(
            fx.cases.find_by_id(&case_id).await.unwrap().unwrap().status(),
            CaseStatus::Dismissed,
            "a reporter's appeal leaves the case as decided"
        );

        fx.publisher.clear();
        let out = fx.resolve_appeal_handler().handle(resolve(appeal.id(), true), t0()).await.unwrap();
        assert!(out.reversal.is_none(), "nothing is reversed");
        assert_eq!(fx.decisions.count(), 1, "no reversal decision");
        assert_eq!(
            fx.cases.find_by_id(&case_id).await.unwrap().unwrap().status(),
            CaseStatus::Triaged,
            "back to review for a new decision"
        );
        assert_eq!(fx.publisher.event_types(), vec!["moderation.appeal_resolved"]);
    }

    #[tokio::test]
    async fn a_reporters_appeal_never_lifts_an_enforcement() {
        let fx = Fixture::new();
        reported(&fx, reporter(), t0() - chrono::Duration::hours(1)).await;
        let decision_id = decided(&fx, ActionType::Suspend).await;
        assert!(fx.projection.is_actor_restricted(&subject().actor_id()).await.unwrap());

        // The reporter finds a suspension too light; overturned, it is reviewed
        // again — the suspension stands meanwhile.
        let appeal = fx.file_appeal_handler().handle(file_as(reporter(), decision_id), t0()).await.unwrap();
        fx.resolve_appeal_handler().handle(resolve(appeal.id(), true), t0()).await.unwrap();
        assert!(fx.projection.is_actor_restricted(&subject().actor_id()).await.unwrap());

        // The sanctioned account still has its own appeal, the case being back
        // under review; resolving it leaves the case there.
        let own = fx.file_appeal_handler().handle(file_as(subject().actor_id(), decision_id), t0()).await.unwrap();
        assert_eq!(own.appellant(), Appellant::Sanctioned);
        assert_ne!(own.id(), appeal.id());
        fx.resolve_appeal_handler().handle(resolve(own.id(), false), t0()).await.unwrap();
        assert_eq!(
            fx.cases.find_by_id(&CaseId::for_subject(&subject())).await.unwrap().unwrap().status(),
            CaseStatus::Triaged
        );
    }

    #[tokio::test]
    async fn a_reporters_overturn_during_the_sanctioned_accounts_appeal_reopens_the_review() {
        let fx = Fixture::new();
        reported(&fx, reporter(), t0() - chrono::Duration::hours(1)).await;
        let decision_id = decided(&fx, ActionType::Suspend).await;
        let own = fx.file_appeal_handler().handle(file_as(subject().actor_id(), decision_id), t0()).await.unwrap();
        let theirs = fx.file_appeal_handler().handle(file_as(reporter(), decision_id), t0()).await.unwrap();
        let case_id = CaseId::for_subject(&subject());
        assert_eq!(fx.cases.find_by_id(&case_id).await.unwrap().unwrap().status(), CaseStatus::Appealed);

        fx.resolve_appeal_handler().handle(resolve(theirs.id(), true), t0()).await.unwrap();
        assert_eq!(fx.cases.find_by_id(&case_id).await.unwrap().unwrap().status(), CaseStatus::Triaged);
        // The sanctioned account's appeal still resolves (overturned: lifted).
        let out = fx.resolve_appeal_handler().handle(resolve(own.id(), true), t0()).await.unwrap();
        assert!(out.reversal.is_some());
        assert!(!fx.projection.is_actor_restricted(&subject().actor_id()).await.unwrap());
    }

    #[tokio::test]
    async fn only_someone_who_reported_before_the_decision_may_appeal_it() {
        let fx = Fixture::new();
        let late = ActorId::from_uuid(Uuid::from_u128(3));
        reported(&fx, late, t0() + chrono::Duration::hours(1)).await; // after the decision
        let decision_id = decided(&fx, ActionType::NoAction).await;

        for who in [late, ActorId::from_uuid(Uuid::from_u128(4))] {
            let err = fx.file_appeal_handler().handle(file_as(who, decision_id), t0()).await.unwrap_err();
            assert!(matches!(err, ModerationError::DecisionNotFound { .. }), "{err:?}");
        }
    }
}
