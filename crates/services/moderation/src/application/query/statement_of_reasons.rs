use std::sync::Arc;

use chrono::{DateTime, Utc};
use cqrs::{Envelope, Query, QueryHandler};

use crate::application::port::{AppealRepository, DecisionRepository};
use crate::domain::aggregate::appeal::appealable_until;
use crate::domain::aggregate::Appeal;
use crate::domain::value_object::{ActionType, ActorId, DecisionId, PolicyCategory, SubjectRef};
use crate::error::ModerationError;

/// Fetch the DSA Statement of Reasons for a decision (compliance/back-office).
#[derive(Debug, Clone)]
pub struct GetStatementOfReasonsQuery {
    pub decision_id: DecisionId,
    /// The account reading it (the edge token), whose appeal of the decision
    /// comes with the statement; `None` over the mesh.
    pub reader: Option<ActorId>,
}

impl Query for GetStatementOfReasonsQuery {
    type Response = StatementOfReasons;
}

/// The DSA Article 17 machine-readable statement, derived from the decision ledger.
#[derive(Debug, Clone)]
pub struct StatementOfReasons {
    pub decision_id: DecisionId,
    pub subject: SubjectRef,
    pub category: PolicyCategory,
    pub action: ActionType,
    pub policy_version: String,
    pub facts: String,
    pub automated: bool,
    pub decided_at: DateTime<Utc>,
    /// Until when it may be appealed (DSA Art. 20(1)); `None` when the category
    /// is not appealable.
    pub appealable_until: Option<DateTime<Utc>>,
    /// The reader's appeal of this decision, if they filed one.
    pub appeal: Option<Appeal>,
}

pub struct GetStatementOfReasonsHandler {
    decisions: Arc<dyn DecisionRepository>,
    appeals: Arc<dyn AppealRepository>,
}

impl GetStatementOfReasonsHandler {
    pub fn new(decisions: Arc<dyn DecisionRepository>, appeals: Arc<dyn AppealRepository>) -> Self {
        Self { decisions, appeals }
    }
}

impl QueryHandler<GetStatementOfReasonsQuery> for GetStatementOfReasonsHandler {
    type Error = ModerationError;

    async fn handle(
        &self,
        envelope: Envelope<GetStatementOfReasonsQuery>,
    ) -> Result<StatementOfReasons, Self::Error> {
        let GetStatementOfReasonsQuery { decision_id: id, reader } = envelope.payload;
        let decision = self
            .decisions
            .find_by_id(&id)
            .await?
            .ok_or(ModerationError::DecisionNotFound { id: id.as_str() })?;
        Ok(StatementOfReasons {
            decision_id: decision.id(),
            subject: decision.subject().clone(),
            category: decision.category(),
            action: decision.action(),
            policy_version: decision.policy_version().as_str().to_owned(),
            facts: decision.rationale().to_owned(),
            automated: decision.is_automated(),
            decided_at: decision.decided_at(),
            appealable_until: decision.category().is_appealable().then(|| appealable_until(decision.decided_at())),
            appeal: match reader {
                Some(reader) => self.appeals.find_for(&id, &reader).await?,
                None => None,
            },
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::application::command::{DecideCaseCommand, OpenCaseCommand};
    use crate::application::fakes::{t0, Fixture};
    use crate::domain::value_object::{ActorId, EntityType};
    use uuid::Uuid;

    #[tokio::test]
    async fn builds_statement_from_a_recorded_decision() {
        let fx = Fixture::new();
        let subject = SubjectRef::new(EntityType::Post, "p1", ActorId::from_uuid(Uuid::from_u128(1)), "feed").unwrap();
        let open = Envelope::new(
            Uuid::now_v7(),
            OpenCaseCommand { subject, category: PolicyCategory::Hate, queue: "q".into(), priority: "p".into() },
        );
        let case = fx.open_case_handler().handle(open, t0()).await.unwrap().case;
        let decide = Envelope::new(
            Uuid::now_v7(),
            DecideCaseCommand {
                case_id: case.id(),
                action: ActionType::RemoveContent,
                category: PolicyCategory::Hate,
                rationale: "violates hate policy".into(),
                reviewer_id: "rev-1".into(),
                policy_version: "2026.06.1".into(),
            },
        );
        let decision_id = fx.decide_handler().handle(decide, t0()).await.unwrap().decision.id();

        let q = Envelope::new(Uuid::now_v7(), GetStatementOfReasonsQuery { decision_id, reader: None });
        let sor = fx.statement_of_reasons_handler().handle(q).await.unwrap();
        assert_eq!(sor.action, ActionType::RemoveContent);
        assert_eq!(sor.category, PolicyCategory::Hate);
        assert_eq!(sor.policy_version, "2026.06.1");
        assert_eq!(sor.facts, "violates hate policy");
        assert!(!sor.automated);
        assert_eq!(sor.appealable_until, Some(appealable_until(t0())));
        assert!(sor.appeal.is_none(), "nobody appealed; the mesh reads no appeal");

        // The sanctioned account appeals: its statement carries the appeal.
        let owner = ActorId::from_uuid(Uuid::from_u128(1));
        let file = Envelope::new(
            Uuid::now_v7(),
            crate::application::command::FileAppealCommand { decision_id, actor_id: owner, statement: "unfair".into() },
        );
        let appeal = fx.file_appeal_handler().handle(file, t0()).await.unwrap();
        let q = Envelope::new(Uuid::now_v7(), GetStatementOfReasonsQuery { decision_id, reader: Some(owner) });
        let sor = fx.statement_of_reasons_handler().handle(q).await.unwrap();
        assert_eq!(sor.appeal.map(|a| a.id()), Some(appeal.id()));
    }

    #[tokio::test]
    async fn missing_decision_errs() {
        let fx = Fixture::new();
        let q = Envelope::new(Uuid::now_v7(), GetStatementOfReasonsQuery { decision_id: DecisionId::new(), reader: None });
        let err = fx.statement_of_reasons_handler().handle(q).await.unwrap_err();
        assert!(matches!(err, ModerationError::DecisionNotFound { .. }));
    }
}
