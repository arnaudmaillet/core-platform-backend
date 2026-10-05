use chrono::{DateTime, Utc};
use uuid::Uuid;

use crate::domain::aggregate::Appeal;
use crate::domain::value_object::{ActorId, AppealId, AppealStatus, DecisionId};
use crate::error::ModerationError;

/// Flat projection of the `appeals` table.
#[derive(Debug, sqlx::FromRow)]
pub struct AppealRow {
    pub id: Uuid,
    pub decision_id: Uuid,
    pub actor_id: Uuid,
    pub statement: String,
    pub status: String,
    pub filed_at: DateTime<Utc>,
    pub resolved_at: Option<DateTime<Utc>>,
    pub outcome: Option<String>,
}

impl TryFrom<AppealRow> for Appeal {
    type Error = ModerationError;

    fn try_from(row: AppealRow) -> Result<Self, Self::Error> {
        Ok(Appeal::reconstitute(
            AppealId::from_uuid(row.id),
            DecisionId::from_uuid(row.decision_id),
            ActorId::from_uuid(row.actor_id),
            row.statement,
            AppealStatus::try_from(row.status.as_str())?,
            row.filed_at,
            row.resolved_at,
            row.outcome,
        ))
    }
}
