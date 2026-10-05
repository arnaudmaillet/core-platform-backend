use chrono::{DateTime, Utc};
use uuid::Uuid;

use crate::application::port::FiledReport;
use crate::domain::aggregate::Report;
use crate::domain::value_object::{ActorId, CaseStatus, PolicyCategory, ReportId, ReporterKind};
use crate::error::ModerationError;

use super::subject_from;

/// Flat projection of a `reports` row joined with its case's status.
#[derive(Debug, sqlx::FromRow)]
pub struct FiledReportRow {
    pub id: Uuid,
    pub reporter_kind: String,
    pub reporter_id: Uuid,
    pub entity_type: String,
    pub entity_id: String,
    pub actor_id: Uuid,
    pub surface: String,
    pub category: String,
    pub reason: String,
    pub reported_at: DateTime<Utc>,
    /// `NULL` when the report's case is not persisted.
    pub case_status: Option<String>,
    /// The latest decision on the subject since the report, once the case is
    /// decided.
    pub decision_id: Option<Uuid>,
}

impl TryFrom<FiledReportRow> for FiledReport {
    type Error = ModerationError;

    fn try_from(row: FiledReportRow) -> Result<Self, Self::Error> {
        let subject = subject_from(&row.entity_type, row.entity_id, row.actor_id, row.surface)?;
        let report = Report::reconstitute(
            ReportId::from_uuid(row.id),
            ActorId::from_uuid(row.reporter_id),
            ReporterKind::try_from(row.reporter_kind.as_str())?,
            subject,
            PolicyCategory::try_from(row.category.as_str())?,
            row.reason,
            row.reported_at,
        );
        let case_status = row.case_status.as_deref().map(CaseStatus::try_from).transpose()?;
        let decision_id = row.decision_id.map(crate::domain::value_object::DecisionId::from_uuid);
        Ok(FiledReport { report, case_status, decision_id })
    }
}
