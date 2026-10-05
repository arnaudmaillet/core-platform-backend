use async_trait::async_trait;
use chrono::{DateTime, Utc};
use postgres_storage::TransactionManager;

use crate::application::port::{FiledReport, ReportCursor, ReportRepository};
use crate::domain::aggregate::Report;
use crate::domain::value_object::{ActorId, CaseId, ReporterKind, SubjectRef};
use crate::error::ModerationError;

use super::model::FiledReportRow;
use super::storage_err;

/// PostgreSQL adapter for [`ReportRepository`].
#[derive(Clone)]
pub struct PgReportRepository {
    tx: TransactionManager,
}

impl PgReportRepository {
    pub fn new(tx: TransactionManager) -> Self {
        Self { tx }
    }
}

#[async_trait]
impl ReportRepository for PgReportRepository {
    async fn record(&self, report: &Report) -> Result<(), ModerationError> {
        let subject = report.subject();
        sqlx::query(
            r#"
            INSERT INTO reports (
                id, reporter_kind, reporter_id, case_id, entity_type, entity_id,
                actor_id, surface, category, reason, reported_at
            ) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11)
            ON CONFLICT (id) DO NOTHING
            "#,
        )
        .bind(report.id().as_uuid())
        .bind(report.reporter_kind().as_str())
        .bind(report.reporter_id().as_uuid())
        .bind(CaseId::for_subject(subject).as_uuid())
        .bind(subject.entity_type().as_str())
        .bind(subject.entity_id())
        .bind(subject.actor_id().as_uuid())
        .bind(subject.surface())
        .bind(report.category().as_str())
        .bind(report.reason())
        .bind(report.reported_at())
        .execute(self.tx.pool())
        .await
        .map_err(storage_err)?;
        Ok(())
    }

    async fn list_for_reporter(
        &self,
        kind: ReporterKind,
        reporter_id: &ActorId,
        after: Option<ReportCursor>,
        limit: usize,
    ) -> Result<Vec<FiledReport>, ModerationError> {
        // Keyset pagination on (reported_at, id), both descending; the cursor's
        // bounds are NULL on the first page.
        let rows = sqlx::query_as::<_, FiledReportRow>(
            r#"
            SELECT r.id, r.reporter_kind, r.reporter_id, r.entity_type, r.entity_id,
                   r.actor_id, r.surface, r.category, r.reason, r.reported_at,
                   c.status AS case_status,
                   CASE WHEN c.status IN ('actioned', 'dismissed') THEN (
                       SELECT d.id FROM decisions d
                       WHERE d.entity_type = r.entity_type AND d.entity_id = r.entity_id
                         AND d.actor_id = r.actor_id AND d.decided_at >= r.reported_at
                       ORDER BY d.decided_at DESC
                       LIMIT 1
                   ) END AS decision_id
            FROM reports r
            LEFT JOIN cases c ON c.id = r.case_id
            WHERE r.reporter_kind = $1 AND r.reporter_id = $2
              AND ($3::timestamptz IS NULL OR (r.reported_at, r.id) < ($3, $4))
            ORDER BY r.reported_at DESC, r.id DESC
            LIMIT $5
            "#,
        )
        .bind(kind.as_str())
        .bind(reporter_id.as_uuid())
        .bind(after.map(|c| c.reported_at))
        .bind(after.map(|c| c.id.as_uuid()))
        .bind(limit as i64)
        .fetch_all(self.tx.pool())
        .await
        .map_err(storage_err)?;
        rows.into_iter().map(FiledReport::try_from).collect()
    }

    async fn reported_before(
        &self,
        kind: ReporterKind,
        reporter_id: &ActorId,
        subject: &SubjectRef,
        by: DateTime<Utc>,
    ) -> Result<bool, ModerationError> {
        sqlx::query_scalar::<_, bool>(
            r#"
            SELECT EXISTS (
                SELECT 1 FROM reports
                WHERE reporter_kind = $1 AND reporter_id = $2
                  AND entity_type = $3 AND entity_id = $4 AND actor_id = $5
                  AND reported_at <= $6
            )
            "#,
        )
        .bind(kind.as_str())
        .bind(reporter_id.as_uuid())
        .bind(subject.entity_type().as_str())
        .bind(subject.entity_id())
        .bind(subject.actor_id().as_uuid())
        .bind(by)
        .fetch_one(self.tx.pool())
        .await
        .map_err(storage_err)
    }
}
