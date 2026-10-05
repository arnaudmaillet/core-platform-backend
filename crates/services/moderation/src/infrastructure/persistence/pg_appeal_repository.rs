use async_trait::async_trait;
use postgres_storage::TransactionManager;

use crate::application::port::{AppealCursor, AppealRepository};
use crate::domain::aggregate::Appeal;
use crate::domain::value_object::{ActorId, AppealId, DecisionId};
use crate::error::ModerationError;

use super::model::AppealRow;
use super::storage_err;

/// PostgreSQL adapter for [`AppealRepository`].
#[derive(Clone)]
pub struct PgAppealRepository {
    tx: TransactionManager,
}

impl PgAppealRepository {
    pub fn new(tx: TransactionManager) -> Self {
        Self { tx }
    }
}

#[async_trait]
impl AppealRepository for PgAppealRepository {
    async fn save(&self, appeal: &Appeal) -> Result<(), ModerationError> {
        sqlx::query(
            r#"
            INSERT INTO appeals (
                id, decision_id, actor_id, statement, status, filed_at, resolved_at, outcome
            ) VALUES ($1,$2,$3,$4,$5,$6,$7,$8)
            ON CONFLICT (id) DO UPDATE SET
                status      = EXCLUDED.status,
                resolved_at = EXCLUDED.resolved_at,
                outcome     = EXCLUDED.outcome
            "#,
        )
        .bind(appeal.id().as_uuid())
        .bind(appeal.decision_id().as_uuid())
        .bind(appeal.actor_id().as_uuid())
        .bind(appeal.statement())
        .bind(appeal.status().as_str())
        .bind(appeal.filed_at())
        .bind(appeal.resolved_at())
        .bind(appeal.outcome())
        .execute(self.tx.pool())
        .await
        .map_err(storage_err)?;
        Ok(())
    }

    async fn file(&self, appeal: &Appeal) -> Result<Appeal, ModerationError> {
        // One appeal per (decision, appellant): a second (or a concurrent) file
        // inserts nothing, and the stored one is returned.
        sqlx::query(
            r#"
            INSERT INTO appeals (
                id, decision_id, actor_id, statement, status, filed_at, resolved_at, outcome
            ) VALUES ($1,$2,$3,$4,$5,$6,$7,$8)
            ON CONFLICT (decision_id, actor_id) DO NOTHING
            "#,
        )
        .bind(appeal.id().as_uuid())
        .bind(appeal.decision_id().as_uuid())
        .bind(appeal.actor_id().as_uuid())
        .bind(appeal.statement())
        .bind(appeal.status().as_str())
        .bind(appeal.filed_at())
        .bind(appeal.resolved_at())
        .bind(appeal.outcome())
        .execute(self.tx.pool())
        .await
        .map_err(storage_err)?;
        self.find_for(&appeal.decision_id(), &appeal.actor_id())
            .await?
            .ok_or_else(|| ModerationError::AppealNotFound { id: appeal.id().as_str() })
    }

    async fn find_for(&self, decision: &DecisionId, appellant: &ActorId) -> Result<Option<Appeal>, ModerationError> {
        let row = sqlx::query_as::<_, AppealRow>("SELECT * FROM appeals WHERE decision_id = $1 AND actor_id = $2")
            .bind(decision.as_uuid())
            .bind(appellant.as_uuid())
            .fetch_optional(self.tx.pool())
            .await
            .map_err(storage_err)?;
        row.map(Appeal::try_from).transpose()
    }

    async fn list_for_appellant(
        &self,
        appellant: &ActorId,
        after: Option<AppealCursor>,
        limit: usize,
    ) -> Result<Vec<Appeal>, ModerationError> {
        // Keyset pagination on (filed_at, id), both descending; the cursor's
        // bounds are NULL on the first page.
        let rows = sqlx::query_as::<_, AppealRow>(
            r#"
            SELECT * FROM appeals
            WHERE actor_id = $1
              AND ($2::timestamptz IS NULL OR (filed_at, id) < ($2, $3))
            ORDER BY filed_at DESC, id DESC
            LIMIT $4
            "#,
        )
        .bind(appellant.as_uuid())
        .bind(after.map(|c| c.filed_at))
        .bind(after.map(|c| c.id.as_uuid()))
        .bind(limit as i64)
        .fetch_all(self.tx.pool())
        .await
        .map_err(storage_err)?;
        rows.into_iter().map(Appeal::try_from).collect()
    }

    async fn find_by_id(&self, id: &AppealId) -> Result<Option<Appeal>, ModerationError> {
        let row = sqlx::query_as::<_, AppealRow>("SELECT * FROM appeals WHERE id = $1")
            .bind(id.as_uuid())
            .fetch_optional(self.tx.pool())
            .await
            .map_err(storage_err)?;
        row.map(Appeal::try_from).transpose()
    }
}
