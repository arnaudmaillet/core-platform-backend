use std::collections::HashSet;
use std::sync::Arc;

use async_trait::async_trait;
use chrono::{DateTime, TimeZone, Utc};
use scylla::observability::history::HistoryListener;
use scylla::statement::unprepared::Statement;
use scylla::value::CqlTimestamp;
use scylla::DeserializeRow;
use uuid::Uuid;

use scylla_storage::{ProfileKind as ScyllaProfileKind, ScyllaClient, ScyllaStorageError};

use crate::application::port::RestrictionRepository;
use crate::domain::value_object::ProfileId;
use crate::error::SocialGraphError;

fn scylla_err(e: scylla::errors::ExecutionError) -> SocialGraphError {
    SocialGraphError::Storage(ScyllaStorageError::from(e))
}

fn row_err(ctx: &'static str, e: impl ToString) -> SocialGraphError {
    SocialGraphError::DomainViolation { field: ctx.to_owned(), message: e.to_string() }
}

#[derive(DeserializeRow)]
struct RestrictionRow {
    restricted_id: Uuid,
    restricted_at: Option<CqlTimestamp>,
}

/// ScyllaDB-backed restrictions (`social_graph.restrictions`, partitioned by
/// owner).
pub struct ScyllaRestrictionRepository {
    client: Arc<ScyllaClient>,
}

impl ScyllaRestrictionRepository {
    pub fn new(client: Arc<ScyllaClient>) -> Self {
        Self { client }
    }

    fn stmt(&self, cql: &str, kind: ScyllaProfileKind, label: &str) -> Statement {
        let mut s = Statement::new(cql);
        s.set_execution_profile_handle(Some(
            self.client.profiles.get(kind).clone().into_handle_with_label(label.to_string()),
        ));
        s.set_history_listener(Arc::clone(&self.client.history_listener) as Arc<dyn HistoryListener>);
        s
    }

    async fn rows(
        &self,
        stmt: Statement,
        values: impl scylla::serialize::row::SerializeRow,
        ctx: &'static str,
    ) -> Result<Vec<RestrictionRow>, SocialGraphError> {
        self.client
            .session
            .execute_unpaged(stmt, values)
            .await
            .map_err(scylla_err)?
            .into_rows_result()
            .map_err(|e| row_err(ctx, e))?
            .rows::<RestrictionRow>()
            .map_err(|e| row_err(ctx, e))?
            .collect::<Result<Vec<_>, _>>()
            .map_err(|e| row_err(ctx, e))
    }
}

#[async_trait]
impl RestrictionRepository for ScyllaRestrictionRepository {
    async fn add(&self, owner: &ProfileId, restricted: &ProfileId, at: DateTime<Utc>) -> Result<(), SocialGraphError> {
        let stmt = self.stmt(
            "INSERT INTO social_graph.restrictions (owner_id, restricted_id, restricted_at) VALUES (?, ?, ?)",
            ScyllaProfileKind::Strict,
            "strict",
        );
        self.client
            .session
            .execute_unpaged(stmt, (owner.as_uuid(), restricted.as_uuid(), CqlTimestamp(at.timestamp_millis())))
            .await
            .map_err(scylla_err)?;
        Ok(())
    }

    async fn remove(&self, owner: &ProfileId, restricted: &ProfileId) -> Result<(), SocialGraphError> {
        let stmt = self.stmt(
            "DELETE FROM social_graph.restrictions WHERE owner_id = ? AND restricted_id = ?",
            ScyllaProfileKind::Strict,
            "strict",
        );
        self.client
            .session
            .execute_unpaged(stmt, (owner.as_uuid(), restricted.as_uuid()))
            .await
            .map_err(scylla_err)?;
        Ok(())
    }

    async fn list(
        &self,
        owner: &ProfileId,
        limit: i32,
        page_token: Option<&str>,
    ) -> Result<(Vec<(ProfileId, DateTime<Utc>)>, Option<String>), SocialGraphError> {
        let limit = limit.clamp(1, 100);
        const COLUMNS: &str = "SELECT restricted_id, restricted_at FROM social_graph.restrictions";
        let rows = match page_token {
            Some(token) => {
                let after = Uuid::parse_str(token).map_err(|_| SocialGraphError::DomainViolation {
                    field:   "page_token".into(),
                    message: "invalid restriction page token".into(),
                })?;
                let stmt = self.stmt(
                    &format!("{COLUMNS} WHERE owner_id = ? AND restricted_id > ? LIMIT ?"),
                    ScyllaProfileKind::Fast,
                    "fast",
                );
                self.rows(stmt, (owner.as_uuid(), after, limit), "list_restricted").await?
            }
            None => {
                let stmt = self.stmt(&format!("{COLUMNS} WHERE owner_id = ? LIMIT ?"), ScyllaProfileKind::Fast, "fast");
                self.rows(stmt, (owner.as_uuid(), limit), "list_restricted").await?
            }
        };
        let next = (rows.len() == limit as usize)
            .then(|| rows.last().map(|r| r.restricted_id.to_string()))
            .flatten();
        let entries = rows
            .into_iter()
            .map(|r| {
                let at = r.restricted_at.and_then(|t| Utc.timestamp_millis_opt(t.0).single()).unwrap_or_default();
                (ProfileId::from_uuid(r.restricted_id), at)
            })
            .collect();
        Ok((entries, next))
    }

    async fn restricted_among(&self, owner: &ProfileId, candidates: &[ProfileId]) -> Result<HashSet<ProfileId>, SocialGraphError> {
        if candidates.is_empty() {
            return Ok(HashSet::new());
        }
        let stmt = self.stmt(
            "SELECT restricted_id, restricted_at FROM social_graph.restrictions \
             WHERE owner_id = ? AND restricted_id IN ?",
            ScyllaProfileKind::Fast,
            "fast",
        );
        let ids: Vec<Uuid> = candidates.iter().map(ProfileId::as_uuid).collect();
        let rows = self.rows(stmt, (owner.as_uuid(), ids), "restricted_among").await?;
        Ok(rows.into_iter().map(|r| ProfileId::from_uuid(r.restricted_id)).collect())
    }
}
