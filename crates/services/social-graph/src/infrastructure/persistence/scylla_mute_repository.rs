use std::collections::HashSet;
use std::sync::Arc;

use async_trait::async_trait;
use chrono::{TimeZone, Utc};
use scylla::observability::history::HistoryListener;
use scylla::statement::unprepared::Statement;
use scylla::value::CqlTimestamp;
use scylla::DeserializeRow;
use uuid::Uuid;

use scylla_storage::{ProfileKind as ScyllaProfileKind, ScyllaClient, ScyllaStorageError};

use crate::application::port::{MuteRepository, MAX_MUTES_READ};
use crate::domain::mute::{Mute, MuteScope, MuteScopes};
use crate::domain::value_object::ProfileId;
use crate::error::SocialGraphError;

fn scylla_err(e: scylla::errors::ExecutionError) -> SocialGraphError {
    SocialGraphError::Storage(ScyllaStorageError::from(e))
}

fn row_err(ctx: &'static str, e: impl ToString) -> SocialGraphError {
    SocialGraphError::DomainViolation { field: ctx.to_owned(), message: e.to_string() }
}

#[derive(DeserializeRow)]
struct MuteRow {
    muted_id: Uuid,
    posts:    Option<bool>,
    stories:  Option<bool>,
    messages: Option<bool>,
    muted_at: Option<CqlTimestamp>,
}

impl MuteRow {
    fn scopes(&self) -> MuteScopes {
        MuteScopes {
            posts:    self.posts.unwrap_or(false),
            stories:  self.stories.unwrap_or(false),
            messages: self.messages.unwrap_or(false),
        }
    }

    fn into_mute(self) -> Mute {
        let muted_at = self
            .muted_at
            .and_then(|t| Utc.timestamp_millis_opt(t.0).single())
            .unwrap_or_default();
        Mute { profile_id: ProfileId::from_uuid(self.muted_id), scopes: self.scopes(), muted_at }
    }
}

/// ScyllaDB-backed mutes (`social_graph.mutes`, partitioned by muter).
pub struct ScyllaMuteRepository {
    client: Arc<ScyllaClient>,
}

impl ScyllaMuteRepository {
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

    fn fast(&self, cql: &str) -> Statement {
        self.stmt(cql, ScyllaProfileKind::Fast, "fast")
    }

    fn strict(&self, cql: &str) -> Statement {
        self.stmt(cql, ScyllaProfileKind::Strict, "strict")
    }

    async fn rows(&self, stmt: Statement, values: impl scylla::serialize::row::SerializeRow, ctx: &'static str) -> Result<Vec<MuteRow>, SocialGraphError> {
        self.client
            .session
            .execute_unpaged(stmt, values)
            .await
            .map_err(scylla_err)?
            .into_rows_result()
            .map_err(|e| row_err(ctx, e))?
            .rows::<MuteRow>()
            .map_err(|e| row_err(ctx, e))?
            .collect::<Result<Vec<_>, _>>()
            .map_err(|e| row_err(ctx, e))
    }
}

#[async_trait]
impl MuteRepository for ScyllaMuteRepository {
    async fn upsert(&self, muter: &ProfileId, mute: &Mute) -> Result<(), SocialGraphError> {
        let stmt = self.strict(
            "INSERT INTO social_graph.mutes (muter_id, muted_id, posts, stories, messages, muted_at) \
             VALUES (?, ?, ?, ?, ?, ?)",
        );
        let s = mute.scopes;
        self.client
            .session
            .execute_unpaged(
                stmt,
                (
                    muter.as_uuid(),
                    mute.profile_id.as_uuid(),
                    s.posts,
                    s.stories,
                    s.messages,
                    CqlTimestamp(mute.muted_at.timestamp_millis()),
                ),
            )
            .await
            .map_err(scylla_err)?;
        Ok(())
    }

    async fn delete(&self, muter: &ProfileId, muted: &ProfileId) -> Result<(), SocialGraphError> {
        let stmt = self.strict("DELETE FROM social_graph.mutes WHERE muter_id = ? AND muted_id = ?");
        self.client
            .session
            .execute_unpaged(stmt, (muter.as_uuid(), muted.as_uuid()))
            .await
            .map_err(scylla_err)?;
        Ok(())
    }

    async fn scopes(&self, muter: &ProfileId, muted: &ProfileId) -> Result<MuteScopes, SocialGraphError> {
        let stmt = self.fast(
            "SELECT muted_id, posts, stories, messages, muted_at FROM social_graph.mutes \
             WHERE muter_id = ? AND muted_id = ?",
        );
        let rows = self.rows(stmt, (muter.as_uuid(), muted.as_uuid()), "mute_scopes").await?;
        Ok(rows.first().map(MuteRow::scopes).unwrap_or_default())
    }

    async fn list(
        &self,
        muter: &ProfileId,
        limit: i32,
        page_token: Option<&str>,
    ) -> Result<(Vec<Mute>, Option<String>), SocialGraphError> {
        let limit = limit.clamp(1, 100);
        const COLUMNS: &str = "SELECT muted_id, posts, stories, messages, muted_at FROM social_graph.mutes";
        let rows = match page_token {
            Some(token) => {
                let after = Uuid::parse_str(token).map_err(|_| SocialGraphError::DomainViolation {
                    field:   "page_token".into(),
                    message: "invalid mute page token".into(),
                })?;
                let stmt = self.fast(&format!("{COLUMNS} WHERE muter_id = ? AND muted_id > ? LIMIT ?"));
                self.rows(stmt, (muter.as_uuid(), after, limit), "list_mutes").await?
            }
            None => {
                let stmt = self.fast(&format!("{COLUMNS} WHERE muter_id = ? LIMIT ?"));
                self.rows(stmt, (muter.as_uuid(), limit), "list_mutes").await?
            }
        };
        let next = (rows.len() == limit as usize)
            .then(|| rows.last().map(|r| r.muted_id.to_string()))
            .flatten();
        Ok((rows.into_iter().map(MuteRow::into_mute).collect(), next))
    }

    async fn muted_by(&self, muters: &[ProfileId], scope: MuteScope) -> Result<HashSet<ProfileId>, SocialGraphError> {
        if muters.is_empty() {
            return Ok(HashSet::new());
        }
        let stmt = self.fast(
            "SELECT muted_id, posts, stories, messages, muted_at FROM social_graph.mutes \
             WHERE muter_id IN ? PER PARTITION LIMIT ?",
        );
        let ids: Vec<Uuid> = muters.iter().map(ProfileId::as_uuid).collect();
        let rows = self.rows(stmt, (ids, MAX_MUTES_READ), "muted_by").await?;
        Ok(rows
            .into_iter()
            .filter(|r| r.scopes().covers(scope))
            .map(|r| ProfileId::from_uuid(r.muted_id))
            .collect())
    }
}
