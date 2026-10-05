use std::sync::Arc;

use async_trait::async_trait;
use scylla::observability::history::HistoryListener;
use scylla::statement::unprepared::Statement;
use scylla_storage::{ProfileKind as ScyllaProfileKind, ScyllaClient, ScyllaStorageError};
use uuid::Uuid;

use crate::application::port::ShareTokenStore;
use crate::domain::value_object::ProfileId;
use crate::error::ProfileError;

fn scylla_err(e: scylla::errors::ExecutionError) -> ProfileError {
    ProfileError::Storage(ScyllaStorageError::from(e))
}

fn row_err(ctx: &'static str, e: impl ToString) -> ProfileError {
    ProfileError::DomainViolation { field: ctx.to_owned(), message: e.to_string() }
}

/// The `[applied]` flag of an LWT result (column 0, untyped: the row's shape
/// varies with the outcome and the Scylla version).
fn lwt_applied(rows: scylla::response::query_result::QueryRowsResult, ctx: &'static str) -> Result<bool, ProfileError> {
    let row = rows.maybe_first_row::<scylla::value::Row>().map_err(|e| row_err(ctx, e))?;
    Ok(matches!(row.and_then(|r| r.columns.into_iter().next().flatten()), Some(scylla::value::CqlValue::Boolean(true))))
}

/// ScyllaDB adapter for `profile.share_token_by_profile` / `share_tokens`
/// (migration 0015).
pub struct ScyllaShareTokenStore {
    client: Arc<ScyllaClient>,
}

impl ScyllaShareTokenStore {
    pub fn new(client: Arc<ScyllaClient>) -> Self {
        Self { client }
    }

    fn stmt(&self, cql: &str) -> Statement {
        let mut s = Statement::new(cql);
        s.set_execution_profile_handle(Some(
            self.client.profiles.get(ScyllaProfileKind::Strict).clone().into_handle_with_label("strict".to_string()),
        ));
        s.set_history_listener(Arc::clone(&self.client.history_listener) as Arc<dyn HistoryListener>);
        s
    }

    /// Points `token` at `profile` (idempotent).
    async fn index(&self, token: &str, profile: &ProfileId) -> Result<(), ProfileError> {
        let stmt = self.stmt("INSERT INTO profile.share_tokens (share_token, profile_id) VALUES (?, ?)");
        self.client.session.execute_unpaged(stmt, (token, profile.as_uuid())).await.map_err(scylla_err)?;
        Ok(())
    }
}

#[async_trait]
impl ShareTokenStore for ScyllaShareTokenStore {
    async fn current(&self, profile: &ProfileId) -> Result<Option<String>, ProfileError> {
        let stmt = self.stmt("SELECT share_token FROM profile.share_token_by_profile WHERE profile_id = ?");
        let row = self
            .client
            .session
            .execute_unpaged(stmt, (profile.as_uuid(),))
            .await
            .map_err(scylla_err)?
            .into_rows_result()
            .map_err(|e| row_err("share_token.current:rows", e))?
            .maybe_first_row::<(Option<String>,)>()
            .map_err(|e| row_err("share_token.current:deser", e))?;
        let token = row.and_then(|(t,)| t);
        // Repairs a lookup row a crash may have left unwritten.
        if let Some(token) = &token {
            self.index(token, profile).await?;
        }
        Ok(token)
    }

    async fn issue(&self, profile: &ProfileId, token: &str) -> Result<String, ProfileError> {
        let stmt = self.stmt("INSERT INTO profile.share_token_by_profile (profile_id, share_token) VALUES (?, ?) IF NOT EXISTS");
        let result = self.client.session.execute_unpaged(stmt, (profile.as_uuid(), token)).await.map_err(scylla_err)?;
        let applied =
            lwt_applied(result.into_rows_result().map_err(|e| row_err("share_token.issue:rows", e))?, "share_token.issue")?;
        if applied {
            self.index(token, profile).await?;
            return Ok(token.to_owned());
        }
        self.current(profile)
            .await?
            .ok_or_else(|| row_err("share_token.issue", "a concurrent issue left no token"))
    }

    async fn replace(&self, profile: &ProfileId, current: &str, next: &str) -> Result<bool, ProfileError> {
        let stmt = self.stmt("UPDATE profile.share_token_by_profile SET share_token = ? WHERE profile_id = ? IF share_token = ?");
        let result =
            self.client.session.execute_unpaged(stmt, (next, profile.as_uuid(), current)).await.map_err(scylla_err)?;
        if !lwt_applied(result.into_rows_result().map_err(|e| row_err("share_token.replace:rows", e))?, "share_token.replace")? {
            return Ok(false);
        }
        self.index(next, profile).await?;
        // Best effort: the old token no longer resolves either way (resolve
        // checks the profile's current one).
        let delete = self.stmt("DELETE FROM profile.share_tokens WHERE share_token = ?");
        let _ = self.client.session.execute_unpaged(delete, (current,)).await;
        Ok(true)
    }

    async fn resolve(&self, token: &str) -> Result<Option<ProfileId>, ProfileError> {
        let stmt = self.stmt("SELECT profile_id FROM profile.share_tokens WHERE share_token = ?");
        let row = self
            .client
            .session
            .execute_unpaged(stmt, (token,))
            .await
            .map_err(scylla_err)?
            .into_rows_result()
            .map_err(|e| row_err("share_token.resolve:rows", e))?
            .maybe_first_row::<(Option<Uuid>,)>()
            .map_err(|e| row_err("share_token.resolve:deser", e))?;
        let Some(profile) = row.and_then(|(p,)| p).map(ProfileId::from_uuid) else { return Ok(None) };
        // Only the profile's current token resolves.
        let stmt = self.stmt("SELECT share_token FROM profile.share_token_by_profile WHERE profile_id = ?");
        let current = self
            .client
            .session
            .execute_unpaged(stmt, (profile.as_uuid(),))
            .await
            .map_err(scylla_err)?
            .into_rows_result()
            .map_err(|e| row_err("share_token.resolve:rows", e))?
            .maybe_first_row::<(Option<String>,)>()
            .map_err(|e| row_err("share_token.resolve:deser", e))?
            .and_then(|(t,)| t);
        Ok((current.as_deref() == Some(token)).then_some(profile))
    }
}
