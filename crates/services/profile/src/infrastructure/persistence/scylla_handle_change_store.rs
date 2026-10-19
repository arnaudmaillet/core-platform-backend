//! [`HandleChangeStore`] on `profile.handle_changes` (migration 0019, #911).

use std::sync::Arc;

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use scylla::observability::history::HistoryListener;
use scylla::statement::unprepared::Statement;
use scylla::value::CqlTimestamp;
use scylla_storage::{ProfileKind as ScyllaProfileKind, ScyllaClient, ScyllaStorageError};

use crate::application::handle_changes::{HandleChangeRecord, HandleChangeStore};
use crate::domain::value_object::AccountId;
use crate::error::ProfileError;

fn scylla_err(e: scylla::errors::ExecutionError) -> ProfileError {
    ProfileError::Storage(ScyllaStorageError::from(e))
}

fn row_err(ctx: &'static str, e: impl ToString) -> ProfileError {
    ProfileError::DomainViolation { field: ctx.to_owned(), message: e.to_string() }
}

pub struct ScyllaHandleChangeStore {
    client: Arc<ScyllaClient>,
}

impl ScyllaHandleChangeStore {
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
}

#[async_trait]
impl HandleChangeStore for ScyllaHandleChangeStore {
    async fn get(&self, account: &AccountId) -> Result<Option<HandleChangeRecord>, ProfileError> {
        let stmt = self.stmt("SELECT changes, last_change_at FROM profile.handle_changes WHERE account_id = ?");
        let row = self
            .client
            .session
            .execute_unpaged(stmt, (account.as_uuid(),))
            .await
            .map_err(scylla_err)?
            .into_rows_result()
            .map_err(|e| row_err("handle_changes:rows", e))?
            .maybe_first_row::<(Option<i32>, Option<CqlTimestamp>)>()
            .map_err(|e| row_err("handle_changes:deser", e))?;
        Ok(row.and_then(|(changes, last)| {
            Some(HandleChangeRecord { changes: changes?, last_at: DateTime::from_timestamp_millis(last?.0)? })
        }))
    }

    async fn record(&self, account: &AccountId, from: i32, at: DateTime<Utc>) -> Result<(), ProfileError> {
        let at = CqlTimestamp(at.timestamp_millis());
        let result = if from == 0 {
            let stmt = self.stmt(
                "INSERT INTO profile.handle_changes (account_id, changes, last_change_at) VALUES (?, 1, ?) IF NOT EXISTS",
            );
            self.client.session.execute_unpaged(stmt, (account.as_uuid(), at)).await
        } else {
            let stmt = self.stmt(
                "UPDATE profile.handle_changes SET changes = ?, last_change_at = ? WHERE account_id = ? IF changes = ?",
            );
            self.client.session.execute_unpaged(stmt, (from + 1, at, account.as_uuid(), from)).await
        };
        result.map_err(scylla_err)?;
        Ok(())
    }

    async fn erase(&self, account: &AccountId) -> Result<(), ProfileError> {
        let stmt = self.stmt("DELETE FROM profile.handle_changes WHERE account_id = ?");
        self.client.session.execute_unpaged(stmt, (account.as_uuid(),)).await.map_err(scylla_err)?;
        Ok(())
    }
}
