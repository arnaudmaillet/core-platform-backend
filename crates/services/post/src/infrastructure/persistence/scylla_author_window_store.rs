use std::sync::Arc;

use async_trait::async_trait;
use scylla::DeserializeRow;
use scylla::statement::unprepared::Statement;
use scylla_storage::{ScyllaClient, ScyllaStorageError};

use crate::application::port::AuthorWindowStore;
use crate::domain::value_object::ProfileId;
use crate::error::PostError;

fn scylla_err(e: scylla::errors::ExecutionError) -> PostError {
    PostError::Storage(ScyllaStorageError::from(e))
}

fn rows_err(ctx: &'static str, e: impl ToString) -> PostError {
    PostError::DomainViolation { field: ctx.to_owned(), message: e.to_string() }
}

#[derive(DeserializeRow)]
struct WindowRow {
    window_days: Option<i32>,
}

/// ScyllaDB-backed `profile_id → post window` projection
/// (`post.author_post_windows`, upsert / point read).
pub struct ScyllaAuthorWindowStore {
    client: Arc<ScyllaClient>,
}

impl ScyllaAuthorWindowStore {
    pub fn new(client: Arc<ScyllaClient>) -> Self {
        Self { client }
    }
}

#[async_trait]
impl AuthorWindowStore for ScyllaAuthorWindowStore {
    async fn get(&self, profile_id: &ProfileId) -> Result<Option<u32>, PostError> {
        let stmt = Statement::new("SELECT window_days FROM post.author_post_windows WHERE profile_id = ?");
        let row = self
            .client
            .session
            .execute_unpaged(stmt, (profile_id.as_uuid(),))
            .await
            .map_err(scylla_err)?
            .into_rows_result()
            .map_err(|e| rows_err("author_window_rows", e))?
            .maybe_first_row::<WindowRow>()
            .map_err(|e| rows_err("author_window_deser", e))?;
        Ok(row.and_then(|r| r.window_days).and_then(|d| u32::try_from(d).ok()))
    }

    async fn set(&self, profile_id: &ProfileId, window_days: Option<u32>) -> Result<(), PostError> {
        let stmt = Statement::new("INSERT INTO post.author_post_windows (profile_id, window_days) VALUES (?, ?)");
        let days = window_days.map(|d| d as i32);
        self.client
            .session
            .execute_unpaged(stmt, (profile_id.as_uuid(), days))
            .await
            .map_err(scylla_err)?;
        Ok(())
    }
}
