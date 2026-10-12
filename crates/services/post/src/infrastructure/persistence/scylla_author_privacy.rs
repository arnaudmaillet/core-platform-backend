use std::sync::Arc;

use async_trait::async_trait;
use scylla::DeserializeRow;
use scylla::statement::unprepared::Statement;
use scylla_storage::{ScyllaClient, ScyllaStorageError};

use crate::application::port::AuthorPrivacy;
use crate::domain::value_object::ProfileId;
use crate::error::PostError;

fn scylla_err(e: scylla::errors::ExecutionError) -> PostError {
    PostError::Storage(ScyllaStorageError::from(e))
}

fn rows_err(ctx: &'static str, e: impl ToString) -> PostError {
    PostError::DomainViolation { field: ctx.to_owned(), message: e.to_string() }
}

#[derive(DeserializeRow)]
struct PrivacyRow {
    private: Option<bool>,
}

/// ScyllaDB-backed author privacy projection (`post.author_visibility`).
pub struct ScyllaAuthorPrivacy {
    client: Arc<ScyllaClient>,
}

impl ScyllaAuthorPrivacy {
    pub fn new(client: Arc<ScyllaClient>) -> Self {
        Self { client }
    }
}

#[async_trait]
impl AuthorPrivacy for ScyllaAuthorPrivacy {
    async fn is_private(&self, author: &ProfileId) -> Result<bool, PostError> {
        let stmt = Statement::new("SELECT private FROM post.author_visibility WHERE profile_id = ?");
        let row = self
            .client
            .session
            .execute_unpaged(stmt, (author.as_uuid(),))
            .await
            .map_err(scylla_err)?
            .into_rows_result()
            .map_err(|e| rows_err("author_visibility_rows", e))?
            .maybe_first_row::<PrivacyRow>()
            .map_err(|e| rows_err("author_visibility_deser", e))?;
        Ok(row.and_then(|r| r.private).unwrap_or(false))
    }

    async fn set_private(&self, author: &ProfileId, private: bool) -> Result<(), PostError> {
        let stmt = Statement::new("INSERT INTO post.author_visibility (profile_id, private) VALUES (?, ?)");
        self.client.session.execute_unpaged(stmt, (author.as_uuid(), private)).await.map_err(scylla_err)?;
        Ok(())
    }
}
