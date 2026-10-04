use std::sync::Arc;

use async_trait::async_trait;
use scylla::DeserializeRow;
use scylla::statement::unprepared::Statement;
use scylla_storage::{ScyllaClient, ScyllaStorageError};

use crate::application::port::AuthorLocationStore;
use crate::domain::value_object::{LocationSharing, ProfileId};
use crate::error::PostError;

fn scylla_err(e: scylla::errors::ExecutionError) -> PostError {
    PostError::Storage(ScyllaStorageError::from(e))
}

fn rows_err(ctx: &'static str, e: impl ToString) -> PostError {
    PostError::DomainViolation {
        field:   ctx.to_owned(),
        message: e.to_string(),
    }
}

#[derive(DeserializeRow)]
struct SharingRow {
    ghost: Option<bool>,
    city:  Option<bool>,
}

/// ScyllaDB-backed `profile_id → location sharing` projection (table
/// `post.author_location_settings`, upsert / point read).
pub struct ScyllaAuthorLocationStore {
    client: Arc<ScyllaClient>,
}

impl ScyllaAuthorLocationStore {
    pub fn new(client: Arc<ScyllaClient>) -> Self {
        Self { client }
    }
}

#[async_trait]
impl AuthorLocationStore for ScyllaAuthorLocationStore {
    async fn get(&self, profile_id: &ProfileId) -> Result<LocationSharing, PostError> {
        let stmt = Statement::new(
            "SELECT ghost, city FROM post.author_location_settings WHERE profile_id = ?",
        );
        let result = self
            .client
            .session
            .execute_unpaged(stmt, (profile_id.as_uuid(),))
            .await
            .map_err(scylla_err)?;

        let row = result
            .into_rows_result()
            .map_err(|e| rows_err("author_location_rows", e))?
            .maybe_first_row::<SharingRow>()
            .map_err(|e| rows_err("author_location_deser", e))?;

        Ok(row
            .map(|r| LocationSharing {
                ghost: r.ghost.unwrap_or(false),
                city:  r.city.unwrap_or(false),
            })
            .unwrap_or_default())
    }

    async fn set(&self, profile_id: &ProfileId, sharing: LocationSharing) -> Result<(), PostError> {
        let stmt = Statement::new(
            "INSERT INTO post.author_location_settings (profile_id, ghost, city) VALUES (?, ?, ?)",
        );
        self.client
            .session
            .execute_unpaged(stmt, (profile_id.as_uuid(), sharing.ghost, sharing.city))
            .await
            .map_err(scylla_err)?;
        Ok(())
    }
}
