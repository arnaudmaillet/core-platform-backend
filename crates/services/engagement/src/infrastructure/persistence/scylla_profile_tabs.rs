//! [`ProfileTabs`] on Scylla (migration 0009): `engagement.profile_tabs`.

use std::sync::Arc;

use async_trait::async_trait;
use scylla::statement::unprepared::Statement;
use scylla_storage::{ScyllaClient, ScyllaStorageError};

use crate::application::port::ProfileTabs;
use crate::error::EngagementError;

pub struct ScyllaProfileTabs {
    client: Arc<ScyllaClient>,
}

impl ScyllaProfileTabs {
    pub fn new(client: Arc<ScyllaClient>) -> Self {
        Self { client }
    }
}

fn scylla(e: impl Into<ScyllaStorageError>) -> EngagementError {
    EngagementError::Scylla(e.into())
}

#[async_trait]
impl ProfileTabs for ScyllaProfileTabs {
    async fn shows_likes(&self, profile_id: &str) -> Result<bool, EngagementError> {
        let stmt = Statement::new("SELECT show_likes FROM engagement.profile_tabs WHERE profile_id = ?");
        let rows = self
            .client
            .session
            .execute_unpaged(stmt, (profile_id,))
            .await
            .map_err(scylla)?
            .into_rows_result()
            .map_err(|e| EngagementError::DomainViolation { field: "profile_tabs".into(), message: e.to_string() })?;
        let row = rows
            .maybe_first_row::<(Option<bool>,)>()
            .map_err(|e| EngagementError::DomainViolation { field: "profile_tabs".into(), message: e.to_string() })?;
        // Never told otherwise: shown (the default).
        Ok(row.and_then(|(shown,)| shown).unwrap_or(true))
    }

    async fn set_shows_likes(&self, profile_id: &str, shown: bool) -> Result<(), EngagementError> {
        let stmt = Statement::new("INSERT INTO engagement.profile_tabs (profile_id, show_likes) VALUES (?, ?)");
        self.client.session.execute_unpaged(stmt, (profile_id, shown)).await.map_err(scylla)?;
        Ok(())
    }
}
