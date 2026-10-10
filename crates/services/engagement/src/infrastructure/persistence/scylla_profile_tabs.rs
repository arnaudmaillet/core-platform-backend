//! [`ProfileTabs`] on Scylla (migration 0009): `engagement.profile_tabs`; a
//! deleted profile's Likes tab (`liked_posts_by_profile`) goes with it.

use std::sync::Arc;

use async_trait::async_trait;
use scylla::observability::history::HistoryListener;
use scylla::statement::batch::{Batch, BatchType};
use scylla::statement::unprepared::Statement;
use scylla_storage::{ProfileKind as ScyllaProfileKind, ScyllaClient, ScyllaStorageError};

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

    async fn forget(&self, profile_id: &str, at_micros: i64) -> Result<(), EngagementError> {
        // Both partitions or neither, as of the deletion: a stake recorded
        // late with an earlier time stays deleted.
        let mut batch = Batch::new(BatchType::Logged);
        batch.set_execution_profile_handle(Some(
            self.client.profiles.get(ScyllaProfileKind::Strict).clone().into_handle_with_label("strict-batch".to_string()),
        ));
        batch.set_history_listener(Arc::clone(&self.client.history_listener) as Arc<dyn HistoryListener>);
        batch.set_timestamp(Some(at_micros));
        batch.append_statement("DELETE FROM engagement.profile_tabs WHERE profile_id = ?");
        batch.append_statement("DELETE FROM engagement.liked_posts_by_profile WHERE profile_id = ?");
        self.client.session.batch(&batch, ((profile_id,), (profile_id,))).await.map_err(scylla)?;
        Ok(())
    }
}
