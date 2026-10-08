use std::sync::Arc;

use async_trait::async_trait;
use scylla::observability::history::HistoryListener;
use scylla::statement::unprepared::Statement;
use scylla_storage::{ProfileKind as ScyllaProfileKind, ScyllaClient, ScyllaStorageError};
use uuid::Uuid;

use crate::application::port::CounterLedger;
use crate::domain::value_object::PostId;
use crate::error::EngagementError;

fn scylla_err(e: scylla::errors::ExecutionError) -> EngagementError {
    EngagementError::Scylla(ScyllaStorageError::from(e))
}

/// [`CounterLedger`] on Scylla: `engagement.post_interaction_counters`.
pub struct ScyllaCounterLedger {
    client: Arc<ScyllaClient>,
}

impl ScyllaCounterLedger {
    pub fn new(client: Arc<ScyllaClient>) -> Self {
        Self { client }
    }

    fn strict_stmt(&self, cql: &str) -> Statement {
        let mut s = Statement::new(cql);
        s.set_execution_profile_handle(Some(
            self.client.profiles.get(ScyllaProfileKind::Strict).clone().into_handle_with_label("strict".to_string()),
        ));
        s.set_history_listener(Arc::clone(&self.client.history_listener) as Arc<dyn HistoryListener>);
        s
    }

    async fn add(&self, column: &str, post: Uuid, delta: i64) -> Result<(), EngagementError> {
        if delta == 0 {
            return Ok(());
        }
        let stmt = self.strict_stmt(&format!(
            "UPDATE engagement.post_interaction_counters SET {column} = {column} + ? WHERE post_id = ?"
        ));
        self.client.session.execute_unpaged(stmt, (delta, post)).await.map_err(scylla_err)?;
        Ok(())
    }
}

#[async_trait]
impl CounterLedger for ScyllaCounterLedger {
    async fn apply_interaction_delta(
        &self,
        post_id:       &PostId,
        view_delta:    i64,
        share_delta:   i64,
        comment_delta: i64,
    ) -> Result<(), EngagementError> {
        let post = post_id.as_uuid();
        self.add("view_count", post, view_delta).await?;
        self.add("share_count", post, share_delta).await?;
        self.add("comment_count", post, comment_delta).await
    }
}
