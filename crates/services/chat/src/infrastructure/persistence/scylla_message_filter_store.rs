use std::sync::Arc;

use async_trait::async_trait;
use scylla::DeserializeRow;
use scylla_storage::ScyllaClient;
use text_filter::ContentFilter;

use crate::application::port::MessageFilterStore;
use crate::domain::value_object::ProfileId;
use crate::error::ChatError;
use crate::infrastructure::persistence::statement::{fast, row_err, scylla_err, strict};

#[derive(DeserializeRow)]
struct Row {
    hidden_words:     Option<Vec<String>>,
    filter_offensive: Option<bool>,
}

/// ScyllaDB adapter for the members' message filters (`chat.message_filters`).
pub struct ScyllaMessageFilterStore {
    client: Arc<ScyllaClient>,
}

impl ScyllaMessageFilterStore {
    pub fn new(client: Arc<ScyllaClient>) -> Self {
        Self { client }
    }
}

#[async_trait]
impl MessageFilterStore for ScyllaMessageFilterStore {
    async fn set(&self, profile: &ProfileId, filter: &ContentFilter) -> Result<(), ChatError> {
        let stmt = strict(
            &self.client,
            "INSERT INTO chat.message_filters (profile_id, hidden_words, filter_offensive) VALUES (?, ?, ?)",
        );
        self.client
            .session
            .execute_unpaged(stmt, (profile.as_uuid(), &filter.hidden_words, filter.filter_offensive))
            .await
            .map_err(scylla_err)?;
        Ok(())
    }

    async fn get(&self, profile: &ProfileId) -> Result<ContentFilter, ChatError> {
        let stmt = fast(
            &self.client,
            "SELECT hidden_words, filter_offensive FROM chat.message_filters WHERE profile_id = ?",
        );
        let row = self
            .client
            .session
            .execute_unpaged(stmt, (profile.as_uuid(),))
            .await
            .map_err(scylla_err)?
            .into_rows_result()
            .map_err(|e| row_err("message_filters", e))?
            .maybe_first_row::<Row>()
            .map_err(|e| row_err("message_filters", e))?;
        Ok(row
            .map(|r| ContentFilter {
                hidden_words:     r.hidden_words.unwrap_or_default(),
                filter_offensive: r.filter_offensive.unwrap_or(true),
            })
            .unwrap_or_default())
    }
}
