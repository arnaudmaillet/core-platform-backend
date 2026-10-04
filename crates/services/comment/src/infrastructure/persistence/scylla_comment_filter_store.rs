use std::sync::Arc;

use async_trait::async_trait;
use scylla::DeserializeRow;
use scylla::observability::history::HistoryListener;
use scylla::statement::unprepared::Statement;
use scylla_storage::{ProfileKind as ScyllaProfileKind, ScyllaClient, ScyllaStorageError};

use crate::application::port::CommentFilterStore;
use crate::domain::comment_filter::CommentFilter;
use crate::domain::value_object::ProfileId;
use crate::error::CommentError;

fn scylla_err(e: scylla::errors::ExecutionError) -> CommentError {
    CommentError::Storage(ScyllaStorageError::from(e))
}

fn row_err(ctx: &'static str, e: impl ToString) -> CommentError {
    CommentError::DomainViolation { field: ctx.to_owned(), message: e.to_string() }
}

#[derive(DeserializeRow)]
struct Row {
    hidden_words:     Option<Vec<String>>,
    filter_offensive: Option<bool>,
}

/// ScyllaDB adapter for the post owners' comment filters (`comment.comment_filters`).
pub struct ScyllaCommentFilterStore {
    client: Arc<ScyllaClient>,
}

impl ScyllaCommentFilterStore {
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
}

#[async_trait]
impl CommentFilterStore for ScyllaCommentFilterStore {
    async fn get(&self, owner: &ProfileId) -> Result<CommentFilter, CommentError> {
        let stmt = self.stmt(
            "SELECT hidden_words, filter_offensive FROM comment.comment_filters WHERE profile_id = ?",
            ScyllaProfileKind::Fast,
            "fast",
        );
        let row = self
            .client
            .session
            .execute_unpaged(stmt, (owner.as_uuid(),))
            .await
            .map_err(scylla_err)?
            .into_rows_result()
            .map_err(|e| row_err("comment_filters", e))?
            .maybe_first_row::<Row>()
            .map_err(|e| row_err("comment_filters", e))?;
        Ok(match row {
            Some(r) => CommentFilter {
                hidden_words:     r.hidden_words.unwrap_or_default(),
                filter_offensive: r.filter_offensive.unwrap_or(true),
            },
            None => CommentFilter::default(),
        })
    }

    async fn set(&self, owner: &ProfileId, filter: &CommentFilter) -> Result<(), CommentError> {
        let stmt = self.stmt(
            "INSERT INTO comment.comment_filters (profile_id, hidden_words, filter_offensive) VALUES (?, ?, ?)",
            ScyllaProfileKind::Strict,
            "strict",
        );
        self.client
            .session
            .execute_unpaged(stmt, (owner.as_uuid(), &filter.hidden_words, filter.filter_offensive))
            .await
            .map_err(scylla_err)?;
        Ok(())
    }
}
