use std::sync::Arc;

use async_trait::async_trait;
use base64::{engine::general_purpose::URL_SAFE_NO_PAD, Engine as _};
use chrono::{DateTime, Utc};
use scylla::DeserializeRow;
use scylla::statement::unprepared::Statement;
use scylla::value::CqlTimestamp;
use scylla_storage::{ScyllaClient, ScyllaStorageError};
use uuid::Uuid;

use crate::application::port::RecentlyDeleted;
use crate::domain::value_object::{PostId, ProfileId};
use crate::error::PostError;

fn scylla_err(e: scylla::errors::ExecutionError) -> PostError {
    PostError::Storage(ScyllaStorageError::from(e))
}

fn rows_err(ctx: &'static str, e: impl ToString) -> PostError {
    PostError::DomainViolation { field: ctx.to_owned(), message: e.to_string() }
}

fn token_err() -> PostError {
    PostError::DomainViolation { field: "page_token".into(), message: "invalid page token".into() }
}

#[derive(DeserializeRow)]
struct Row {
    deleted_at: CqlTimestamp,
    post_id:    Uuid,
}

/// ScyllaDB adapter for `post.deleted_by_profile` (30-day TTL).
pub struct ScyllaRecentlyDeleted {
    client: Arc<ScyllaClient>,
}

impl ScyllaRecentlyDeleted {
    pub fn new(client: Arc<ScyllaClient>) -> Self {
        Self { client }
    }
}

/// `deleted_at_ms:post_id` of the last row of a page.
fn encode(row: &Row) -> String {
    URL_SAFE_NO_PAD.encode(format!("{}:{}", row.deleted_at.0, row.post_id))
}

fn decode(token: &str) -> Result<(i64, Uuid), PostError> {
    let raw = String::from_utf8(URL_SAFE_NO_PAD.decode(token).map_err(|_| token_err())?).map_err(|_| token_err())?;
    let (ms, id) = raw.split_once(':').ok_or_else(token_err)?;
    Ok((ms.parse().map_err(|_| token_err())?, Uuid::parse_str(id).map_err(|_| token_err())?))
}

#[async_trait]
impl RecentlyDeleted for ScyllaRecentlyDeleted {
    async fn add(&self, author: &ProfileId, deleted_at: DateTime<Utc>, post: &PostId) -> Result<(), PostError> {
        let stmt = Statement::new(
            "INSERT INTO post.deleted_by_profile (profile_id, deleted_at, post_id) VALUES (?, ?, ?)",
        );
        self.client
            .session
            .execute_unpaged(stmt, (author.as_uuid(), CqlTimestamp(deleted_at.timestamp_millis()), post.as_uuid()))
            .await
            .map_err(scylla_err)?;
        Ok(())
    }

    async fn remove(&self, author: &ProfileId, deleted_at: DateTime<Utc>, post: &PostId) -> Result<(), PostError> {
        let stmt = Statement::new(
            "DELETE FROM post.deleted_by_profile WHERE profile_id = ? AND deleted_at = ? AND post_id = ?",
        );
        self.client
            .session
            .execute_unpaged(stmt, (author.as_uuid(), CqlTimestamp(deleted_at.timestamp_millis()), post.as_uuid()))
            .await
            .map_err(scylla_err)?;
        Ok(())
    }

    async fn list(
        &self,
        author: &ProfileId,
        limit: i32,
        page_token: Option<&str>,
    ) -> Result<(Vec<PostId>, Option<String>), PostError> {
        let limit = limit.clamp(1, 100);
        let result = match page_token.map(decode).transpose()? {
            // (deleted_at, post_id) after the cursor in clustering order:
            // older deletions, or the same instant with a smaller post id.
            Some((ms, id)) => {
                let stmt = Statement::new(
                    "SELECT deleted_at, post_id FROM post.deleted_by_profile \
                     WHERE profile_id = ? AND (deleted_at, post_id) < (?, ?) LIMIT ?",
                );
                self.client.session.execute_unpaged(stmt, (author.as_uuid(), CqlTimestamp(ms), id, limit)).await
            }
            None => {
                let stmt = Statement::new(
                    "SELECT deleted_at, post_id FROM post.deleted_by_profile WHERE profile_id = ? LIMIT ?",
                );
                self.client.session.execute_unpaged(stmt, (author.as_uuid(), limit)).await
            }
        }
        .map_err(scylla_err)?;
        let rows: Vec<Row> = result
            .into_rows_result()
            .map_err(|e| rows_err("deleted_by_profile", e))?
            .rows::<Row>()
            .map_err(|e| rows_err("deleted_by_profile", e))?
            .collect::<Result<_, _>>()
            .map_err(|e| rows_err("deleted_by_profile", e))?;
        let next = (rows.len() == limit as usize).then(|| rows.last().map(encode)).flatten();
        Ok((rows.iter().map(|r| PostId::from_uuid(r.post_id)).collect(), next))
    }
}
