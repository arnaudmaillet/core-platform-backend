use std::sync::Arc;

use async_trait::async_trait;
use scylla::frame::types::Consistency;
use scylla::response::query_result::QueryRowsResult;
use scylla::statement::unprepared::Statement;
use scylla_storage::{ScyllaClient, ScyllaStorageError};
use uuid::Uuid;

use crate::application::port::{CreateClaim, CreateKeys};
use crate::domain::value_object::{IdempotencyKey, PostId, ProfileId};
use crate::error::PostError;

/// How long a claimed key waits for its post: a call that dies between the
/// claim and the write frees its key after this.
const PENDING_TTL_SECS: i32 = 60;

/// How long a used key answers its post: long enough for any client retry.
const CREATED_TTL_SECS: i32 = 24 * 60 * 60;

fn scylla_err(e: scylla::errors::ExecutionError) -> PostError {
    PostError::Storage(ScyllaStorageError::from(e))
}

fn rows_err(ctx: &'static str, e: impl ToString) -> PostError {
    PostError::DomainViolation { field: ctx.to_owned(), message: e.to_string() }
}

/// The `[applied]` flag of an LWT result: column 0, read untyped (the row
/// carries the existing columns too when it was not applied).
fn applied(rows: QueryRowsResult, ctx: &'static str) -> Result<bool, PostError> {
    let row = rows.maybe_first_row::<scylla::value::Row>().map_err(|e| rows_err(ctx, e))?;
    Ok(matches!(
        row.and_then(|r| r.columns.into_iter().next().flatten()),
        Some(scylla::value::CqlValue::Boolean(true))
    ))
}

/// ScyllaDB adapter for `post.create_keys` (#876). Every write is an LWT, so
/// the claim, its completion and its release serialize per key.
pub struct ScyllaCreateKeys {
    client: Arc<ScyllaClient>,
}

impl ScyllaCreateKeys {
    pub fn new(client: Arc<ScyllaClient>) -> Self {
        Self { client }
    }

    async fn lwt(&self, cql: &str, values: impl scylla::serialize::row::SerializeRow, ctx: &'static str) -> Result<bool, PostError> {
        let result = self
            .client
            .session
            .execute_unpaged(Statement::new(cql), values)
            .await
            .map_err(scylla_err)?;
        applied(result.into_rows_result().map_err(|e| rows_err(ctx, e))?, ctx)
    }
}

#[async_trait]
impl CreateKeys for ScyllaCreateKeys {
    async fn claim(&self, author: &ProfileId, key: &IdempotencyKey, post_id: PostId) -> Result<CreateClaim, PostError> {
        let fresh = self
            .lwt(
                "INSERT INTO post.create_keys (profile_id, idempotency_key, post_id, created) \
                 VALUES (?, ?, ?, false) IF NOT EXISTS USING TTL ?",
                (author.as_uuid(), key.as_str(), post_id.as_uuid(), PENDING_TTL_SECS),
                "create_keys.claim",
            )
            .await?;
        if fresh {
            return Ok(CreateClaim::Fresh);
        }

        // Taken: read the holder's claim at serial consistency, so an LWT in
        // progress is seen committed.
        let mut read = Statement::new(
            "SELECT post_id, created FROM post.create_keys WHERE profile_id = ? AND idempotency_key = ?",
        );
        read.set_consistency(Consistency::LocalSerial);
        let row = self
            .client
            .session
            .execute_unpaged(read, (author.as_uuid(), key.as_str()))
            .await
            .map_err(scylla_err)?
            .into_rows_result()
            .map_err(|e| rows_err("create_keys.read", e))?
            .maybe_first_row::<(Option<Uuid>, Option<bool>)>()
            .map_err(|e| rows_err("create_keys.read", e))?;
        Ok(match row {
            Some((Some(post_id), Some(true))) => CreateClaim::Created(PostId::from_uuid(post_id)),
            // Pending, or expired between the claim and this read: the caller
            // retries, and its next claim finds the key settled either way.
            _ => CreateClaim::InFlight,
        })
    }

    async fn complete(&self, author: &ProfileId, key: &IdempotencyKey, post_id: PostId) -> Result<(), PostError> {
        self.lwt(
            "UPDATE post.create_keys USING TTL ? SET post_id = ?, created = true \
             WHERE profile_id = ? AND idempotency_key = ? IF post_id = ?",
            (CREATED_TTL_SECS, post_id.as_uuid(), author.as_uuid(), key.as_str(), post_id.as_uuid()),
            "create_keys.complete",
        )
        .await
        .map(drop)
    }

    async fn release(&self, author: &ProfileId, key: &IdempotencyKey, post_id: PostId) -> Result<(), PostError> {
        self.lwt(
            "DELETE FROM post.create_keys WHERE profile_id = ? AND idempotency_key = ? IF post_id = ?",
            (author.as_uuid(), key.as_str(), post_id.as_uuid()),
            "create_keys.release",
        )
        .await
        .map(drop)
    }
}
