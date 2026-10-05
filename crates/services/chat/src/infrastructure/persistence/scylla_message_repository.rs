use std::sync::Arc;

use async_trait::async_trait;
use chrono::Utc;
use scylla::serialize::row::SerializeRow;
use scylla::statement::unprepared::Statement;
use scylla::value::CqlTimestamp;
use scylla_storage::ScyllaClient;
use uuid::Uuid;

use crate::application::port::{MessageRepository, MessageSummary};
use crate::domain::aggregate::Message;
use crate::domain::value_object::{ContentType, ConversationId};
use crate::error::ChatError;
use crate::infrastructure::persistence::bucket::{message_bucket, MAX_BUCKET_WALK};
use crate::infrastructure::persistence::model::MessageRow;
use crate::infrastructure::persistence::statement::{fast, row_err, scylla_err, strict};
use crate::infrastructure::persistence::time::{to_cql, to_utc};

const HISTORY_COLS: &str =
    "created_at, message_id, sender_id, content_type, body, media_ref, reply_to, withheld";

/// ScyllaDB adapter for the time-bucketed message log
/// (`chat.messages_by_conversation`).
pub struct ScyllaMessageRepository {
    client:       Arc<ScyllaClient>,
    /// Bucket window in hours; must match the writer's value cluster-wide so a
    /// message is always read from the partition it was written to.
    bucket_hours: u32,
}

impl ScyllaMessageRepository {
    pub fn new(client: Arc<ScyllaClient>, bucket_hours: u32) -> Self {
        Self { client, bucket_hours }
    }

    /// Executes a profiled statement and collects the resulting message rows.
    async fn run_rows(
        &self,
        stmt:   Statement,
        values: impl SerializeRow,
    ) -> Result<Vec<MessageRow>, ChatError> {
        self.client
            .session
            .execute_unpaged(stmt, values)
            .await
            .map_err(scylla_err)?
            .into_rows_result()
            .map_err(|e| row_err("message.history:rows", e))?
            .rows::<MessageRow>()
            .map_err(|e| row_err("message.history:iter", e))?
            .collect::<Result<Vec<_>, _>>()
            .map_err(|e| row_err("message.history:deser", e))
    }

    /// Reads up to `limit` rows from a single time bucket, newest-first.
    ///
    /// `cursor` is applied only for the bucket that contains it (the first bucket
    /// of a page walk); older buckets are wholly older than the cursor and need no
    /// predicate. `floor_ms` adds the Audience-Plane watermark predicate
    /// (`created_at >= ?`) server-side.
    async fn fetch_bucket(
        &self,
        conversation_id: Uuid,
        bucket:          i32,
        limit:           i32,
        cursor:          Option<(i64, Uuid)>,
        floor_ms:        Option<i64>,
    ) -> Result<Vec<MessageRow>, ChatError> {
        match (cursor, floor_ms) {
            (Some((cts, cid)), Some(floor)) => {
                let stmt = fast(&self.client, &format!(
                    "SELECT {HISTORY_COLS} FROM chat.messages_by_conversation \
                     WHERE conversation_id = ? AND bucket = ? \
                       AND (created_at, message_id) < (?, ?) \
                       AND created_at >= ? \
                     LIMIT ?"
                ));
                self.run_rows(
                    stmt,
                    (conversation_id, bucket, CqlTimestamp(cts), cid, CqlTimestamp(floor), limit),
                )
                .await
            }
            (Some((cts, cid)), None) => {
                let stmt = fast(&self.client, &format!(
                    "SELECT {HISTORY_COLS} FROM chat.messages_by_conversation \
                     WHERE conversation_id = ? AND bucket = ? \
                       AND (created_at, message_id) < (?, ?) \
                     LIMIT ?"
                ));
                self.run_rows(stmt, (conversation_id, bucket, CqlTimestamp(cts), cid, limit))
                    .await
            }
            (None, Some(floor)) => {
                let stmt = fast(&self.client, &format!(
                    "SELECT {HISTORY_COLS} FROM chat.messages_by_conversation \
                     WHERE conversation_id = ? AND bucket = ? \
                       AND created_at >= ? \
                     LIMIT ?"
                ));
                self.run_rows(stmt, (conversation_id, bucket, CqlTimestamp(floor), limit))
                    .await
            }
            (None, None) => {
                let stmt = fast(&self.client, &format!(
                    "SELECT {HISTORY_COLS} FROM chat.messages_by_conversation \
                     WHERE conversation_id = ? AND bucket = ? \
                     LIMIT ?"
                ));
                self.run_rows(stmt, (conversation_id, bucket, limit)).await
            }
        }
    }
}

#[async_trait]
impl MessageRepository for ScyllaMessageRepository {
    async fn insert(&self, m: &Message) -> Result<(), ChatError> {
        let bucket = message_bucket(m.created_at().timestamp_millis(), self.bucket_hours);

        // Strict (LocalQuorum): member writes are the durable source of truth.
        let stmt = strict(
            &self.client,
            "INSERT INTO chat.messages_by_conversation \
             (conversation_id, bucket, created_at, message_id, sender_id, content_type, \
              body, media_ref, reply_to, withheld) \
             VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        );
        self.client
            .session
            .execute_unpaged(
                stmt,
                (
                    m.conversation_id().as_uuid(),
                    bucket,
                    to_cql(m.created_at()),
                    m.id().as_uuid(),
                    m.sender_id().as_uuid(),
                    m.content_type().as_tinyint(),
                    m.content().as_str(),
                    m.media_ref(),
                    m.reply_to().map(|r| r.as_uuid()),
                    // NULL unless withheld: the column stays sparse.
                    m.withheld().then_some(true),
                ),
            )
            .await
            .map_err(scylla_err)?;
        Ok(())
    }

    async fn list_history(
        &self,
        conversation_id:     &ConversationId,
        limit:               i32,
        cursor:              Option<(i64, Uuid)>,
        floor_created_at_ms: Option<i64>,
    ) -> Result<(Vec<MessageSummary>, Option<(i64, Uuid)>), ChatError> {
        let target = limit.max(0) as usize;
        if target == 0 {
            return Ok((Vec::new(), None));
        }

        let conv = conversation_id.as_uuid();

        // Start at the cursor's bucket when paginating, otherwise the live tail.
        let start_ms = cursor.map(|(ts, _)| ts).unwrap_or_else(|| Utc::now().timestamp_millis());
        let mut bucket = message_bucket(start_ms, self.bucket_hours);

        // Lower bound on the walk: the watermark bucket for audience reads, else a
        // fixed look-back so a sparse-history request cannot scan unboundedly.
        let floor_bucket = floor_created_at_ms.map(|ms| message_bucket(ms, self.bucket_hours));
        let min_bucket = floor_bucket.unwrap_or(bucket - MAX_BUCKET_WALK);

        let mut out: Vec<MessageSummary> = Vec::with_capacity(target);
        let mut walked = 0;
        let mut first = true;

        while out.len() < target && bucket >= min_bucket && walked < MAX_BUCKET_WALK {
            let remaining = (target - out.len()) as i32;
            let page_cursor = if first { cursor } else { None };
            let rows = self
                .fetch_bucket(conv, bucket, remaining, page_cursor, floor_created_at_ms)
                .await?;

            for r in rows {
                out.push(message_summary(r)?);
            }

            first = false;
            bucket -= 1;
            walked += 1;
        }

        let next = if out.len() == target {
            out.last().map(|s| (s.created_at.timestamp_millis(), s.message_id))
        } else {
            None
        };

        Ok((out, next))
    }
}

fn message_summary(r: MessageRow) -> Result<MessageSummary, ChatError> {
    Ok(MessageSummary {
        message_id:   r.message_id,
        sender_id:    r.sender_id,
        content_type: ContentType::try_from(r.content_type)?,
        body:         r.body.unwrap_or_default(),
        media_ref:    r.media_ref,
        reply_to:     r.reply_to,
        created_at:   to_utc(r.created_at),
        withheld:     r.withheld.unwrap_or(false),
    })
}
