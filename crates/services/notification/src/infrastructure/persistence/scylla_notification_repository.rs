use std::sync::Arc;

use async_trait::async_trait;
use chrono::{DateTime, TimeZone, Utc};
use scylla::observability::history::HistoryListener;
use scylla::statement::unprepared::Statement;
use scylla::value::{Counter, CqlTimestamp};
use scylla_storage::{ProfileKind as ScyllaProfileKind, ScyllaClient, ScyllaStorageError};
use uuid::Uuid;

use crate::application::port::{NotificationRepository, NotificationSummary};
use crate::application::query::list_notifications::encode_cursor;
use crate::domain::aggregate::Notification;
use crate::domain::value_object::{NotificationKind, ProfileId, SubjectKind};
use crate::error::NotificationError;
use crate::infrastructure::persistence::model::NotificationRow;

fn scylla_err(e: scylla::errors::ExecutionError) -> NotificationError {
    NotificationError::Scylla(ScyllaStorageError::from(e))
}

fn row_err(ctx: &'static str, e: impl ToString) -> NotificationError {
    NotificationError::DomainViolation {
        field:   ctx.to_owned(),
        message: e.to_string(),
    }
}

pub struct ScyllaNotificationRepository {
    client: Arc<ScyllaClient>,
}

impl ScyllaNotificationRepository {
    pub fn new(client: Arc<ScyllaClient>) -> Self {
        Self { client }
    }

    fn strict_stmt(&self, cql: &str) -> Statement {
        let mut s = Statement::new(cql);
        s.set_execution_profile_handle(Some(
            self.client
                .profiles
                .get(ScyllaProfileKind::Strict)
                .clone()
                .into_handle_with_label("strict".to_string()),
        ));
        s.set_history_listener(
            Arc::clone(&self.client.history_listener) as Arc<dyn HistoryListener>,
        );
        s
    }

    fn fast_stmt(&self, cql: &str) -> Statement {
        let mut s = Statement::new(cql);
        s.set_execution_profile_handle(Some(
            self.client
                .profiles
                .get(ScyllaProfileKind::Fast)
                .clone()
                .into_handle_with_label("fast".to_string()),
        ));
        s.set_history_listener(
            Arc::clone(&self.client.history_listener) as Arc<dyn HistoryListener>,
        );
        s
    }
}

#[async_trait]
impl NotificationRepository for ScyllaNotificationRepository {
    async fn insert(&self, n: &Notification) -> Result<(), NotificationError> {
        let stmt = self.strict_stmt(
            "INSERT INTO notification.notifications_by_profile \
             (target_profile_id, created_at, notification_id, notification_kind, subject_kind, \
              subject_id, sender_profile_id, sender_count, sample_sender_ids, is_read) \
             VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        );
        self.client
            .session
            .execute_unpaged(
                stmt,
                (
                    n.target_profile_id().as_uuid(),
                    CqlTimestamp(n.created_at().timestamp_millis()),
                    n.id().as_uuid(),
                    n.kind().as_tinyint(),
                    n.subject_kind().as_tinyint(),
                    n.subject_id().as_uuid(),
                    n.sender_profile_id().as_uuid(),
                    n.sender_count(),
                    n.sample_sender_ids().to_vec(),
                    n.is_read(),
                ),
            )
            .await
            .map_err(scylla_err)?;
        Ok(())
    }

    async fn list_paginated(
        &self,
        profile_id: &ProfileId,
        limit:      i32,
        cursor:     Option<(i64, Uuid)>,
    ) -> Result<(Vec<NotificationSummary>, Option<String>), NotificationError> {
        const COLS: &str =
            "target_profile_id, created_at, notification_id, notification_kind, \
             subject_kind, subject_id, sender_profile_id, sender_count, sample_sender_ids, is_read";

        let rows: Vec<NotificationRow> = if let Some((cursor_ts, cursor_id)) = cursor {
            let stmt = self.fast_stmt(&format!(
                "SELECT {COLS} FROM notification.notifications_by_profile \
                 WHERE target_profile_id = ? \
                   AND (created_at, notification_id) < (?, ?) \
                 LIMIT ?"
            ));
            self.client
                .session
                .execute_unpaged(
                    stmt,
                    (
                        profile_id.as_uuid(),
                        CqlTimestamp(cursor_ts),
                        cursor_id,
                        limit,
                    ),
                )
                .await
                .map_err(scylla_err)?
                .into_rows_result()
                .map_err(|e| row_err("list_paginated:rows", e))?
                .rows::<NotificationRow>()
                .map_err(|e| row_err("list_paginated:iter", e))?
                .collect::<Result<Vec<_>, _>>()
                .map_err(|e| row_err("list_paginated:deser", e))?
        } else {
            let stmt = self.fast_stmt(&format!(
                "SELECT {COLS} FROM notification.notifications_by_profile \
                 WHERE target_profile_id = ? \
                 LIMIT ?"
            ));
            self.client
                .session
                .execute_unpaged(stmt, (profile_id.as_uuid(), limit))
                .await
                .map_err(scylla_err)?
                .into_rows_result()
                .map_err(|e| row_err("list_paginated:rows", e))?
                .rows::<NotificationRow>()
                .map_err(|e| row_err("list_paginated:iter", e))?
                .collect::<Result<Vec<_>, _>>()
                .map_err(|e| row_err("list_paginated:deser", e))?
        };

        let next_token = if rows.len() == limit as usize {
            rows.last().map(|r| encode_cursor(r.created_at.0, r.notification_id))
        } else {
            None
        };

        let summaries = rows
            .into_iter()
            .map(row_to_summary)
            .collect::<Result<Vec<_>, _>>()?;

        Ok((summaries, next_token))
    }

    async fn mark_read(
        &self,
        profile_id:      &ProfileId,
        notification_id: Uuid,
        created_at_ms:   i64,
    ) -> Result<bool, NotificationError> {
        // Read current state to determine if the notification exists and is unread.
        let select = self.fast_stmt(
            "SELECT is_read FROM notification.notifications_by_profile \
             WHERE target_profile_id = ? AND created_at = ? AND notification_id = ?",
        );
        let result = self.client
            .session
            .execute_unpaged(
                select,
                (
                    profile_id.as_uuid(),
                    CqlTimestamp(created_at_ms),
                    notification_id,
                ),
            )
            .await
            .map_err(scylla_err)?
            .into_rows_result()
            .map_err(|e| row_err("mark_read:rows", e))?;

        let is_read: bool = result
            .rows::<(bool,)>()
            .map_err(|e| row_err("mark_read:iter", e))?
            .next()
            .ok_or_else(|| NotificationError::NotificationNotFound {
                notification_id: notification_id.to_string(),
                profile_id:      profile_id.as_str(),
            })?
            .map_err(|e| row_err("mark_read:deser", e))?
            .0;

        if is_read {
            return Ok(false);
        }

        let update = self.strict_stmt(
            "UPDATE notification.notifications_by_profile \
             SET is_read = true \
             WHERE target_profile_id = ? AND created_at = ? AND notification_id = ?",
        );
        self.client
            .session
            .execute_unpaged(
                update,
                (
                    profile_id.as_uuid(),
                    CqlTimestamp(created_at_ms),
                    notification_id,
                ),
            )
            .await
            .map_err(scylla_err)?;

        Ok(true)
    }

    async fn increment_counter(&self, profile_id: &ProfileId) -> Result<(), NotificationError> {
        let stmt = self.strict_stmt(
            "UPDATE notification.notification_unread_counters \
             SET unread_count = unread_count + 1 \
             WHERE target_profile_id = ?",
        );
        self.client
            .session
            .execute_unpaged(stmt, (profile_id.as_uuid(),))
            .await
            .map_err(scylla_err)?;
        Ok(())
    }

    async fn decrement_counter(&self, profile_id: &ProfileId) -> Result<(), NotificationError> {
        let stmt = self.strict_stmt(
            "UPDATE notification.notification_unread_counters \
             SET unread_count = unread_count - 1 \
             WHERE target_profile_id = ?",
        );
        self.client
            .session
            .execute_unpaged(stmt, (profile_id.as_uuid(),))
            .await
            .map_err(scylla_err)?;
        Ok(())
    }

    async fn reset_counter(&self, profile_id: &ProfileId) -> Result<(), NotificationError> {
        let stmt = self.strict_stmt(
            "DELETE FROM notification.notification_unread_counters \
             WHERE target_profile_id = ?",
        );
        self.client
            .session
            .execute_unpaged(stmt, (profile_id.as_uuid(),))
            .await
            .map_err(scylla_err)?;
        Ok(())
    }

    async fn read_counter(&self, profile_id: &ProfileId) -> Result<i64, NotificationError> {
        let stmt = self.fast_stmt(
            "SELECT unread_count FROM notification.notification_unread_counters \
             WHERE target_profile_id = ?",
        );
        let result = self.client
            .session
            .execute_unpaged(stmt, (profile_id.as_uuid(),))
            .await
            .map_err(scylla_err)?
            .into_rows_result()
            .map_err(|e| row_err("read_counter:rows", e))?;

        // A `counter` column decodes as `Counter`, never as a bare `i64` (the
        // type check fails up front, even with no row).
        let count: i64 = result
            .rows::<(Counter,)>()
            .map_err(|e| row_err("read_counter:iter", e))?
            .next()
            .transpose()
            .map_err(|e| row_err("read_counter:deser", e))?
            .map(|(Counter(c),)| c)
            .unwrap_or(0);

        Ok(count)
    }
}

fn row_to_summary(row: NotificationRow) -> Result<NotificationSummary, NotificationError> {
    let kind = NotificationKind::from_tinyint(row.notification_kind)?;
    let subj = SubjectKind::from_tinyint(row.subject_kind)?;
    let created_at: DateTime<Utc> = Utc
        .timestamp_millis_opt(row.created_at.0)
        .single()
        .unwrap_or_else(Utc::now);

    Ok(NotificationSummary {
        notification_id:   row.notification_id,
        target_profile_id: row.target_profile_id,
        sender_profile_id: row.sender_profile_id,
        sample_sender_ids: row.sample_sender_ids,
        sender_count:      row.sender_count,
        kind,
        subject_kind:      subj,
        subject_id:        row.subject_id,
        created_at,
        is_read:           row.is_read,
    })
}
