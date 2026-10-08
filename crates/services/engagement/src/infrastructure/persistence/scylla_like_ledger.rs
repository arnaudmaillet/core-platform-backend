//! [`LikeLedger`] on Scylla (migration 0005): `likes_by_target` and
//! `likes_by_account`, one LOGGED batch, the stake's time as the write
//! timestamp so the newer total wins.

use std::sync::Arc;

use async_trait::async_trait;
use scylla::observability::history::HistoryListener;
use scylla::statement::batch::{Batch, BatchType};
use scylla::statement::unprepared::Statement;
use scylla::value::CqlTimestamp;
use scylla::DeserializeRow;
use scylla_storage::{ProfileKind as ScyllaProfileKind, ScyllaClient, ScyllaStorageError};
use uuid::Uuid;

use crate::application::port::{AccountLike, LikeLedger};
use crate::domain::value_object::LikeTarget;
use crate::error::EngagementError;

pub struct ScyllaLikeLedger {
    client: Arc<ScyllaClient>,
}

impl ScyllaLikeLedger {
    pub fn new(client: Arc<ScyllaClient>) -> Self {
        Self { client }
    }
}

#[async_trait]
impl LikeLedger for ScyllaLikeLedger {
    async fn record(
        &self,
        target: &LikeTarget,
        account: &str,
        profile_id: &str,
        total: i64,
        at_micros: i64,
    ) -> Result<(), EngagementError> {
        let account = Uuid::parse_str(account)
            .map_err(|_| EngagementError::DomainViolation { field: "account_id".into(), message: account.to_owned() })?;
        // Both tables or neither (a logged batch), the stake's time as the
        // write time: the newer total wins whatever the order.
        let mut batch = Batch::new(BatchType::Logged);
        batch.set_execution_profile_handle(Some(
            self.client.profiles.get(ScyllaProfileKind::Strict).clone().into_handle_with_label("strict-batch".to_string()),
        ));
        batch.set_history_listener(Arc::clone(&self.client.history_listener) as Arc<dyn HistoryListener>);
        batch.set_timestamp(Some(at_micros));
        batch.append_statement(
            "INSERT INTO engagement.likes_by_target (target_kind, target_id, account_id, total) VALUES (?, ?, ?, ?)",
        );
        batch.append_statement(
            "INSERT INTO engagement.likes_by_account (account_id, target_kind, target_id, total, profile_id, liked_at) \
             VALUES (?, ?, ?, ?, ?, ?)",
        );
        let liked_at = CqlTimestamp(at_micros / 1_000);
        let values = (
            (target.kind(), target.id(), account, total),
            (account, target.kind(), target.id(), total, profile_id, liked_at),
        );
        self.client.session.batch(&batch, values).await.map_err(|e| EngagementError::Scylla(ScyllaStorageError::from(e)))?;
        Ok(())
    }

    async fn list_by_account(
        &self,
        account: &str,
        limit: i32,
        after: Option<&LikeTarget>,
    ) -> Result<Vec<AccountLike>, EngagementError> {
        #[derive(DeserializeRow)]
        struct Row {
            target_kind: String,
            target_id:   String,
            total:       Option<i64>,
            profile_id:  Option<String>,
            liked_at:    Option<CqlTimestamp>,
        }
        let account = Uuid::parse_str(account)
            .map_err(|_| EngagementError::DomainViolation { field: "account_id".into(), message: account.to_owned() })?;
        let mut stmt = Statement::new(match after {
            Some(_) => {
                "SELECT target_kind, target_id, total, profile_id, liked_at FROM engagement.likes_by_account \
                 WHERE account_id = ? AND (target_kind, target_id) > (?, ?) LIMIT ?"
            }
            None => {
                "SELECT target_kind, target_id, total, profile_id, liked_at FROM engagement.likes_by_account \
                 WHERE account_id = ? LIMIT ?"
            }
        });
        stmt.set_execution_profile_handle(Some(
            self.client.profiles.get(ScyllaProfileKind::Strict).clone().into_handle_with_label("strict".to_string()),
        ));
        stmt.set_history_listener(Arc::clone(&self.client.history_listener) as Arc<dyn HistoryListener>);
        let result = match after {
            Some(t) => self.client.session.execute_unpaged(stmt, (account, t.kind(), t.id(), limit)).await,
            None => self.client.session.execute_unpaged(stmt, (account, limit)).await,
        }
        .map_err(|e| EngagementError::Scylla(ScyllaStorageError::from(e)))?;
        let rows = result
            .into_rows_result()
            .map_err(|e| EngagementError::DomainViolation { field: "likes_by_account".into(), message: e.to_string() })?;
        let mut likes = Vec::new();
        for row in rows
            .rows::<Row>()
            .map_err(|e| EngagementError::DomainViolation { field: "likes_by_account".into(), message: e.to_string() })?
        {
            let row = row.map_err(|e| EngagementError::DomainViolation { field: "likes_by_account".into(), message: e.to_string() })?;
            // A row the service cannot read is skipped, never guessed.
            let Ok(target) = LikeTarget::parse(&row.target_kind, &row.target_id) else { continue };
            likes.push(AccountLike {
                target,
                total:      row.total.unwrap_or(0),
                profile_id: row.profile_id.unwrap_or_default(),
                liked_at:   row
                    .liked_at
                    .and_then(|t| chrono::DateTime::from_timestamp_millis(t.0))
                    .unwrap_or_default(),
            });
        }
        Ok(likes)
    }
}
