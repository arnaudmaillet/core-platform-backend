//! [`LikeLedger`] on Scylla (migration 0005): `likes_by_target` and
//! `likes_by_account`, one LOGGED batch, the stake's time as the write
//! timestamp so the newer total wins.

use std::sync::Arc;

use async_trait::async_trait;
use scylla::observability::history::HistoryListener;
use scylla::statement::batch::{Batch, BatchType};
use scylla::value::CqlTimestamp;
use scylla_storage::{ProfileKind as ScyllaProfileKind, ScyllaClient, ScyllaStorageError};
use uuid::Uuid;

use crate::application::port::LikeLedger;
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
}
