//! [`ProfileSlotStore`] on `profile.profile_slots` (migration 0018, #916).

use std::sync::Arc;

use async_trait::async_trait;
use scylla::observability::history::HistoryListener;
use scylla::statement::unprepared::Statement;
use scylla_storage::{ProfileKind as ScyllaProfileKind, ScyllaClient, ScyllaStorageError};

use crate::application::port::ProfileSlotStore;
use crate::domain::value_object::AccountId;
use crate::error::ProfileError;

fn scylla_err(e: scylla::errors::ExecutionError) -> ProfileError {
    ProfileError::Storage(ScyllaStorageError::from(e))
}

fn row_err(ctx: &'static str, e: impl ToString) -> ProfileError {
    ProfileError::DomainViolation { field: ctx.to_owned(), message: e.to_string() }
}

pub struct ScyllaProfileSlotStore {
    client: Arc<ScyllaClient>,
}

impl ScyllaProfileSlotStore {
    pub fn new(client: Arc<ScyllaClient>) -> Self {
        Self { client }
    }

    fn stmt(&self, cql: &str) -> Statement {
        let mut s = Statement::new(cql);
        s.set_execution_profile_handle(Some(
            self.client.profiles.get(ScyllaProfileKind::Strict).clone().into_handle_with_label("strict".to_string()),
        ));
        s.set_history_listener(Arc::clone(&self.client.history_listener) as Arc<dyn HistoryListener>);
        s
    }

    /// The account's slots after an LWT: the stored value, applied or not.
    async fn slots_after(&self, account: &AccountId, fallback: i32) -> Result<i32, ProfileError> {
        Ok(self.get(account).await?.unwrap_or(fallback))
    }
}

#[async_trait]
impl ProfileSlotStore for ScyllaProfileSlotStore {
    async fn get(&self, account: &AccountId) -> Result<Option<i32>, ProfileError> {
        let stmt = self.stmt("SELECT slots FROM profile.profile_slots WHERE account_id = ?");
        let row = self
            .client
            .session
            .execute_unpaged(stmt, (account.as_uuid(),))
            .await
            .map_err(scylla_err)?
            .into_rows_result()
            .map_err(|e| row_err("profile_slots:rows", e))?
            .maybe_first_row::<(Option<i32>,)>()
            .map_err(|e| row_err("profile_slots:deser", e))?;
        Ok(row.and_then(|(slots,)| slots))
    }

    async fn init(&self, account: &AccountId, slots: i32) -> Result<i32, ProfileError> {
        let stmt = self.stmt("INSERT INTO profile.profile_slots (account_id, slots) VALUES (?, ?) IF NOT EXISTS");
        self.client.session.execute_unpaged(stmt, (account.as_uuid(), slots)).await.map_err(scylla_err)?;
        self.slots_after(account, slots).await
    }

    async fn grow(&self, account: &AccountId, from: i32, to: i32) -> Result<i32, ProfileError> {
        // No row is one slot: make it explicit before growing it.
        if from == 1 {
            self.init(account, 1).await?;
        }
        let stmt = self.stmt("UPDATE profile.profile_slots SET slots = ? WHERE account_id = ? IF slots = ?");
        self.client.session.execute_unpaged(stmt, (to, account.as_uuid(), from)).await.map_err(scylla_err)?;
        self.slots_after(account, from).await
    }

    async fn erase(&self, account: &AccountId) -> Result<(), ProfileError> {
        let stmt = self.stmt("DELETE FROM profile.profile_slots WHERE account_id = ?");
        self.client.session.execute_unpaged(stmt, (account.as_uuid(),)).await.map_err(scylla_err)?;
        Ok(())
    }
}
