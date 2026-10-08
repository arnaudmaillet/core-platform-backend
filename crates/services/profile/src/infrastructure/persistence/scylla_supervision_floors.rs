//! [`SupervisionFloors`] on `profile.supervision_floors` (migration 0016).

use std::sync::Arc;

use async_trait::async_trait;
use scylla::observability::history::HistoryListener;
use scylla::statement::unprepared::Statement;
use scylla_storage::{ProfileKind as ScyllaProfileKind, ScyllaClient, ScyllaStorageError};

use crate::application::port::SupervisionFloors;
use crate::domain::value_object::{AccountId, InteractionAudience, SupervisionFloor};
use crate::error::ProfileError;

fn scylla_err(e: scylla::errors::ExecutionError) -> ProfileError {
    ProfileError::Storage(ScyllaStorageError::from(e))
}

fn row_err(ctx: &'static str, e: impl ToString) -> ProfileError {
    ProfileError::DomainViolation { field: ctx.to_owned(), message: e.to_string() }
}

pub fn audience_str(audience: InteractionAudience) -> &'static str {
    audience.as_str()
}

pub fn audience_from(s: &str) -> Option<InteractionAudience> {
    match s {
        "followers" => Some(InteractionAudience::Followers),
        "mutuals" => Some(InteractionAudience::Mutuals),
        "no_one" => Some(InteractionAudience::NoOne),
        _ => None,
    }
}

pub struct ScyllaSupervisionFloors {
    client: Arc<ScyllaClient>,
}

impl ScyllaSupervisionFloors {
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
}

type FloorRow = (Option<bool>, Option<String>, Option<String>, Option<bool>);

#[async_trait]
impl SupervisionFloors for ScyllaSupervisionFloors {
    async fn get(&self, account: &AccountId) -> Result<Option<SupervisionFloor>, ProfileError> {
        let stmt = self.stmt(
            "SELECT private_account, messages, comments, hidden_from_search FROM profile.supervision_floors \
             WHERE account_id = ?",
        );
        let row = self
            .client
            .session
            .execute_unpaged(stmt, (account.as_uuid(),))
            .await
            .map_err(scylla_err)?
            .into_rows_result()
            .map_err(|e| row_err("supervision_floors:rows", e))?
            .maybe_first_row::<FloorRow>()
            .map_err(|e| row_err("supervision_floors:deser", e))?;
        Ok(row.map(|(private_account, messages, comments, hidden)| SupervisionFloor {
            private_account:    private_account.unwrap_or(false),
            messages:           messages.as_deref().and_then(audience_from),
            comments:           comments.as_deref().and_then(audience_from),
            hidden_from_search: hidden.unwrap_or(false),
        }))
    }

    async fn put(&self, account: &AccountId, floor: &SupervisionFloor) -> Result<(), ProfileError> {
        let stmt = self.stmt(
            "INSERT INTO profile.supervision_floors (account_id, private_account, messages, comments, hidden_from_search) \
             VALUES (?, ?, ?, ?, ?)",
        );
        self.client
            .session
            .execute_unpaged(
                stmt,
                (
                    account.as_uuid(),
                    floor.private_account,
                    floor.messages.map(audience_str),
                    floor.comments.map(audience_str),
                    floor.hidden_from_search,
                ),
            )
            .await
            .map_err(scylla_err)?;
        Ok(())
    }

    async fn clear(&self, account: &AccountId) -> Result<(), ProfileError> {
        let stmt = self.stmt("DELETE FROM profile.supervision_floors WHERE account_id = ?");
        self.client.session.execute_unpaged(stmt, (account.as_uuid(),)).await.map_err(scylla_err)?;
        Ok(())
    }
}
