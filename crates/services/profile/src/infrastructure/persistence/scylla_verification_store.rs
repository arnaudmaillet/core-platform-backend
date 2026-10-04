use std::sync::Arc;

use async_trait::async_trait;
use scylla::DeserializeRow;
use scylla::observability::history::HistoryListener;
use scylla::statement::unprepared::Statement;
use scylla::value::CqlTimestamp;
use scylla_storage::{ProfileKind as ScyllaProfileKind, ScyllaClient, ScyllaStorageError};
use uuid::Uuid;

use crate::application::port::VerificationStore;
use crate::domain::entity::{VerificationRequest, VerificationStatus};
use crate::domain::value_object::ProfileId;
use crate::error::ProfileError;

/// The single partition of the staff queue (low volume by design).
const PENDING: &str = "pending";

fn scylla_err(e: scylla::errors::ExecutionError) -> ProfileError {
    ProfileError::Storage(ScyllaStorageError::from(e))
}

fn row_err(ctx: &'static str, e: impl ToString) -> ProfileError {
    ProfileError::DomainViolation { field: ctx.to_owned(), message: e.to_string() }
}

fn token_err() -> ProfileError {
    ProfileError::DomainViolation { field: "page_token".into(), message: "invalid page token".into() }
}

/// ScyllaDB adapter for `profile.verification_requests` / `verification_queue`.
pub struct ScyllaVerificationStore {
    client: Arc<ScyllaClient>,
}

impl ScyllaVerificationStore {
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

    async fn exec(&self, cql: &str, values: impl scylla::serialize::row::SerializeRow) -> Result<(), ProfileError> {
        let stmt = self.stmt(cql, ScyllaProfileKind::Strict, "strict");
        self.client.session.execute_unpaged(stmt, values).await.map_err(scylla_err)?;
        Ok(())
    }
}

#[async_trait]
impl VerificationStore for ScyllaVerificationStore {
    async fn get(&self, profile: &ProfileId) -> Result<Option<VerificationRequest>, ProfileError> {
        #[derive(DeserializeRow)]
        struct Row {
            request: Option<String>,
        }
        let stmt = self.stmt(
            "SELECT request FROM profile.verification_requests WHERE profile_id = ?",
            ScyllaProfileKind::Fast,
            "fast",
        );
        let row = self
            .client
            .session
            .execute_unpaged(stmt, (profile.as_uuid(),))
            .await
            .map_err(scylla_err)?
            .into_rows_result()
            .map_err(|e| row_err("verification_requests", e))?
            .maybe_first_row::<Row>()
            .map_err(|e| row_err("verification_requests", e))?;
        match row.and_then(|r| r.request) {
            Some(json) => serde_json::from_str(&json).map(Some).map_err(|e| row_err("verification_request", e)),
            None => Ok(None),
        }
    }

    async fn put(&self, profile: &ProfileId, request: &VerificationRequest) -> Result<(), ProfileError> {
        let json = serde_json::to_string(request).map_err(|e| row_err("verification_request", e))?;
        self.exec(
            "INSERT INTO profile.verification_requests (profile_id, request) VALUES (?, ?)",
            (profile.as_uuid(), json),
        )
        .await?;
        let at = CqlTimestamp(request.submitted_at.timestamp_millis());
        if request.status == VerificationStatus::Pending {
            self.exec(
                "INSERT INTO profile.verification_queue (bucket, submitted_at, profile_id) VALUES (?, ?, ?)",
                (PENDING, at, profile.as_uuid()),
            )
            .await
        } else {
            self.exec(
                "DELETE FROM profile.verification_queue WHERE bucket = ? AND submitted_at = ? AND profile_id = ?",
                (PENDING, at, profile.as_uuid()),
            )
            .await
        }
    }

    async fn pending(&self, limit: i32, page_token: Option<&str>) -> Result<(Vec<ProfileId>, Option<String>), ProfileError> {
        #[derive(DeserializeRow)]
        struct Row {
            submitted_at: CqlTimestamp,
            profile_id:   Uuid,
        }
        let limit = limit.clamp(1, 100);
        let result = match page_token {
            Some(token) => {
                let (ms, id) = token.split_once(':').ok_or_else(token_err)?;
                let ms: i64 = ms.parse().map_err(|_| token_err())?;
                let id = Uuid::parse_str(id).map_err(|_| token_err())?;
                let stmt = self.stmt(
                    "SELECT submitted_at, profile_id FROM profile.verification_queue \
                     WHERE bucket = ? AND (submitted_at, profile_id) > (?, ?) LIMIT ?",
                    ScyllaProfileKind::Fast,
                    "fast",
                );
                self.client.session.execute_unpaged(stmt, (PENDING, CqlTimestamp(ms), id, limit)).await
            }
            None => {
                let stmt = self.stmt(
                    "SELECT submitted_at, profile_id FROM profile.verification_queue WHERE bucket = ? LIMIT ?",
                    ScyllaProfileKind::Fast,
                    "fast",
                );
                self.client.session.execute_unpaged(stmt, (PENDING, limit)).await
            }
        }
        .map_err(scylla_err)?;
        let rows: Vec<Row> = result
            .into_rows_result()
            .map_err(|e| row_err("verification_queue", e))?
            .rows::<Row>()
            .map_err(|e| row_err("verification_queue", e))?
            .collect::<Result<_, _>>()
            .map_err(|e| row_err("verification_queue", e))?;
        let next = (rows.len() == limit as usize)
            .then(|| rows.last().map(|r| format!("{}:{}", r.submitted_at.0, r.profile_id)))
            .flatten();
        Ok((rows.into_iter().map(|r| ProfileId::from_uuid(r.profile_id)).collect(), next))
    }
}
