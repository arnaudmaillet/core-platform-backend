use async_trait::async_trait;
use postgres_storage::{StorageError, TransactionManager};
use tracing::instrument;

use crate::application::port::{DeviceHistory, SessionRepository};
use crate::domain::aggregate::Session;
use crate::domain::value_object::{AccountId, SessionId};
use crate::error::AuthError;

use super::model::SessionRow;

/// PostgreSQL adapter for [`SessionRepository`]. Writes route on `account_id`.
#[derive(Clone)]
pub struct PgSessionRepository {
    tx: TransactionManager,
}

impl PgSessionRepository {
    pub fn new(tx: TransactionManager) -> Self {
        Self { tx }
    }
}

fn storage(e: sqlx::Error) -> AuthError {
    AuthError::Storage(StorageError::from(e))
}

#[async_trait]
impl SessionRepository for PgSessionRepository {
    #[instrument(name = "auth.session.save", skip(self, session), fields(
        session.id = %session.id(), session.version = session.version()
    ))]
    async fn save(&self, session: &Session) -> Result<(), AuthError> {
        let id = session.id();
        let account_id = session.account_id();

        // Pre-materialize owned values so the 'static closure does not borrow.
        let p_account = account_id.as_uuid();
        let p_kind = session.kind().as_str();
        let p_issuer = session.subject().issuer().to_owned();
        let p_subject = session.subject().subject().to_owned();
        let p_generation = session.generation().value();
        let p_status = session.status().as_str().to_owned();
        let p_ua = session.device().user_agent().map(str::to_owned);
        let p_ip = session.device().ip_address().map(str::to_owned);
        let p_did = session.device().device_id().map(str::to_owned);
        let p_issued = session.issued_at();
        let p_expires = session.expires_at();
        let p_absolute = session.absolute_expiry();
        let p_revoked_at = session.revoked_at();
        let new_version = session.version();
        let id_uuid = id.as_uuid();

        if new_version == 0 {
            self.tx
                .run_on_shard(&account_id, move |tx| {
                    Box::pin(async move {
                        sqlx::query(
                            r#"
                            INSERT INTO sessions (
                                id, account_id, issuer, subject, generation, status,
                                device_user_agent, device_ip, device_id,
                                issued_at, expires_at, absolute_expiry, revoked_at, version,
                                kind
                            ) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15)
                            "#,
                        )
                        .bind(id_uuid)
                        .bind(p_account)
                        .bind(p_issuer)
                        .bind(p_subject)
                        .bind(p_generation)
                        .bind(p_status)
                        .bind(p_ua)
                        .bind(p_ip)
                        .bind(p_did)
                        .bind(p_issued)
                        .bind(p_expires)
                        .bind(p_absolute)
                        .bind(p_revoked_at)
                        .bind(0i64)
                        .bind(p_kind)
                        .execute(&mut **tx)
                        .await
                        .map(|_| ())
                        .map_err(storage)
                    })
                })
                .await
        } else {
            self.tx
                .run_on_shard(&account_id, move |tx| {
                    Box::pin(async move {
                        let affected = sqlx::query(
                            r#"
                            UPDATE sessions SET
                                status = $2,
                                expires_at = $3,
                                revoked_at = $4,
                                version = $5
                            WHERE id = $1 AND version = $6
                            "#,
                        )
                        .bind(id_uuid)
                        .bind(p_status)
                        .bind(p_expires)
                        .bind(p_revoked_at)
                        .bind(new_version)
                        .bind(new_version - 1)
                        .execute(&mut **tx)
                        .await
                        .map_err(storage)?
                        .rows_affected();

                        if affected == 0 {
                            Err(AuthError::ConcurrentModification)
                        } else {
                            Ok(())
                        }
                    })
                })
                .await
        }
    }

    #[instrument(name = "auth.session.find_by_id", skip(self), fields(session.id = %id))]
    async fn find_by_id(&self, id: &SessionId) -> Result<Option<Session>, AuthError> {
        // session_id is not the shard key; single-pool lookup (SingleNode/Cockroach).
        let row = sqlx::query_as::<_, SessionRow>("SELECT * FROM sessions WHERE id = $1")
            .bind(id.as_uuid())
            .fetch_optional(self.tx.pool())
            .await
            .map_err(storage)?;
        row.map(Session::try_from).transpose()
    }

    #[instrument(name = "auth.session.list_active_by_account", skip(self), fields(account.id = %account_id))]
    async fn list_active_by_account(
        &self,
        account_id: &AccountId,
    ) -> Result<Vec<Session>, AuthError> {
        let pool = self.tx.pool_for(account_id).map_err(AuthError::Storage)?;
        let rows = sqlx::query_as::<_, SessionRow>(
            "SELECT * FROM sessions WHERE account_id = $1 AND status = 'active' ORDER BY issued_at DESC",
        )
        .bind(account_id.as_uuid())
        .fetch_all(pool)
        .await
        .map_err(storage)?;
        rows.into_iter().map(Session::try_from).collect()
    }

    async fn device_history(&self, account_id: &AccountId, device_id: &str) -> Result<DeviceHistory, AuthError> {
        let pool = self.tx.pool_for(account_id).map_err(AuthError::Storage)?;
        let (any_session, seen_device): (bool, bool) = sqlx::query_as(
            "SELECT COUNT(*) > 0, COALESCE(BOOL_OR(device_id = $2), false) FROM sessions WHERE account_id = $1",
        )
        .bind(account_id.as_uuid())
        .bind(device_id)
        .fetch_one(pool)
        .await
        .map_err(storage)?;
        Ok(DeviceHistory { any_session, seen_device })
    }
}
