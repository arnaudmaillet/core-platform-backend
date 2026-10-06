use async_trait::async_trait;
use chrono::{DateTime, Utc};
use postgres_storage::{StorageError, TransactionManager};
use tracing::instrument;

use crate::application::port::{PasskeyRepository, StoredPasskey};
use crate::domain::value_object::AccountId;
use crate::error::AuthError;

/// PostgreSQL adapter for [`PasskeyRepository`] (`passkeys`, migration 0007),
/// on the account's shard.
#[derive(Clone)]
pub struct PgPasskeyRepository {
    tx: TransactionManager,
}

impl PgPasskeyRepository {
    pub fn new(tx: TransactionManager) -> Self {
        Self { tx }
    }
}

fn storage(e: sqlx::Error) -> AuthError {
    AuthError::Storage(StorageError::from(e))
}

#[derive(sqlx::FromRow)]
struct PasskeyRow {
    credential_id:   Vec<u8>,
    public_key:      Vec<u8>,
    sign_count:      i64,
    name:            String,
    aaguid:          uuid::Uuid,
    backup_eligible: bool,
    backed_up:       bool,
    created_at:      DateTime<Utc>,
    last_used_at:    Option<DateTime<Utc>>,
}

impl From<PasskeyRow> for StoredPasskey {
    fn from(row: PasskeyRow) -> Self {
        Self {
            credential_id:   row.credential_id,
            public_key:      row.public_key,
            sign_count:      u32::try_from(row.sign_count).unwrap_or(u32::MAX),
            name:            row.name,
            aaguid:          row.aaguid,
            backup_eligible: row.backup_eligible,
            backed_up:       row.backed_up,
            created_at:      row.created_at,
            last_used_at:    row.last_used_at,
        }
    }
}

#[async_trait]
impl PasskeyRepository for PgPasskeyRepository {
    #[instrument(name = "auth.passkey.list", skip(self), fields(account.id = %account_id.as_str()))]
    async fn list(&self, account_id: &AccountId) -> Result<Vec<StoredPasskey>, AuthError> {
        let rows = sqlx::query_as::<_, PasskeyRow>(
            "SELECT credential_id, public_key, sign_count, name, aaguid, backup_eligible, backed_up, created_at, \
             last_used_at FROM passkeys WHERE account_id = $1 ORDER BY created_at, credential_id",
        )
        .bind(account_id.as_uuid())
        .fetch_all(self.tx.pool_for(account_id)?)
        .await
        .map_err(storage)?;
        Ok(rows.into_iter().map(StoredPasskey::from).collect())
    }

    #[instrument(name = "auth.passkey.add", skip(self, passkey), fields(account.id = %account_id.as_str()))]
    async fn add(&self, account_id: &AccountId, passkey: &StoredPasskey, max: usize) -> Result<(), AuthError> {
        let account = account_id.as_uuid();
        let passkey = passkey.clone();
        self.tx
            .run_on_shard(account_id, move |tx| {
                Box::pin(async move {
                    // One registration at a time per account, so the count holds.
                    sqlx::query("SELECT pg_advisory_xact_lock(hashtextextended($1::text, 808))")
                        .bind(account)
                        .execute(&mut **tx)
                        .await
                        .map_err(storage)?;
                    let held: i64 = sqlx::query_scalar("SELECT count(*) FROM passkeys WHERE account_id = $1")
                        .bind(account)
                        .fetch_one(&mut **tx)
                        .await
                        .map_err(storage)?;
                    if held as usize >= max {
                        return Err(AuthError::PasskeyLimitReached);
                    }
                    let inserted = sqlx::query(
                        "INSERT INTO passkeys (account_id, credential_id, public_key, sign_count, name, aaguid, \
                         backup_eligible, backed_up, created_at, last_used_at) \
                         VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10) ON CONFLICT DO NOTHING",
                    )
                    .bind(account)
                    .bind(&passkey.credential_id)
                    .bind(&passkey.public_key)
                    .bind(i64::from(passkey.sign_count))
                    .bind(&passkey.name)
                    .bind(passkey.aaguid)
                    .bind(passkey.backup_eligible)
                    .bind(passkey.backed_up)
                    .bind(passkey.created_at)
                    .bind(passkey.last_used_at)
                    .execute(&mut **tx)
                    .await
                    .map_err(storage)?
                    .rows_affected();
                    if inserted == 0 {
                        return Err(AuthError::PasskeyAlreadyRegistered);
                    }
                    Ok(())
                })
            })
            .await
    }

    #[instrument(name = "auth.passkey.remove", skip(self, credential_id), fields(account.id = %account_id.as_str()))]
    async fn remove(&self, account_id: &AccountId, credential_id: &[u8]) -> Result<bool, AuthError> {
        let removed = sqlx::query("DELETE FROM passkeys WHERE account_id = $1 AND credential_id = $2")
            .bind(account_id.as_uuid())
            .bind(credential_id)
            .execute(self.tx.pool_for(account_id)?)
            .await
            .map_err(storage)?
            .rows_affected();
        Ok(removed > 0)
    }
}
