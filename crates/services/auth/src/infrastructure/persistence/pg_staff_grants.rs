//! Staff enrollment grants (#880) on the account's shard: only the grant's
//! SHA-256 is stored, and a grant is used up by a single conditional update.

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use postgres_storage::{StorageError, TransactionManager};
use tracing::instrument;
use uuid::Uuid;

use crate::application::port::{StaffEnrollmentGrants, StaffGrant};
use crate::domain::value_object::AccountId;
use crate::error::AuthError;

pub struct PgStaffGrants {
    tx: TransactionManager,
}

impl PgStaffGrants {
    pub fn new(tx: TransactionManager) -> Self {
        Self { tx }
    }
}

fn storage(e: sqlx::Error) -> AuthError {
    AuthError::Storage(StorageError::from(e))
}

#[async_trait]
impl StaffEnrollmentGrants for PgStaffGrants {
    #[instrument(name = "auth.staff_grant.issue", skip(self, grant, grant_hash), fields(account.id = %grant.account_id.as_str()))]
    async fn issue(&self, grant: &StaffGrant, grant_hash: &str) -> Result<(), AuthError> {
        sqlx::query(
            "INSERT INTO staff_enrollment_grants (grant_hash, grant_id, account_id, issued_by, issued_at, expires_at) \
             VALUES ($1, $2, $3, $4, $5, $6)",
        )
        .bind(grant_hash)
        .bind(grant.grant_id)
        .bind(grant.account_id.as_uuid())
        .bind(grant.issued_by.map(|a| a.as_uuid()))
        .bind(grant.issued_at)
        .bind(grant.expires_at)
        .execute(self.tx.pool_for(&grant.account_id)?)
        .await
        .map_err(storage)?;
        Ok(())
    }

    #[instrument(name = "auth.staff_grant.redeem", skip(self, grant_hash), fields(account.id = %account_id.as_str()))]
    async fn redeem(&self, account_id: &AccountId, grant_hash: &str, now: DateTime<Utc>) -> Result<Option<Uuid>, AuthError> {
        sqlx::query_scalar(
            "UPDATE staff_enrollment_grants SET used_at = $3 \
             WHERE grant_hash = $1 AND account_id = $2 AND used_at IS NULL AND expires_at > $3 \
             RETURNING grant_id",
        )
        .bind(grant_hash)
        .bind(account_id.as_uuid())
        .bind(now)
        .fetch_optional(self.tx.pool_for(account_id)?)
        .await
        .map_err(storage)
    }
}
