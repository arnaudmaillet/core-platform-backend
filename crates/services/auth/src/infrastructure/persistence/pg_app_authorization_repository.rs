use async_trait::async_trait;
use chrono::{DateTime, Utc};
use postgres_storage::{StorageError, TransactionManager};
use tracing::instrument;

use crate::application::port::{AppAuthorization, AppAuthorizationRepository};
use crate::domain::value_object::AccountId;
use crate::error::AuthError;

/// PostgreSQL adapter for [`AppAuthorizationRepository`] (`app_authorizations`,
/// migration 0008), on the account's shard.
#[derive(Clone)]
pub struct PgAppAuthorizationRepository {
    tx: TransactionManager,
}

impl PgAppAuthorizationRepository {
    pub fn new(tx: TransactionManager) -> Self {
        Self { tx }
    }
}

fn storage(e: sqlx::Error) -> AuthError {
    AuthError::Storage(StorageError::from(e))
}

#[derive(sqlx::FromRow)]
struct AuthorizationRow {
    app_id:       String,
    display_name: String,
    icon_url:     String,
    scopes:       Vec<String>,
    granted_at:   DateTime<Utc>,
    last_used_at: Option<DateTime<Utc>>,
}

impl From<AuthorizationRow> for AppAuthorization {
    fn from(row: AuthorizationRow) -> Self {
        Self {
            app_id:       row.app_id,
            display_name: row.display_name,
            icon_url:     row.icon_url,
            scopes:       row.scopes,
            granted_at:   row.granted_at,
            last_used_at: row.last_used_at,
        }
    }
}

const COLUMNS: &str = "app_id, display_name, icon_url, scopes, granted_at, last_used_at";

#[async_trait]
impl AppAuthorizationRepository for PgAppAuthorizationRepository {
    #[instrument(name = "auth.app_authorization.list", skip(self), fields(account.id = %account_id.as_str()))]
    async fn list(&self, account_id: &AccountId) -> Result<Vec<AppAuthorization>, AuthError> {
        let rows = sqlx::query_as::<_, AuthorizationRow>(&format!(
            "SELECT {COLUMNS} FROM app_authorizations WHERE account_id = $1 AND revoked_at IS NULL \
             ORDER BY granted_at DESC, app_id"
        ))
        .bind(account_id.as_uuid())
        .fetch_all(self.tx.pool_for(account_id)?)
        .await
        .map_err(storage)?;
        Ok(rows.into_iter().map(AppAuthorization::from).collect())
    }

    #[instrument(name = "auth.app_authorization.grant", skip(self, authorization), fields(account.id = %account_id.as_str()))]
    async fn grant(&self, account_id: &AccountId, authorization: &AppAuthorization) -> Result<AppAuthorization, AuthError> {
        // The SET expressions read the existing row: the same scopes on an
        // active grant keep its consent (granted_at, last use); anything else
        // is a new consent.
        let row = sqlx::query_as::<_, AuthorizationRow>(&format!(
            "INSERT INTO app_authorizations (account_id, app_id, display_name, icon_url, scopes, granted_at) \
             VALUES ($1, $2, $3, $4, $5, $6) \
             ON CONFLICT (account_id, app_id) DO UPDATE SET \
               display_name = EXCLUDED.display_name, \
               icon_url = EXCLUDED.icon_url, \
               granted_at = CASE WHEN app_authorizations.revoked_at IS NULL \
                                  AND app_authorizations.scopes = EXCLUDED.scopes \
                             THEN app_authorizations.granted_at ELSE EXCLUDED.granted_at END, \
               last_used_at = CASE WHEN app_authorizations.revoked_at IS NULL \
                                    AND app_authorizations.scopes = EXCLUDED.scopes \
                               THEN app_authorizations.last_used_at ELSE NULL END, \
               scopes = EXCLUDED.scopes, \
               revoked_at = NULL \
             RETURNING {COLUMNS}"
        ))
        .bind(account_id.as_uuid())
        .bind(&authorization.app_id)
        .bind(&authorization.display_name)
        .bind(&authorization.icon_url)
        .bind(&authorization.scopes)
        .bind(authorization.granted_at)
        .fetch_one(self.tx.pool_for(account_id)?)
        .await
        .map_err(storage)?;
        Ok(row.into())
    }

    #[instrument(name = "auth.app_authorization.revoke", skip(self), fields(account.id = %account_id.as_str()))]
    async fn revoke(&self, account_id: &AccountId, app_id: &str, at: DateTime<Utc>) -> Result<Option<DateTime<Utc>>, AuthError> {
        let revoked: Option<(DateTime<Utc>,)> = sqlx::query_as(
            "UPDATE app_authorizations SET revoked_at = COALESCE(revoked_at, $3) \
             WHERE account_id = $1 AND app_id = $2 RETURNING revoked_at",
        )
        .bind(account_id.as_uuid())
        .bind(app_id)
        .bind(at)
        .fetch_optional(self.tx.pool_for(account_id)?)
        .await
        .map_err(storage)?;
        Ok(revoked.map(|(at,)| at))
    }

    #[instrument(name = "auth.app_authorization.record_use", skip(self), fields(account.id = %account_id.as_str()))]
    async fn record_use(&self, account_id: &AccountId, app_id: &str, at: DateTime<Utc>) -> Result<bool, AuthError> {
        let used = sqlx::query(
            "UPDATE app_authorizations SET last_used_at = GREATEST(COALESCE(last_used_at, $3), $3) \
             WHERE account_id = $1 AND app_id = $2 AND revoked_at IS NULL",
        )
        .bind(account_id.as_uuid())
        .bind(app_id)
        .bind(at)
        .execute(self.tx.pool_for(account_id)?)
        .await
        .map_err(storage)?
        .rows_affected();
        Ok(used > 0)
    }

    #[instrument(name = "auth.app_authorization.is_active", skip(self), fields(account.id = %account_id.as_str()))]
    async fn is_active(&self, account_id: &AccountId, app_id: &str) -> Result<bool, AuthError> {
        let (active,): (bool,) = sqlx::query_as(
            "SELECT EXISTS (SELECT 1 FROM app_authorizations \
             WHERE account_id = $1 AND app_id = $2 AND revoked_at IS NULL)",
        )
        .bind(account_id.as_uuid())
        .bind(app_id)
        .fetch_one(self.tx.pool_for(account_id)?)
        .await
        .map_err(storage)?;
        Ok(active)
    }
}
