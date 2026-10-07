use async_trait::async_trait;
use postgres_storage::{StorageError, TransactionManager};
use tracing::instrument;

use crate::application::port::{AccountEraser, ErasedAccount};
use crate::domain::value_object::AccountId;
use crate::error::AuthError;

/// Deletes an account's auth data: its own rows on its shard (sessions, refresh
/// tokens, passkeys, app authorisations, identity links; one transaction), then, on every shard,
/// the guests that became it with their sessions (a guest is sharded on its
/// own id).
pub struct PgAccountEraser {
    tx: TransactionManager,
}

impl PgAccountEraser {
    pub fn new(tx: TransactionManager) -> Self {
        Self { tx }
    }
}

fn storage(e: sqlx::Error) -> AuthError {
    AuthError::Storage(StorageError::from(e))
}

#[async_trait]
impl AccountEraser for PgAccountEraser {
    #[instrument(name = "auth.account.erase", skip(self), fields(account.id = %account_id.as_str()))]
    async fn erase(&self, account_id: &AccountId) -> Result<ErasedAccount, AuthError> {
        let account = account_id.as_uuid();
        let (sessions, links) = self
            .tx
            .run_on_shard(account_id, move |tx| {
                Box::pin(async move {
                    sqlx::query("DELETE FROM refresh_tokens WHERE account_id = $1")
                        .bind(account)
                        .execute(&mut **tx)
                        .await
                        .map_err(storage)?;
                    let sessions = sqlx::query("DELETE FROM sessions WHERE account_id = $1")
                        .bind(account)
                        .execute(&mut **tx)
                        .await
                        .map_err(storage)?
                        .rows_affected();
                    sqlx::query("DELETE FROM passkeys WHERE account_id = $1")
                        .bind(account)
                        .execute(&mut **tx)
                        .await
                        .map_err(storage)?;
                    // Third-party app authorisations (#667); the consent records stay
                    // on the audit plane, crypto-shredded with the account.
                    sqlx::query("DELETE FROM app_authorizations WHERE account_id = $1")
                        .bind(account)
                        .execute(&mut **tx)
                        .await
                        .map_err(storage)?;
                    let links = sqlx::query("DELETE FROM subject_links WHERE account_id = $1")
                        .bind(account)
                        .execute(&mut **tx)
                        .await
                        .map_err(storage)?
                        .rows_affected();
                    Ok::<_, AuthError>((sessions, links))
                })
            })
            .await?;

        let mut erased = ErasedAccount { sessions, links, guests: 0 };
        for pool in self.tx.all_pools() {
            let mut tx = pool.begin().await.map_err(storage)?;
            let guests: Vec<uuid::Uuid> = sqlx::query_scalar(
                "DELETE FROM guest_principals WHERE upgraded_to_account_id = $1 RETURNING guest_id",
            )
            .bind(account)
            .fetch_all(&mut *tx)
            .await
            .map_err(storage)?;
            if !guests.is_empty() {
                sqlx::query("DELETE FROM refresh_tokens WHERE account_id = ANY($1)")
                    .bind(&guests)
                    .execute(&mut *tx)
                    .await
                    .map_err(storage)?;
                erased.sessions += sqlx::query("DELETE FROM sessions WHERE account_id = ANY($1)")
                    .bind(&guests)
                    .execute(&mut *tx)
                    .await
                    .map_err(storage)?
                    .rows_affected();
            }
            tx.commit().await.map_err(storage)?;
            erased.guests += guests.len() as u64;
        }
        Ok(erased)
    }
}
