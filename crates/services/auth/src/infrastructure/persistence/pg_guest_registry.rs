use async_trait::async_trait;
use postgres_storage::{StorageError, TransactionManager};
use tracing::instrument;

use chrono::{DateTime, Utc};

use crate::application::port::{GuestRecord, GuestRegistry};
use crate::domain::value_object::AccountId;
use crate::error::AuthError;

/// `guest_principals`, sharded like sessions on the guest id.
pub struct PgGuestRegistry {
    tx: TransactionManager,
}

impl PgGuestRegistry {
    pub fn new(tx: TransactionManager) -> Self {
        Self { tx }
    }
}

fn storage(e: sqlx::Error) -> AuthError {
    AuthError::Storage(StorageError::from(e))
}

#[async_trait]
impl GuestRegistry for PgGuestRegistry {
    #[instrument(name = "auth.guest.record", skip(self, guest), fields(guest.id = %guest.guest_id.as_str()))]
    async fn record(&self, guest: &GuestRecord) -> Result<(), AuthError> {
        let guest_id = guest.guest_id;
        let g = guest.clone();
        self.tx
            .run_on_shard(&guest_id, move |tx| {
                Box::pin(async move {
                    sqlx::query(
                        r#"
                        INSERT INTO guest_principals (
                            guest_id, device_id, attestation_sent, locale, region_hint,
                            current_country, first_seen_at, attest_key_id
                        ) VALUES ($1,$2,$3,$4,$5,$6,$7,$8)
                        ON CONFLICT (guest_id) DO NOTHING
                        "#,
                    )
                    .bind(g.guest_id.as_uuid())
                    .bind(g.device_id)
                    .bind(g.attestation_sent)
                    .bind(g.locale)
                    .bind(g.region_hint)
                    .bind(g.current_country)
                    .bind(g.first_seen_at)
                    .bind(g.attest_key_id)
                    .execute(&mut **tx)
                    .await
                    .map(|_| ())
                    .map_err(storage)
                })
            })
            .await
    }

    #[instrument(name = "auth.guest.mark_upgraded", skip(self), fields(guest.id = %guest_id.as_str()))]
    async fn mark_upgraded(
        &self,
        guest_id: &AccountId,
        account_id: &AccountId,
        at: DateTime<Utc>,
    ) -> Result<(), AuthError> {
        let guest = guest_id.as_uuid();
        let account = account_id.as_uuid();
        self.tx
            .run_on_shard(guest_id, move |tx| {
                Box::pin(async move {
                    sqlx::query(
                        r#"
                        UPDATE guest_principals
                           SET upgraded_to_account_id = $2, upgraded_at = $3
                         WHERE guest_id = $1 AND upgraded_to_account_id IS NULL
                        "#,
                    )
                    .bind(guest)
                    .bind(account)
                    .bind(at)
                    .execute(&mut **tx)
                    .await
                    .map(|_| ())
                    .map_err(storage)
                })
            })
            .await
    }

    #[instrument(name = "auth.guest.purge_stale", skip(self))]
    async fn purge_stale(
        &self,
        seen_before: DateTime<Utc>,
        now: DateTime<Utc>,
        limit: i64,
    ) -> Result<u64, AuthError> {
        let mut purged = 0;
        for pool in self.tx.all_pools() {
            // Ended guest sessions first (their refresh tokens with them), so the
            // guests they belonged to no longer have a live one.
            sqlx::query(
                r#"
                WITH gone AS (
                    DELETE FROM sessions
                     WHERE id IN (SELECT id FROM sessions
                                   WHERE kind = 'guest' AND absolute_expiry < $1
                                   LIMIT $2)
                    RETURNING id
                )
                DELETE FROM refresh_tokens WHERE session_id IN (SELECT id FROM gone)
                "#,
            )
            .bind(seen_before)
            .bind(limit)
            .execute(pool)
            .await
            .map_err(storage)?;
            let deleted = sqlx::query(
                r#"
                DELETE FROM guest_principals
                 WHERE guest_id IN (
                    SELECT g.guest_id FROM guest_principals g
                     WHERE g.upgraded_to_account_id IS NULL
                       AND g.first_seen_at < $1
                       AND NOT EXISTS (
                           SELECT 1 FROM sessions s
                            WHERE s.account_id = g.guest_id AND s.kind = 'guest'
                              AND s.status = 'active' AND s.absolute_expiry > $2)
                     LIMIT $3)
                "#,
            )
            .bind(seen_before)
            .bind(now)
            .bind(limit)
            .execute(pool)
            .await
            .map_err(storage)?;
            purged += deleted.rows_affected();
        }
        Ok(purged)
    }
}
