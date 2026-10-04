use async_trait::async_trait;
use postgres_storage::{StorageError, TransactionManager};
use tracing::instrument;

use crate::application::port::{GuestRecord, GuestRegistry};
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
                            current_country, first_seen_at
                        ) VALUES ($1,$2,$3,$4,$5,$6,$7)
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
                    .execute(&mut **tx)
                    .await
                    .map(|_| ())
                    .map_err(storage)
                })
            })
            .await
    }
}
