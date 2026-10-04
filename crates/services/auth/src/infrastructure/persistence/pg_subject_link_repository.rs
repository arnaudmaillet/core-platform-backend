use async_trait::async_trait;
use postgres_storage::{StorageError, TransactionManager};
use tracing::instrument;

use crate::application::port::SubjectLinkRepository;
use crate::domain::aggregate::SubjectLink;
use crate::domain::value_object::{AccountId, IdpSubject};
use crate::error::AuthError;

use super::model::SubjectLinkRow;

/// PostgreSQL adapter for [`SubjectLinkRepository`]. The link is immutable, so
/// `save` is always an insert; uniqueness on `(issuer, subject)` is enforced by
/// the primary key and surfaced as [`AuthError::SubjectAlreadyLinked`].
#[derive(Clone)]
pub struct PgSubjectLinkRepository {
    tx: TransactionManager,
}

impl PgSubjectLinkRepository {
    pub fn new(tx: TransactionManager) -> Self {
        Self { tx }
    }
}

fn storage(e: sqlx::Error) -> AuthError {
    AuthError::Storage(StorageError::from(e))
}

#[async_trait]
impl SubjectLinkRepository for PgSubjectLinkRepository {
    #[instrument(name = "auth.subject_link.find_by_subject", skip(self), fields(subject = %subject))]
    async fn find_by_subject(
        &self,
        subject: &IdpSubject,
    ) -> Result<Option<SubjectLink>, AuthError> {
        let row = sqlx::query_as::<_, SubjectLinkRow>(
            "SELECT * FROM subject_links WHERE issuer = $1 AND subject = $2",
        )
        .bind(subject.issuer())
        .bind(subject.subject())
        .fetch_optional(self.tx.pool())
        .await
        .map_err(storage)?;
        row.map(SubjectLink::try_from).transpose()
    }

    #[instrument(name = "auth.subject_link.find_by_account", skip(self), fields(account.id = %account_id.as_str()))]
    async fn find_by_account(&self, account_id: &AccountId) -> Result<Vec<SubjectLink>, AuthError> {
        let rows = sqlx::query_as::<_, SubjectLinkRow>(
            "SELECT * FROM subject_links WHERE account_id = $1 ORDER BY linked_at",
        )
        .bind(account_id.as_uuid())
        .fetch_all(self.tx.pool())
        .await
        .map_err(storage)?;
        rows.into_iter().map(SubjectLink::try_from).collect()
    }

    #[instrument(name = "auth.subject_link.save", skip(self, link), fields(subject = %link.subject()))]
    async fn save(&self, link: &SubjectLink) -> Result<(), AuthError> {
        let account_id = link.account_id();
        let issuer = link.subject().issuer().to_owned();
        let subject = link.subject().subject().to_owned();
        let account_uuid = account_id.as_uuid();
        let linked_at = link.linked_at();

        let affected = self
            .tx
            .run_on_shard(&account_id, move |tx| {
                Box::pin(async move {
                    sqlx::query(
                        r#"
                        INSERT INTO subject_links (issuer, subject, account_id, linked_at, version)
                        VALUES ($1, $2, $3, $4, 0)
                        ON CONFLICT (issuer, subject) DO NOTHING
                        "#,
                    )
                    .bind(issuer)
                    .bind(subject)
                    .bind(account_uuid)
                    .bind(linked_at)
                    .execute(&mut **tx)
                    .await
                    .map(|r| r.rows_affected())
                    .map_err(storage)
                })
            })
            .await?;

        if affected == 0 {
            return Err(AuthError::SubjectAlreadyLinked {
                iss: link.subject().issuer().to_owned(),
                sub: link.subject().subject().to_owned(),
            });
        }
        Ok(())
    }
}
