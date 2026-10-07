//! PostgreSQL adapter for [`SupervisionStore`] (migration 0009). Invites on
//! the shard of their code; supervisions on the teen's shard, with a reverse
//! index on the supervisor's (see the port for the ordering).

use std::sync::Arc;

use async_trait::async_trait;
use chrono::{DateTime, Months, NaiveDate, Utc};
use postgres_storage::{StorageError, TransactionManager};
use tracing::instrument;

use crate::application::port::{AccountAges, AccountRepository, Linked, SupervisionStore};
use crate::domain::supervision::{InviteCode, Supervision, SupervisionInvite, SupervisionRole};
use crate::domain::value_object::{AccountId, AgeBracket};
use crate::error::AccountError;

fn storage(e: sqlx::Error) -> AccountError {
    AccountError::Storage(StorageError::from(e))
}

#[derive(Clone)]
pub struct PgSupervisionStore {
    tx: TransactionManager,
}

impl PgSupervisionStore {
    pub fn new(tx: TransactionManager) -> Self {
        Self { tx }
    }

    fn pool(&self, account: &AccountId) -> Result<&sqlx::PgPool, AccountError> {
        self.tx.pool_for(account).map_err(AccountError::Storage)
    }

    async fn index(&self, link: &Supervision) -> Result<(), AccountError> {
        sqlx::query(
            "INSERT INTO supervisions_by_supervisor (supervisor_id, teen_id, since) VALUES ($1, $2, $3) \
             ON CONFLICT (supervisor_id, teen_id) DO NOTHING",
        )
        .bind(link.supervisor.as_uuid())
        .bind(link.teen.as_uuid())
        .bind(link.since)
        .execute(self.pool(&link.supervisor)?)
        .await
        .map_err(storage)?;
        Ok(())
    }

    async fn unindex(&self, supervisor: &AccountId, teen: &AccountId) -> Result<(), AccountError> {
        sqlx::query("DELETE FROM supervisions_by_supervisor WHERE supervisor_id = $1 AND teen_id = $2")
            .bind(supervisor.as_uuid())
            .bind(teen.as_uuid())
            .execute(self.pool(supervisor)?)
            .await
            .map_err(storage)?;
        Ok(())
    }
}

type LinkRow = (uuid::Uuid, uuid::Uuid, DateTime<Utc>);

fn link_from((teen, supervisor, since): LinkRow) -> Supervision {
    Supervision { teen: AccountId::from_uuid(teen), supervisor: AccountId::from_uuid(supervisor), since }
}

#[async_trait]
impl SupervisionStore for PgSupervisionStore {
    #[instrument(name = "account.supervision.put_invite", skip(self, invite))]
    async fn put_invite(&self, invite: &SupervisionInvite) -> Result<(), AccountError> {
        sqlx::query(
            "INSERT INTO supervision_invites (code, creator_id, role, created_at, expires_at) VALUES ($1, $2, $3, $4, $5)",
        )
        .bind(invite.code.as_str())
        .bind(invite.creator.as_uuid())
        .bind(invite.role.as_str())
        .bind(invite.created_at)
        .bind(invite.expires_at)
        .execute(self.tx.pool_for(invite.code.as_str()).map_err(AccountError::Storage)?)
        .await
        .map_err(storage)?;
        Ok(())
    }

    #[instrument(name = "account.supervision.find_invite", skip(self, code))]
    async fn find_invite(&self, code: &InviteCode) -> Result<Option<SupervisionInvite>, AccountError> {
        let row: Option<(uuid::Uuid, String, DateTime<Utc>, DateTime<Utc>)> = sqlx::query_as(
            "SELECT creator_id, role, created_at, expires_at FROM supervision_invites WHERE code = $1",
        )
        .bind(code.as_str())
        .fetch_optional(self.tx.pool_for(code.as_str()).map_err(AccountError::Storage)?)
        .await
        .map_err(storage)?;
        Ok(row.and_then(|(creator, role, created_at, expires_at)| {
            Some(SupervisionInvite {
                code: code.clone(),
                creator: AccountId::from_uuid(creator),
                role: SupervisionRole::parse(&role)?,
                created_at,
                expires_at,
            })
        }))
    }

    #[instrument(name = "account.supervision.claim_invite", skip(self, code))]
    async fn claim_invite(&self, code: &InviteCode, acceptor: &AccountId) -> Result<bool, AccountError> {
        // A compare-and-set: the first acceptor wins; theirs again is a retry.
        let claimed: Option<(uuid::Uuid,)> = sqlx::query_as(
            "UPDATE supervision_invites SET claimed_by = $2 \
             WHERE code = $1 AND (claimed_by IS NULL OR claimed_by = $2) RETURNING creator_id",
        )
        .bind(code.as_str())
        .bind(acceptor.as_uuid())
        .fetch_optional(self.tx.pool_for(code.as_str()).map_err(AccountError::Storage)?)
        .await
        .map_err(storage)?;
        Ok(claimed.is_some())
    }

    #[instrument(name = "account.supervision.failed_accepts", skip(self))]
    async fn failed_accepts(&self, account: &AccountId, hour: DateTime<Utc>) -> Result<i64, AccountError> {
        let failures: Option<(i32,)> =
            sqlx::query_as("SELECT failures FROM supervision_accept_failures WHERE account_id = $1 AND hour = $2")
                .bind(account.as_uuid())
                .bind(hour)
                .fetch_optional(self.pool(account)?)
                .await
                .map_err(storage)?;
        Ok(failures.map_or(0, |(n,)| i64::from(n)))
    }

    #[instrument(name = "account.supervision.record_failed_accept", skip(self))]
    async fn record_failed_accept(&self, account: &AccountId, hour: DateTime<Utc>) -> Result<(), AccountError> {
        sqlx::query(
            "INSERT INTO supervision_accept_failures AS f (account_id, hour, failures) VALUES ($1, $2, 1) \
             ON CONFLICT (account_id, hour) DO UPDATE SET failures = f.failures + 1",
        )
        .bind(account.as_uuid())
        .bind(hour)
        .execute(self.pool(account)?)
        .await
        .map_err(storage)?;
        Ok(())
    }

    #[instrument(name = "account.supervision.delete_invite", skip(self, code))]
    async fn delete_invite(&self, code: &InviteCode) -> Result<(), AccountError> {
        sqlx::query("DELETE FROM supervision_invites WHERE code = $1")
            .bind(code.as_str())
            .execute(self.tx.pool_for(code.as_str()).map_err(AccountError::Storage)?)
            .await
            .map_err(storage)?;
        Ok(())
    }

    #[instrument(name = "account.supervision.link", skip(self, link), fields(teen = %link.teen))]
    async fn link(&self, link: &Supervision, max: usize) -> Result<Linked, AccountError> {
        let (teen, supervisor, since) = (link.teen.as_uuid(), link.supervisor.as_uuid(), link.since);
        let linked = self
            .tx
            .run_on_shard(&link.teen, move |tx| {
                Box::pin(async move {
                    // One pairing at a time per teen, so the count holds.
                    sqlx::query("SELECT pg_advisory_xact_lock(hashtextextended($1::text, 670))")
                        .bind(teen)
                        .execute(&mut **tx)
                        .await
                        .map_err(storage)?;
                    let supervisors: Vec<(uuid::Uuid,)> =
                        sqlx::query_as("SELECT supervisor_id FROM supervisions WHERE teen_id = $1")
                            .bind(teen)
                            .fetch_all(&mut **tx)
                            .await
                            .map_err(storage)?;
                    if supervisors.iter().any(|(s,)| *s == supervisor) {
                        return Ok(Linked::Existing);
                    }
                    if supervisors.len() >= max {
                        return Err(AccountError::SupervisorLimitReached);
                    }
                    sqlx::query("INSERT INTO supervisions (teen_id, supervisor_id, since) VALUES ($1, $2, $3)")
                        .bind(teen)
                        .bind(supervisor)
                        .bind(since)
                        .execute(&mut **tx)
                        .await
                        .map_err(storage)?;
                    Ok(Linked::Created)
                })
            })
            .await?;
        // The supervisor's index, also when completing a half-done pairing.
        self.index(link).await?;
        Ok(linked)
    }

    #[instrument(name = "account.supervision.unlink", skip(self), fields(teen = %teen))]
    async fn unlink(&self, teen: &AccountId, supervisor: &AccountId) -> Result<bool, AccountError> {
        let removed = sqlx::query("DELETE FROM supervisions WHERE teen_id = $1 AND supervisor_id = $2")
            .bind(teen.as_uuid())
            .bind(supervisor.as_uuid())
            .execute(self.pool(teen)?)
            .await
            .map_err(storage)?
            .rows_affected();
        self.unindex(supervisor, teen).await?;
        Ok(removed > 0)
    }

    #[instrument(name = "account.supervision.supervisors_of", skip(self), fields(teen = %teen))]
    async fn supervisors_of(&self, teen: &AccountId) -> Result<Vec<Supervision>, AccountError> {
        let rows: Vec<LinkRow> = sqlx::query_as(
            "SELECT teen_id, supervisor_id, since FROM supervisions WHERE teen_id = $1 ORDER BY since",
        )
        .bind(teen.as_uuid())
        .fetch_all(self.pool(teen)?)
        .await
        .map_err(storage)?;
        Ok(rows.into_iter().map(link_from).collect())
    }

    #[instrument(name = "account.supervision.teens_of", skip(self), fields(supervisor = %supervisor))]
    async fn teens_of(&self, supervisor: &AccountId) -> Result<Vec<Supervision>, AccountError> {
        let indexed: Vec<(uuid::Uuid,)> =
            sqlx::query_as("SELECT teen_id FROM supervisions_by_supervisor WHERE supervisor_id = $1 ORDER BY since")
                .bind(supervisor.as_uuid())
                .fetch_all(self.pool(supervisor)?)
                .await
                .map_err(storage)?;
        let mut links = Vec::with_capacity(indexed.len());
        for (teen,) in indexed {
            let teen = AccountId::from_uuid(teen);
            // The teen's shard decides; a stale index entry goes.
            match self.supervisors_of(&teen).await?.into_iter().find(|l| l.supervisor == *supervisor) {
                Some(link) => links.push(link),
                None => self.unindex(supervisor, &teen).await?,
            }
        }
        Ok(links)
    }

    #[instrument(name = "account.supervision.came_of_age", skip(self))]
    async fn came_of_age(&self, today: NaiveDate, limit: i64) -> Result<Vec<Supervision>, AccountError> {
        // Born on or before this day: 18 today.
        let born_by = today.checked_sub_months(Months::new(18 * 12)).unwrap_or(today);
        let mut due = Vec::new();
        for pool in self.tx.all_pools() {
            let rows: Vec<LinkRow> = sqlx::query_as(
                "SELECT s.teen_id, s.supervisor_id, s.since FROM supervisions s \
                 JOIN accounts a ON a.id = s.teen_id \
                 WHERE a.date_of_birth <= $1 ORDER BY s.since LIMIT $2",
            )
            .bind(born_by)
            .bind(limit)
            .fetch_all(pool)
            .await
            .map_err(storage)?;
            due.extend(rows.into_iter().map(link_from));
        }
        due.truncate(limit.max(0) as usize);
        Ok(due)
    }

    #[instrument(name = "account.supervision.purge_invites", skip(self))]
    async fn purge_expired_invites(&self, now: DateTime<Utc>) -> Result<u64, AccountError> {
        let mut purged = 0;
        for pool in self.tx.all_pools() {
            purged += sqlx::query("DELETE FROM supervision_invites WHERE expires_at <= $1")
                .bind(now)
                .execute(pool)
                .await
                .map_err(storage)?
                .rows_affected();
            // Failure counts outlive their hour by a day at most.
            sqlx::query("DELETE FROM supervision_accept_failures WHERE hour < $1")
                .bind(now - chrono::Duration::days(1))
                .execute(pool)
                .await
                .map_err(storage)?;
        }
        Ok(purged)
    }
}

/// [`AccountAges`] from the accounts themselves.
pub struct RepoAccountAges(pub Arc<dyn AccountRepository>);

#[async_trait]
impl AccountAges for RepoAccountAges {
    async fn age_bracket(&self, account: &AccountId, today: NaiveDate) -> Result<Option<AgeBracket>, AccountError> {
        Ok(self.0.find_by_id(account).await?.and_then(|a| a.age_bracket(today)))
    }
}
