//! [`LikeLedger`] on Scylla (migration 0005): `likes_by_target` and
//! `likes_by_account`, one LOGGED batch, the stake's time as the write
//! timestamp so the newer total wins. An account's erasure (migration 0007)
//! deletes as of its own time, so a stake made before it never comes back.

use std::sync::Arc;

use async_trait::async_trait;
use scylla::observability::history::HistoryListener;
use scylla::statement::batch::{Batch, BatchType};
use scylla::statement::unprepared::Statement;
use scylla::value::CqlTimestamp;
use scylla::DeserializeRow;
use scylla_storage::{ProfileKind as ScyllaProfileKind, ScyllaClient, ScyllaStorageError};
use uuid::Uuid;

use crate::application::port::{AccountLike, ForgottenLike, LikeLedger, Position};
use crate::domain::value_object::LikeTarget;
use crate::error::EngagementError;

pub struct ScyllaLikeLedger {
    client: Arc<ScyllaClient>,
}

/// How long a deleted account is remembered (the table's default TTL): far
/// beyond the 24 hours a stake batch is accepted and the wallet outbox's lag.
pub const ERASED_FOR_SECS: i32 = 30 * 24 * 3600;

fn account_uuid(account: &str) -> Result<Uuid, EngagementError> {
    Uuid::parse_str(account)
        .map_err(|_| EngagementError::DomainViolation { field: "account_id".into(), message: account.to_owned() })
}

fn scylla(e: impl Into<ScyllaStorageError>) -> EngagementError {
    EngagementError::Scylla(e.into())
}

impl ScyllaLikeLedger {
    pub fn new(client: Arc<ScyllaClient>) -> Self {
        Self { client }
    }

    fn statement(&self, cql: &str, at_micros: Option<i64>) -> Statement {
        let mut stmt = Statement::new(cql);
        stmt.set_execution_profile_handle(Some(
            self.client.profiles.get(ScyllaProfileKind::Strict).clone().into_handle_with_label("strict".to_string()),
        ));
        stmt.set_history_listener(Arc::clone(&self.client.history_listener) as Arc<dyn HistoryListener>);
        stmt.set_timestamp(at_micros);
        stmt
    }
}

#[async_trait]
impl LikeLedger for ScyllaLikeLedger {
    async fn record(
        &self,
        target: &LikeTarget,
        account: &str,
        profile_id: &str,
        position: Position,
        at_micros: i64,
    ) -> Result<(), EngagementError> {
        let account = Uuid::parse_str(account)
            .map_err(|_| EngagementError::DomainViolation { field: "account_id".into(), message: account.to_owned() })?;
        let total = position.total;
        // Both tables or neither (a logged batch), the stake's time as the
        // write time: the newer total wins whatever the order.
        let mut batch = Batch::new(BatchType::Logged);
        batch.set_execution_profile_handle(Some(
            self.client.profiles.get(ScyllaProfileKind::Strict).clone().into_handle_with_label("strict-batch".to_string()),
        ));
        batch.set_history_listener(Arc::clone(&self.client.history_listener) as Arc<dyn HistoryListener>);
        batch.set_timestamp(Some(at_micros));
        // The arrival only when known: a null would delete one written before.
        batch.append_statement(match position.arrival {
            Some(_) => {
                "INSERT INTO engagement.likes_by_target (target_kind, target_id, account_id, total, first_count) \
                 VALUES (?, ?, ?, ?, ?)"
            }
            None => "INSERT INTO engagement.likes_by_target (target_kind, target_id, account_id, total) VALUES (?, ?, ?, ?)",
        });
        batch.append_statement(
            "INSERT INTO engagement.likes_by_account (account_id, target_kind, target_id, total, profile_id, liked_at) \
             VALUES (?, ?, ?, ?, ?, ?)",
        );
        let liked_at = CqlTimestamp(at_micros / 1_000);
        let by_account = (account, target.kind(), target.id(), total, profile_id, liked_at);
        let result = match position.arrival {
            Some(arrival) => {
                self.client.session.batch(&batch, ((target.kind(), target.id(), account, total, arrival), by_account)).await
            }
            None => self.client.session.batch(&batch, ((target.kind(), target.id(), account, total), by_account)).await,
        };
        result.map_err(|e| EngagementError::Scylla(ScyllaStorageError::from(e)))?;
        // The profile's Likes tab (#829): which of the account's profiles
        // liked it, and — for a post — the tab's own row. Same write time, so
        // the account's erasure (stamped later) removes both for good; a
        // retry rewrites them.
        if !profile_id.is_empty() {
            let add = self.statement(
                "UPDATE engagement.likes_by_account SET profile_ids = profile_ids + ? \
                 WHERE account_id = ? AND target_kind = ? AND target_id = ?",
                Some(at_micros),
            );
            let profiles: Vec<&str> = vec![profile_id];
            self.client.session.execute_unpaged(add, (profiles, account, target.kind(), target.id())).await.map_err(scylla)?;
            if let LikeTarget::Post(post_id) = target {
                let tab = self.statement(
                    "INSERT INTO engagement.liked_posts_by_profile (profile_id, post_id, total, liked_at) VALUES (?, ?, ?, ?)",
                    Some(at_micros),
                );
                let liked_at = CqlTimestamp(at_micros / 1_000);
                self.client.session.execute_unpaged(tab, (profile_id, post_id.as_str(), total, liked_at)).await.map_err(scylla)?;
            }
        }
        Ok(())
    }

    async fn list_by_account(
        &self,
        account: &str,
        limit: i32,
        after: Option<&LikeTarget>,
    ) -> Result<Vec<AccountLike>, EngagementError> {
        #[derive(DeserializeRow)]
        struct Row {
            target_kind: String,
            target_id:   String,
            total:       Option<i64>,
            profile_id:  Option<String>,
            liked_at:    Option<CqlTimestamp>,
            profile_ids: Option<Vec<String>>,
        }
        let account = Uuid::parse_str(account)
            .map_err(|_| EngagementError::DomainViolation { field: "account_id".into(), message: account.to_owned() })?;
        let mut stmt = Statement::new(match after {
            Some(_) => {
                "SELECT target_kind, target_id, total, profile_id, liked_at, profile_ids FROM engagement.likes_by_account \
                 WHERE account_id = ? AND (target_kind, target_id) > (?, ?) LIMIT ?"
            }
            None => {
                "SELECT target_kind, target_id, total, profile_id, liked_at, profile_ids FROM engagement.likes_by_account \
                 WHERE account_id = ? LIMIT ?"
            }
        });
        stmt.set_execution_profile_handle(Some(
            self.client.profiles.get(ScyllaProfileKind::Strict).clone().into_handle_with_label("strict".to_string()),
        ));
        stmt.set_history_listener(Arc::clone(&self.client.history_listener) as Arc<dyn HistoryListener>);
        let result = match after {
            Some(t) => self.client.session.execute_unpaged(stmt, (account, t.kind(), t.id(), limit)).await,
            None => self.client.session.execute_unpaged(stmt, (account, limit)).await,
        }
        .map_err(|e| EngagementError::Scylla(ScyllaStorageError::from(e)))?;
        let rows = result
            .into_rows_result()
            .map_err(|e| EngagementError::DomainViolation { field: "likes_by_account".into(), message: e.to_string() })?;
        let mut likes = Vec::new();
        for row in rows
            .rows::<Row>()
            .map_err(|e| EngagementError::DomainViolation { field: "likes_by_account".into(), message: e.to_string() })?
        {
            let row = row.map_err(|e| EngagementError::DomainViolation { field: "likes_by_account".into(), message: e.to_string() })?;
            // A row the service cannot read is skipped, never guessed.
            let Ok(target) = LikeTarget::parse(&row.target_kind, &row.target_id) else { continue };
            likes.push(AccountLike {
                target,
                total:      row.total.unwrap_or(0),
                profile_id: row.profile_id.unwrap_or_default(),
                liked_at:   row
                    .liked_at
                    .and_then(|t| chrono::DateTime::from_timestamp_millis(t.0))
                    .unwrap_or_default(),
                profile_ids: row.profile_ids.unwrap_or_default(),
            });
        }
        Ok(likes)
    }

    async fn erased_among(&self, accounts: &[String]) -> Result<Vec<String>, EngagementError> {
        // Anonymous likers and malformed ids are not accounts: never erased.
        let ids: Vec<Uuid> = accounts.iter().filter_map(|a| Uuid::parse_str(a).ok()).collect();
        let mut erased = Vec::new();
        for chunk in ids.chunks(100) {
            let stmt = self.statement("SELECT account_id FROM engagement.erased_accounts WHERE account_id IN ?", None);
            let rows = self
                .client
                .session
                .execute_unpaged(stmt, (chunk.to_vec(),))
                .await
                .map_err(scylla)?
                .into_rows_result()
                .map_err(|e| EngagementError::DomainViolation { field: "erased_accounts".into(), message: e.to_string() })?;
            for row in rows
                .rows::<(Uuid,)>()
                .map_err(|e| EngagementError::DomainViolation { field: "erased_accounts".into(), message: e.to_string() })?
            {
                let (account,) =
                    row.map_err(|e| EngagementError::DomainViolation { field: "erased_accounts".into(), message: e.to_string() })?;
                erased.push(account.to_string());
            }
        }
        Ok(erased)
    }

    async fn liked_posts_by_profile(
        &self,
        profile_id: &str,
        limit: i32,
        after: Option<&str>,
    ) -> Result<Vec<String>, EngagementError> {
        let result = match after {
            Some(after) => {
                let stmt = self.statement(
                    "SELECT post_id FROM engagement.liked_posts_by_profile WHERE profile_id = ? AND post_id < ? LIMIT ?",
                    None,
                );
                self.client.session.execute_unpaged(stmt, (profile_id, after, limit)).await
            }
            None => {
                let stmt =
                    self.statement("SELECT post_id FROM engagement.liked_posts_by_profile WHERE profile_id = ? LIMIT ?", None);
                self.client.session.execute_unpaged(stmt, (profile_id, limit)).await
            }
        }
        .map_err(scylla)?;
        let rows = result
            .into_rows_result()
            .map_err(|e| EngagementError::DomainViolation { field: "liked_posts_by_profile".into(), message: e.to_string() })?;
        rows.rows::<(String,)>()
            .map_err(|e| EngagementError::DomainViolation { field: "liked_posts_by_profile".into(), message: e.to_string() })?
            .map(|row| {
                row.map(|(post,)| post)
                    .map_err(|e| EngagementError::DomainViolation { field: "liked_posts_by_profile".into(), message: e.to_string() })
            })
            .collect()
    }

    async fn position_of(&self, target: &LikeTarget, account: &str) -> Result<Option<Position>, EngagementError> {
        let account = account_uuid(account)?;
        let stmt = self.statement(
            "SELECT total, first_count FROM engagement.likes_by_target WHERE target_kind = ? AND target_id = ? AND account_id = ?",
            None,
        );
        let rows = self
            .client
            .session
            .execute_unpaged(stmt, (target.kind(), target.id(), account))
            .await
            .map_err(scylla)?
            .into_rows_result()
            .map_err(|e| EngagementError::DomainViolation { field: "likes_by_target".into(), message: e.to_string() })?;
        let row = rows
            .maybe_first_row::<(Option<i64>, Option<i64>)>()
            .map_err(|e| EngagementError::DomainViolation { field: "likes_by_target".into(), message: e.to_string() })?;
        Ok(row.map(|(total, arrival)| Position { total: total.unwrap_or(0), arrival }))
    }

    async fn likers_of(
        &self,
        target: &LikeTarget,
        limit: i32,
        after: Option<&str>,
    ) -> Result<Vec<(String, Position)>, EngagementError> {
        let result = match after {
            Some(after) => {
                let stmt = self.statement(
                    "SELECT account_id, total, first_count FROM engagement.likes_by_target \
                     WHERE target_kind = ? AND target_id = ? AND account_id > ? LIMIT ?",
                    None,
                );
                self.client.session.execute_unpaged(stmt, (target.kind(), target.id(), account_uuid(after)?, limit)).await
            }
            None => {
                let stmt = self.statement(
                    "SELECT account_id, total, first_count FROM engagement.likes_by_target \
                     WHERE target_kind = ? AND target_id = ? LIMIT ?",
                    None,
                );
                self.client.session.execute_unpaged(stmt, (target.kind(), target.id(), limit)).await
            }
        }
        .map_err(scylla)?;
        let rows = result
            .into_rows_result()
            .map_err(|e| EngagementError::DomainViolation { field: "likes_by_target".into(), message: e.to_string() })?;
        let mut likers = Vec::new();
        for row in rows
            .rows::<(Uuid, Option<i64>, Option<i64>)>()
            .map_err(|e| EngagementError::DomainViolation { field: "likes_by_target".into(), message: e.to_string() })?
        {
            let (account, total, arrival) =
                row.map_err(|e| EngagementError::DomainViolation { field: "likes_by_target".into(), message: e.to_string() })?;
            likers.push((account.to_string(), Position { total: total.unwrap_or(0), arrival }));
        }
        Ok(likers)
    }

    async fn mark_erased(&self, account: &str, erased_at_micros: i64) -> Result<(), EngagementError> {
        let account = account_uuid(account)?;
        let stmt = self.statement(
            "INSERT INTO engagement.erased_accounts (account_id, erased_at_us) VALUES (?, ?) USING TTL ?",
            None,
        );
        self.client.session.execute_unpaged(stmt, (account, erased_at_micros, ERASED_FOR_SECS)).await.map_err(scylla)?;
        Ok(())
    }

    async fn erased_at(&self, account: &str) -> Result<Option<i64>, EngagementError> {
        let account = account_uuid(account)?;
        let stmt = self.statement("SELECT erased_at_us FROM engagement.erased_accounts WHERE account_id = ?", None);
        let rows = self
            .client
            .session
            .execute_unpaged(stmt, (account,))
            .await
            .map_err(scylla)?
            .into_rows_result()
            .map_err(|e| EngagementError::DomainViolation { field: "erased_accounts".into(), message: e.to_string() })?;
        let row = rows
            .maybe_first_row::<(Option<i64>,)>()
            .map_err(|e| EngagementError::DomainViolation { field: "erased_accounts".into(), message: e.to_string() })?;
        Ok(row.map(|(at,)| at.unwrap_or(0)))
    }

    async fn forget(&self, account: &str, likes: &[ForgottenLike], at_micros: i64) -> Result<(), EngagementError> {
        let account = account_uuid(account)?;
        // The profiles' Likes-tab rows first: the account's own row, which
        // names those profiles, goes in the batch below.
        let tabs = likes.iter().flat_map(|like| {
            let post = match &like.target {
                LikeTarget::Post(id) => Some(id.as_str()),
                LikeTarget::Comment(_) => None,
            };
            post.into_iter().flat_map(move |post| like.profile_ids.iter().map(move |profile| (profile.as_str(), post)))
        });
        let deletes = tabs.map(|(profile, post)| {
            let stmt = self.statement(
                "DELETE FROM engagement.liked_posts_by_profile WHERE profile_id = ? AND post_id = ?",
                Some(at_micros),
            );
            let session = &self.client.session;
            async move { session.execute_unpaged(stmt, (profile, post)).await }
        });
        futures::future::try_join_all(deletes).await.map_err(scylla)?;
        let forgets = likes.iter().map(|like| {
            let (t, total) = (&like.target, like.total);
            // One target's swap is atomic, and its anonymous id is the
            // erasure's: a re-run or a replayed batch rewrites the same row.
            let mut batch = Batch::new(BatchType::Logged);
            batch.set_execution_profile_handle(Some(
                self.client.profiles.get(ScyllaProfileKind::Strict).clone().into_handle_with_label("strict-batch".to_string()),
            ));
            batch.set_history_listener(Arc::clone(&self.client.history_listener) as Arc<dyn HistoryListener>);
            batch.set_timestamp(Some(at_micros));
            batch.append_statement(
                "DELETE FROM engagement.likes_by_target WHERE target_kind = ? AND target_id = ? AND account_id = ?",
            );
            // Who it was is not kept: the anonymous id, the points only.
            batch.append_statement(
                "INSERT INTO engagement.likes_by_target (target_kind, target_id, account_id, total) VALUES (?, ?, ?, ?)",
            );
            batch.append_statement(
                "DELETE FROM engagement.likes_by_account WHERE account_id = ? AND target_kind = ? AND target_id = ?",
            );
            let values = (
                (t.kind(), t.id(), account),
                (t.kind(), t.id(), like.anonymous_id, total),
                (account, t.kind(), t.id()),
            );
            let session = &self.client.session;
            async move { session.batch(&batch, values).await }
        });
        futures::future::try_join_all(forgets).await.map_err(scylla)?;
        Ok(())
    }

    async fn forget_account(&self, account: &str, at_micros: i64) -> Result<(), EngagementError> {
        let account = account_uuid(account)?;
        let stmt = self.statement("DELETE FROM engagement.likes_by_account WHERE account_id = ?", Some(at_micros));
        self.client.session.execute_unpaged(stmt, (account,)).await.map_err(scylla)?;
        Ok(())
    }
}
