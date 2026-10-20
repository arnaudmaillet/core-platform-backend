//! [`ViewHistory`] on Scylla (migration 0011): `view_history_by_profile`
//! (the History tab, most recent views first) and `views_by_account` (the
//! authority: each post's last view). A view rewrites both in one logged
//! batch with its time as the write timestamp, dropping the post's previous
//! history row; deletions are stamped later. A history row the authority
//! doesn't confirm (two re-views racing) is never listed. Both tables
//! expire rows 90 days after their write.

use std::collections::HashMap;
use std::sync::Arc;

use async_trait::async_trait;
use scylla::observability::history::HistoryListener;
use scylla::statement::batch::{Batch, BatchType};
use scylla::statement::unprepared::Statement;
use scylla::value::CqlTimestamp;
use scylla::DeserializeRow;
use scylla_storage::{ProfileKind as ScyllaProfileKind, ScyllaClient, ScyllaStorageError};
use uuid::Uuid;

use crate::application::port::{ViewCursor, ViewHistory, ViewedPost, REVIEW_DEBOUNCE_MICROS};
use crate::error::EngagementError;

pub struct ScyllaViewHistory {
    client: Arc<ScyllaClient>,
}

fn scylla(e: impl Into<ScyllaStorageError>) -> EngagementError {
    EngagementError::Scylla(e.into())
}

fn rows_err(e: impl std::fmt::Display) -> EngagementError {
    EngagementError::DomainViolation { field: "view_history".into(), message: e.to_string() }
}

fn account_uuid(account: &str) -> Result<Uuid, EngagementError> {
    Uuid::parse_str(account)
        .map_err(|_| EngagementError::DomainViolation { field: "account_id".into(), message: account.to_owned() })
}

impl ScyllaViewHistory {
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

    fn batch(&self, at_micros: i64) -> Batch {
        let mut batch = Batch::new(BatchType::Logged);
        batch.set_execution_profile_handle(Some(
            self.client.profiles.get(ScyllaProfileKind::Strict).clone().into_handle_with_label("strict-batch".to_string()),
        ));
        batch.set_history_listener(Arc::clone(&self.client.history_listener) as Arc<dyn HistoryListener>);
        batch.set_timestamp(Some(at_micros));
        batch
    }

    /// When `profile_id` last viewed `post_id`, within the window.
    async fn viewed_at(&self, account: Uuid, profile_id: &str, post_id: &str) -> Result<Option<CqlTimestamp>, EngagementError> {
        let stmt = self.statement(
            "SELECT viewed_at FROM engagement.views_by_account WHERE account_id = ? AND profile_id = ? AND post_id = ?",
            None,
        );
        let rows = self
            .client
            .session
            .execute_unpaged(stmt, (account, profile_id, post_id))
            .await
            .map_err(scylla)?
            .into_rows_result()
            .map_err(rows_err)?;
        Ok(rows.maybe_first_row::<(Option<CqlTimestamp>,)>().map_err(rows_err)?.and_then(|(at,)| at))
    }

    /// The account a profile's history names, if it has one.
    async fn account_of(&self, profile_id: &str) -> Result<Option<Uuid>, EngagementError> {
        let stmt = self.statement("SELECT account_id FROM engagement.view_history_by_profile WHERE profile_id = ? LIMIT 1", None);
        Ok(self
            .client
            .session
            .execute_unpaged(stmt, (profile_id,))
            .await
            .map_err(scylla)?
            .into_rows_result()
            .map_err(rows_err)?
            .maybe_first_row::<(Option<Uuid>,)>()
            .map_err(rows_err)?
            .and_then(|(account,)| account))
    }
}

#[async_trait]
impl ViewHistory for ScyllaViewHistory {
    async fn record(&self, account: &str, profile_id: &str, post_id: &str, at_micros: i64) -> Result<(), EngagementError> {
        let account = account_uuid(account)?;
        let previous = self.viewed_at(account, profile_id, post_id).await?;
        if previous.is_some_and(|at| at_micros / 1_000 - at.0 < REVIEW_DEBOUNCE_MICROS / 1_000) {
            return Ok(());
        }
        let viewed_at = CqlTimestamp(at_micros / 1_000);
        let mut batch = self.batch(at_micros);
        batch.append_statement(
            "INSERT INTO engagement.views_by_account (account_id, profile_id, post_id, viewed_at) VALUES (?, ?, ?, ?)",
        );
        batch.append_statement(
            "INSERT INTO engagement.view_history_by_profile (profile_id, viewed_at, post_id, account_id) VALUES (?, ?, ?, ?)",
        );
        batch.append_statement("UPDATE engagement.views_by_account SET profile_ids = profile_ids + ? WHERE account_id = ?");
        let insert = (
            (account, profile_id, post_id, viewed_at),
            (profile_id, viewed_at, post_id, account),
            (vec![profile_id], account),
        );
        match previous {
            // A re-view: the post leaves its old place in the history.
            Some(before) => {
                batch.append_statement(
                    "DELETE FROM engagement.view_history_by_profile WHERE profile_id = ? AND viewed_at = ? AND post_id = ?",
                );
                let values = (insert.0, insert.1, insert.2, (profile_id, before, post_id));
                self.client.session.batch(&batch, values).await.map_err(scylla)?;
            }
            None => {
                self.client.session.batch(&batch, insert).await.map_err(scylla)?;
            }
        }
        Ok(())
    }

    async fn list(
        &self,
        profile_id: &str,
        limit: i32,
        after: Option<&ViewCursor>,
    ) -> Result<(Vec<ViewedPost>, Option<ViewCursor>), EngagementError> {
        #[derive(DeserializeRow)]
        struct HistoryRow {
            viewed_at:  Option<CqlTimestamp>,
            post_id:    Option<String>,
            account_id: Option<Uuid>,
        }
        let result = match after {
            Some(after) => {
                let stmt = self.statement(
                    "SELECT viewed_at, post_id, account_id FROM engagement.view_history_by_profile \
                     WHERE profile_id = ? AND (viewed_at, post_id) < (?, ?) LIMIT ?",
                    None,
                );
                let at = CqlTimestamp(after.viewed_at_ms);
                self.client.session.execute_unpaged(stmt, (profile_id, at, after.post_id.as_str(), limit)).await
            }
            None => {
                let stmt = self.statement(
                    "SELECT viewed_at, post_id, account_id FROM engagement.view_history_by_profile \
                     WHERE profile_id = ? LIMIT ?",
                    None,
                );
                self.client.session.execute_unpaged(stmt, (profile_id, limit)).await
            }
        }
        .map_err(scylla)?;
        let rows = result.into_rows_result().map_err(rows_err)?;
        let mut page = Vec::new();
        let mut account = None;
        for row in rows.rows::<HistoryRow>().map_err(rows_err)? {
            let row = row.map_err(rows_err)?;
            account = account.or(row.account_id);
            // A partition left with its static column only has no view.
            if let (Some(at), Some(post)) = (row.viewed_at, row.post_id) {
                page.push((at.0, post));
            }
        }
        let next = (page.len() == limit as usize)
            .then(|| page.last().map(|(at, post)| ViewCursor { viewed_at_ms: *at, post_id: post.clone() }))
            .flatten();
        let Some(account) = account.filter(|_| !page.is_empty()) else { return Ok((Vec::new(), next)) };
        // Only the views the authority confirms, at the same time.
        let mut posts: Vec<&str> = page.iter().map(|(_, post)| post.as_str()).collect();
        posts.sort_unstable();
        posts.dedup();
        let stmt = self.statement(
            "SELECT post_id, viewed_at FROM engagement.views_by_account \
             WHERE account_id = ? AND profile_id = ? AND post_id IN ?",
            None,
        );
        let confirmed: HashMap<String, i64> = self
            .client
            .session
            .execute_unpaged(stmt, (account, profile_id, posts))
            .await
            .map_err(scylla)?
            .into_rows_result()
            .map_err(rows_err)?
            .rows::<(String, CqlTimestamp)>()
            .map_err(rows_err)?
            .map(|row| row.map(|(post, at)| (post, at.0)).map_err(rows_err))
            .collect::<Result<_, _>>()?;
        let views = page
            .into_iter()
            .filter(|(at, post)| confirmed.get(post) == Some(at))
            .map(|(viewed_at_ms, post_id)| ViewedPost { profile_id: profile_id.to_owned(), post_id, viewed_at_ms })
            .collect();
        Ok((views, next))
    }

    async fn remove(&self, account: &str, profile_id: &str, post_id: &str, at_micros: i64) -> Result<(), EngagementError> {
        let account = account_uuid(account)?;
        let Some(viewed_at) = self.viewed_at(account, profile_id, post_id).await? else { return Ok(()) };
        let mut batch = self.batch(at_micros);
        batch.append_statement("DELETE FROM engagement.views_by_account WHERE account_id = ? AND profile_id = ? AND post_id = ?");
        batch.append_statement(
            "DELETE FROM engagement.view_history_by_profile WHERE profile_id = ? AND viewed_at = ? AND post_id = ?",
        );
        let values = ((account, profile_id, post_id), (profile_id, viewed_at, post_id));
        self.client.session.batch(&batch, values).await.map_err(scylla)?;
        Ok(())
    }

    async fn clear(&self, account: &str, profile_id: &str, at_micros: i64) -> Result<(), EngagementError> {
        let account = account_uuid(account)?;
        let mut batch = self.batch(at_micros);
        batch.append_statement("DELETE FROM engagement.view_history_by_profile WHERE profile_id = ?");
        batch.append_statement("DELETE FROM engagement.views_by_account WHERE account_id = ? AND profile_id = ?");
        self.client.session.batch(&batch, ((profile_id,), (account, profile_id))).await.map_err(scylla)?;
        Ok(())
    }

    async fn list_by_account(
        &self,
        account: &str,
        limit: i32,
        after: Option<(&str, &str)>,
    ) -> Result<Vec<ViewedPost>, EngagementError> {
        let account = account_uuid(account)?;
        let result = match after {
            Some((profile, post)) => {
                let stmt = self.statement(
                    "SELECT profile_id, post_id, viewed_at FROM engagement.views_by_account \
                     WHERE account_id = ? AND (profile_id, post_id) > (?, ?) LIMIT ?",
                    None,
                );
                self.client.session.execute_unpaged(stmt, (account, profile, post, limit)).await
            }
            None => {
                let stmt = self.statement(
                    "SELECT profile_id, post_id, viewed_at FROM engagement.views_by_account WHERE account_id = ? LIMIT ?",
                    None,
                );
                self.client.session.execute_unpaged(stmt, (account, limit)).await
            }
        }
        .map_err(scylla)?;
        let rows = result.into_rows_result().map_err(rows_err)?;
        let mut views = Vec::new();
        for row in rows.rows::<(Option<String>, Option<String>, Option<CqlTimestamp>)>().map_err(rows_err)? {
            // A partition left with its static column only (a cleared
            // history) has no view.
            if let (Some(profile_id), Some(post_id), Some(at)) = row.map_err(rows_err)? {
                views.push(ViewedPost { profile_id, post_id, viewed_at_ms: at.0 });
            }
        }
        Ok(views)
    }

    async fn forget_profile(&self, profile_id: &str, at_micros: i64) -> Result<(), EngagementError> {
        // Nothing viewed, nothing to forget.
        let Some(account) = self.account_of(profile_id).await? else { return Ok(()) };
        self.clear(&account.to_string(), profile_id, at_micros).await
    }

    async fn forget_account(&self, account: &str, at_micros: i64) -> Result<(), EngagementError> {
        let account = account_uuid(account)?;
        // Every profile that viewed within the window: each one's whole
        // history goes (rows the authority no longer confirms too), then the
        // authority.
        let stmt = self.statement("SELECT profile_ids FROM engagement.views_by_account WHERE account_id = ? LIMIT 1", None);
        let profiles = self
            .client
            .session
            .execute_unpaged(stmt, (account,))
            .await
            .map_err(scylla)?
            .into_rows_result()
            .map_err(rows_err)?
            .maybe_first_row::<(Option<Vec<String>>,)>()
            .map_err(rows_err)?
            .and_then(|(profiles,)| profiles)
            .unwrap_or_default();
        for profile in &profiles {
            let stmt = self.statement("DELETE FROM engagement.view_history_by_profile WHERE profile_id = ?", Some(at_micros));
            self.client.session.execute_unpaged(stmt, (profile.as_str(),)).await.map_err(scylla)?;
        }
        let stmt = self.statement("DELETE FROM engagement.views_by_account WHERE account_id = ?", Some(at_micros));
        self.client.session.execute_unpaged(stmt, (account,)).await.map_err(scylla)?;
        Ok(())
    }
}
