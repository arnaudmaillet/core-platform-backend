//! [`SavedPosts`] on Scylla (migration 0010): `saved_posts_by_profile` (the
//! Saved tab, newest saves first) and `saves_by_account` (the authority).
//! Both are written in one logged batch with the save's time as the write
//! timestamp; deletions are stamped later. A tab row the authority doesn't
//! confirm (two saves racing, an unsave racing a save) is never listed.

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

use crate::application::port::{SavedCursor, SavedPost, SavedPosts};
use crate::error::EngagementError;

pub struct ScyllaSavedPosts {
    client: Arc<ScyllaClient>,
}

fn scylla(e: impl Into<ScyllaStorageError>) -> EngagementError {
    EngagementError::Scylla(e.into())
}

fn rows_err(e: impl std::fmt::Display) -> EngagementError {
    EngagementError::DomainViolation { field: "saved_posts".into(), message: e.to_string() }
}

fn account_uuid(account: &str) -> Result<Uuid, EngagementError> {
    Uuid::parse_str(account)
        .map_err(|_| EngagementError::DomainViolation { field: "account_id".into(), message: account.to_owned() })
}

impl ScyllaSavedPosts {
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

    /// When `profile_id` saved `post_id`, if it did.
    async fn saved_at(&self, account: Uuid, profile_id: &str, post_id: &str) -> Result<Option<CqlTimestamp>, EngagementError> {
        let stmt = self.statement(
            "SELECT saved_at FROM engagement.saves_by_account WHERE account_id = ? AND profile_id = ? AND post_id = ?",
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
}

#[async_trait]
impl SavedPosts for ScyllaSavedPosts {
    async fn save(&self, account: &str, profile_id: &str, post_id: &str, at_micros: i64) -> Result<(), EngagementError> {
        let account = account_uuid(account)?;
        let tab = "INSERT INTO engagement.saved_posts_by_profile (profile_id, saved_at, post_id, account_id) \
                   VALUES (?, ?, ?, ?)";
        if let Some(first) = self.saved_at(account, profile_id, post_id).await? {
            // Saved already: its first time stays; the tab row is rewritten
            // (a partial write repaired).
            let stmt = self.statement(tab, Some(at_micros));
            self.client.session.execute_unpaged(stmt, (profile_id, first, post_id, account)).await.map_err(scylla)?;
            return Ok(());
        }
        let saved_at = CqlTimestamp(at_micros / 1_000);
        let mut batch = self.batch(at_micros);
        batch.append_statement(
            "INSERT INTO engagement.saves_by_account (account_id, profile_id, post_id, saved_at) VALUES (?, ?, ?, ?)",
        );
        batch.append_statement(tab);
        batch.append_statement("UPDATE engagement.saves_by_account SET profile_ids = profile_ids + ? WHERE account_id = ?");
        let values = (
            (account, profile_id, post_id, saved_at),
            (profile_id, saved_at, post_id, account),
            (vec![profile_id], account),
        );
        self.client.session.batch(&batch, values).await.map_err(scylla)?;
        Ok(())
    }

    async fn unsave(&self, account: &str, profile_id: &str, post_id: &str, at_micros: i64) -> Result<(), EngagementError> {
        let account = account_uuid(account)?;
        let Some(saved_at) = self.saved_at(account, profile_id, post_id).await? else { return Ok(()) };
        let mut batch = self.batch(at_micros);
        batch.append_statement("DELETE FROM engagement.saves_by_account WHERE account_id = ? AND profile_id = ? AND post_id = ?");
        batch.append_statement(
            "DELETE FROM engagement.saved_posts_by_profile WHERE profile_id = ? AND saved_at = ? AND post_id = ?",
        );
        let values = ((account, profile_id, post_id), (profile_id, saved_at, post_id));
        self.client.session.batch(&batch, values).await.map_err(scylla)?;
        Ok(())
    }

    async fn list(
        &self,
        profile_id: &str,
        limit: i32,
        after: Option<&SavedCursor>,
    ) -> Result<(Vec<SavedPost>, Option<SavedCursor>), EngagementError> {
        #[derive(DeserializeRow)]
        struct TabRow {
            saved_at:   Option<CqlTimestamp>,
            post_id:    Option<String>,
            account_id: Option<Uuid>,
        }
        let result = match after {
            Some(after) => {
                let stmt = self.statement(
                    "SELECT saved_at, post_id, account_id FROM engagement.saved_posts_by_profile \
                     WHERE profile_id = ? AND (saved_at, post_id) < (?, ?) LIMIT ?",
                    None,
                );
                let at = CqlTimestamp(after.saved_at_ms);
                self.client.session.execute_unpaged(stmt, (profile_id, at, after.post_id.as_str(), limit)).await
            }
            None => {
                let stmt = self.statement(
                    "SELECT saved_at, post_id, account_id FROM engagement.saved_posts_by_profile \
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
        for row in rows.rows::<TabRow>().map_err(rows_err)? {
            let row = row.map_err(rows_err)?;
            account = account.or(row.account_id);
            // A partition left with its static column only has no save.
            if let (Some(at), Some(post)) = (row.saved_at, row.post_id) {
                page.push((at.0, post));
            }
        }
        let next = (page.len() == limit as usize)
            .then(|| page.last().map(|(at, post)| SavedCursor { saved_at_ms: *at, post_id: post.clone() }))
            .flatten();
        let Some(account) = account.filter(|_| !page.is_empty()) else { return Ok((Vec::new(), next)) };
        // Only the saves the authority confirms, at the same time.
        let posts: Vec<&str> = page.iter().map(|(_, post)| post.as_str()).collect();
        let stmt = self.statement(
            "SELECT post_id, saved_at FROM engagement.saves_by_account \
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
        let saves = page
            .into_iter()
            .filter(|(at, post)| confirmed.get(post) == Some(at))
            .map(|(saved_at_ms, post_id)| SavedPost { profile_id: profile_id.to_owned(), post_id, saved_at_ms })
            .collect();
        Ok((saves, next))
    }

    async fn list_by_account(
        &self,
        account: &str,
        limit: i32,
        after: Option<(&str, &str)>,
    ) -> Result<Vec<SavedPost>, EngagementError> {
        let account = account_uuid(account)?;
        let result = match after {
            Some((profile, post)) => {
                let stmt = self.statement(
                    "SELECT profile_id, post_id, saved_at FROM engagement.saves_by_account \
                     WHERE account_id = ? AND (profile_id, post_id) > (?, ?) LIMIT ?",
                    None,
                );
                self.client.session.execute_unpaged(stmt, (account, profile, post, limit)).await
            }
            None => {
                let stmt = self.statement(
                    "SELECT profile_id, post_id, saved_at FROM engagement.saves_by_account WHERE account_id = ? LIMIT ?",
                    None,
                );
                self.client.session.execute_unpaged(stmt, (account, limit)).await
            }
        }
        .map_err(scylla)?;
        result
            .into_rows_result()
            .map_err(rows_err)?
            .rows::<(String, String, CqlTimestamp)>()
            .map_err(rows_err)?
            .map(|row| {
                row.map(|(profile_id, post_id, at)| SavedPost { profile_id, post_id, saved_at_ms: at.0 }).map_err(rows_err)
            })
            .collect()
    }

    async fn forget_profile(&self, profile_id: &str, at_micros: i64) -> Result<(), EngagementError> {
        let stmt = self.statement("SELECT account_id FROM engagement.saved_posts_by_profile WHERE profile_id = ? LIMIT 1", None);
        let account = self
            .client
            .session
            .execute_unpaged(stmt, (profile_id,))
            .await
            .map_err(scylla)?
            .into_rows_result()
            .map_err(rows_err)?
            .maybe_first_row::<(Option<Uuid>,)>()
            .map_err(rows_err)?
            .and_then(|(account,)| account);
        // Nothing saved, nothing to forget.
        let Some(account) = account else { return Ok(()) };
        let mut batch = self.batch(at_micros);
        batch.append_statement("DELETE FROM engagement.saved_posts_by_profile WHERE profile_id = ?");
        batch.append_statement("DELETE FROM engagement.saves_by_account WHERE account_id = ? AND profile_id = ?");
        self.client.session.batch(&batch, ((profile_id,), (account, profile_id))).await.map_err(scylla)?;
        Ok(())
    }

    async fn forget_account(&self, account: &str, at_micros: i64) -> Result<(), EngagementError> {
        let account = account_uuid(account)?;
        // Every profile that ever saved: each one's whole tab goes (rows the
        // authority no longer confirms too), then the authority.
        let stmt = self.statement("SELECT profile_ids FROM engagement.saves_by_account WHERE account_id = ? LIMIT 1", None);
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
            let stmt = self.statement("DELETE FROM engagement.saved_posts_by_profile WHERE profile_id = ?", Some(at_micros));
            self.client.session.execute_unpaged(stmt, (profile.as_str(),)).await.map_err(scylla)?;
        }
        let stmt = self.statement("DELETE FROM engagement.saves_by_account WHERE account_id = ?", Some(at_micros));
        self.client.session.execute_unpaged(stmt, (account,)).await.map_err(scylla)?;
        Ok(())
    }
}
