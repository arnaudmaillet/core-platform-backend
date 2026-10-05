use std::sync::Arc;

use async_trait::async_trait;
use scylla::observability::history::HistoryListener;
use scylla::response::PagingState;
use scylla::statement::batch::{Batch, BatchType};
use scylla::statement::unprepared::Statement;
use scylla::value::CqlTimestamp;
use scylla_storage::{ProfileKind as ScyllaProfileKind, ScyllaClient, ScyllaStorageError};
use uuid::Uuid;

use crate::application::port::{ProfileReaction, ReactionLedger};
use crate::domain::value_object::{PostId, ProfileId, ReactionKind};
use crate::error::EngagementError;
use crate::infrastructure::persistence::model::ReactionRow;

fn scylla_err(e: scylla::errors::ExecutionError) -> EngagementError {
    EngagementError::Scylla(ScyllaStorageError::from(e))
}

fn row_err(ctx: &'static str, e: impl ToString) -> EngagementError {
    EngagementError::DomainViolation {
        field:   ctx.to_owned(),
        message: e.to_string(),
    }
}

/// The write timestamp (µs) of a ledger change: its event's time, so the
/// latest event wins in Scylla's last-write-wins whatever the order its
/// writes land in. Events of the same millisecond tie: a removal then wins.
fn write_time(event_at_ms: i64) -> i64 {
    event_at_ms.saturating_mul(1_000)
}

/// Rows per page when scanning `post_reactions` to backfill the profile index.
const BACKFILL_PAGE_SIZE: i32 = 500;

pub struct ScyllaReactionLedger {
    client: Arc<ScyllaClient>,
}

impl ScyllaReactionLedger {
    pub fn new(client: Arc<ScyllaClient>) -> Self {
        Self { client }
    }

    fn strict_stmt(&self, cql: &str) -> Statement {
        let mut s = Statement::new(cql);
        s.set_execution_profile_handle(Some(
            self.client
                .profiles
                .get(ScyllaProfileKind::Strict)
                .clone()
                .into_handle_with_label("strict".to_string()),
        ));
        s.set_history_listener(
            Arc::clone(&self.client.history_listener) as Arc<dyn HistoryListener>,
        );
        s
    }

    /// An empty **logged** batch on the Strict profile: its statements apply
    /// together even if the coordinator dies mid-write.
    fn strict_batch(&self) -> Batch {
        let mut batch = Batch::new(BatchType::Logged);
        batch.set_execution_profile_handle(Some(
            self.client
                .profiles
                .get(ScyllaProfileKind::Strict)
                .clone()
                .into_handle_with_label("strict-batch".to_string()),
        ));
        batch.set_history_listener(Arc::clone(&self.client.history_listener) as Arc<dyn HistoryListener>);
        batch
    }

    fn fast_stmt(&self, cql: &str) -> Statement {
        let mut s = Statement::new(cql);
        s.set_execution_profile_handle(Some(
            self.client
                .profiles
                .get(ScyllaProfileKind::Fast)
                .clone()
                .into_handle_with_label("fast".to_string()),
        ));
        s.set_history_listener(
            Arc::clone(&self.client.history_listener) as Arc<dyn HistoryListener>,
        );
        s
    }
}

#[async_trait]
impl ReactionLedger for ScyllaReactionLedger {
    async fn upsert(
        &self,
        post_id:     &PostId,
        profile_id:  &ProfileId,
        kind:        ReactionKind,
        weight:      i64,
        event_at_ms: i64,
    ) -> Result<(), EngagementError> {
        // Both tables or neither (a logged batch): the profile's index never
        // misses a reaction the post's ledger holds (#653).
        let mut batch = self.strict_batch();
        batch.append_statement(
            "INSERT INTO engagement.post_reactions \
             (post_id, profile_id, kind, weight, reacted_at) \
             VALUES (?, ?, ?, ?, ?)",
        );
        batch.append_statement(
            "INSERT INTO engagement.reactions_by_profile (profile_id, post_id, kind, reacted_at) VALUES (?, ?, ?, ?)",
        );
        batch.set_timestamp(Some(write_time(event_at_ms)));
        let at = CqlTimestamp(event_at_ms);
        let values = (
            (post_id.as_uuid(), profile_id.as_uuid(), kind.as_tinyint(), weight as i32, at),
            (profile_id.as_uuid(), post_id.as_uuid(), kind.as_tinyint(), at),
        );
        self.client.session.batch(&batch, values).await.map_err(scylla_err)?;

        Ok(())
    }

    async fn remove(
        &self,
        post_id:     &PostId,
        profile_id:  &ProfileId,
        event_at_ms: i64,
    ) -> Result<(), EngagementError> {
        let mut batch = self.strict_batch();
        batch.set_timestamp(Some(write_time(event_at_ms)));
        batch.append_statement("DELETE FROM engagement.post_reactions WHERE post_id = ? AND profile_id = ?");
        batch.append_statement("DELETE FROM engagement.reactions_by_profile WHERE profile_id = ? AND post_id = ?");
        let values = ((post_id.as_uuid(), profile_id.as_uuid()), (profile_id.as_uuid(), post_id.as_uuid()));
        self.client.session.batch(&batch, values).await.map_err(scylla_err)?;

        Ok(())
    }

    async fn list_by_profile(
        &self,
        profile_id: &ProfileId,
        limit:      i32,
        after:      Option<&PostId>,
    ) -> Result<Vec<ProfileReaction>, EngagementError> {
        let limit = limit.clamp(1, 500);
        let result = match after {
            Some(after) => {
                let stmt = self.fast_stmt(
                    "SELECT post_id, kind, reacted_at FROM engagement.reactions_by_profile \
                     WHERE profile_id = ? AND post_id > ? LIMIT ?",
                );
                self.client.session.execute_unpaged(stmt, (profile_id.as_uuid(), after.as_uuid(), limit)).await
            }
            None => {
                let stmt = self.fast_stmt(
                    "SELECT post_id, kind, reacted_at FROM engagement.reactions_by_profile WHERE profile_id = ? LIMIT ?",
                );
                self.client.session.execute_unpaged(stmt, (profile_id.as_uuid(), limit)).await
            }
        }
        .map_err(scylla_err)?;
        result
            .into_rows_result()
            .map_err(|e| row_err("list_by_profile:rows", e))?
            .rows::<(Uuid, i8, CqlTimestamp)>()
            .map_err(|e| row_err("list_by_profile:iter", e))?
            .map(|row| {
                let (post_id, kind, at) = row.map_err(|e| row_err("list_by_profile:deser", e))?;
                Ok(ProfileReaction {
                    post_id: PostId::from_uuid(post_id),
                    kind: ReactionKind::from_tinyint(kind)?,
                    reacted_at_ms: at.0,
                })
            })
            .collect()
    }

    async fn backfill_profile_index(&self) -> Result<u64, EngagementError> {
        let mut scan = self.fast_stmt("SELECT post_id, profile_id, kind, reacted_at FROM engagement.post_reactions");
        scan.set_page_size(BACKFILL_PAGE_SIZE);
        let mut paging = PagingState::start();
        let mut written = 0u64;
        loop {
            let (result, next) =
                self.client.session.execute_single_page(scan.clone(), (), paging).await.map_err(scylla_err)?;
            let rows = result
                .into_rows_result()
                .map_err(|e| row_err("backfill_profile_index:rows", e))?
                .rows::<(Uuid, Uuid, i8, CqlTimestamp)>()
                .map_err(|e| row_err("backfill_profile_index:iter", e))?
                .collect::<Result<Vec<_>, _>>()
                .map_err(|e| row_err("backfill_profile_index:deser", e))?;
            for (post_id, profile_id, kind, reacted_at) in rows {
                // Stamped with the reaction's own time, as the ledger writes
                // it: a removal processed after the scan still wins.
                let mut stmt = self.strict_stmt(
                    "INSERT INTO engagement.reactions_by_profile (profile_id, post_id, kind, reacted_at) VALUES (?, ?, ?, ?)",
                );
                stmt.set_timestamp(Some(write_time(reacted_at.0)));
                self.client
                    .session
                    .execute_unpaged(stmt, (profile_id, post_id, kind, reacted_at))
                    .await
                    .map_err(scylla_err)?;
                written += 1;
            }
            match next.into_paging_control_flow() {
                std::ops::ControlFlow::Continue(state) => paging = state,
                std::ops::ControlFlow::Break(()) => return Ok(written),
            }
        }
    }

    async fn scan_for_recovery(
        &self,
        post_id: &PostId,
    ) -> Result<Vec<ReactionRow>, EngagementError> {
        let stmt = self.fast_stmt(
            "SELECT post_id, profile_id, kind, weight, reacted_at \
             FROM engagement.post_reactions \
             WHERE post_id = ?",
        );
        let rows = self.client
            .session
            .execute_unpaged(stmt, (post_id.as_uuid(),))
            .await
            .map_err(scylla_err)?
            .into_rows_result()
            .map_err(|e| row_err("scan_for_recovery:rows", e))?
            .rows::<ReactionRow>()
            .map_err(|e| row_err("scan_for_recovery:iter", e))?
            .collect::<Result<Vec<_>, _>>()
            .map_err(|e| row_err("scan_for_recovery:deser", e))?;

        Ok(rows)
    }

    async fn apply_interaction_delta(
        &self,
        post_id:       &PostId,
        view_delta:    i64,
        share_delta:   i64,
        comment_delta: i64,
    ) -> Result<(), EngagementError> {
        let post_uuid: Uuid = post_id.as_uuid();

        if view_delta != 0 {
            let stmt = self.strict_stmt(
                "UPDATE engagement.post_interaction_counters \
                 SET view_count = view_count + ? \
                 WHERE post_id = ?",
            );
            self.client
                .session
                .execute_unpaged(stmt, (view_delta, post_uuid))
                .await
                .map_err(scylla_err)?;
        }

        if share_delta != 0 {
            let stmt = self.strict_stmt(
                "UPDATE engagement.post_interaction_counters \
                 SET share_count = share_count + ? \
                 WHERE post_id = ?",
            );
            self.client
                .session
                .execute_unpaged(stmt, (share_delta, post_uuid))
                .await
                .map_err(scylla_err)?;
        }

        if comment_delta != 0 {
            let stmt = self.strict_stmt(
                "UPDATE engagement.post_interaction_counters \
                 SET comment_count = comment_count + ? \
                 WHERE post_id = ?",
            );
            self.client
                .session
                .execute_unpaged(stmt, (comment_delta, post_uuid))
                .await
                .map_err(scylla_err)?;
        }

        Ok(())
    }
}
