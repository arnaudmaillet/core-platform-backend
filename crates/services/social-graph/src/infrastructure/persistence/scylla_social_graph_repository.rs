use std::sync::Arc;

use async_trait::async_trait;
use base64::{engine::general_purpose::URL_SAFE_NO_PAD, Engine as _};
use chrono::{TimeZone, Utc};
use scylla::observability::history::HistoryListener;
use scylla::statement::batch::{Batch, BatchType};
use scylla::statement::unprepared::Statement;
use scylla::value::CqlTimestamp;
use scylla::DeserializeRow;
use uuid::Uuid;

use scylla_storage::{ProfileKind as ScyllaProfileKind, ScyllaClient, ScyllaStorageError};

use crate::application::port::SocialGraphRepository;
use crate::domain::access::AccessFacts;
use crate::domain::aggregate::{Relation, RelationContext};
use crate::domain::entity::{BlockEdge, FollowEdge};
use crate::domain::value_object::ProfileId;
use crate::error::SocialGraphError;
use crate::infrastructure::persistence::model::{BlockRow, FollowRow};

// ── Page-token types ──────────────────────────────────────────────────────────

#[derive(serde::Serialize, serde::Deserialize)]
struct FollowPageToken {
    followed_at_ms: i64,
}

#[derive(serde::Serialize, serde::Deserialize)]
struct BlockPageToken {
    last_blockee_id: String,
}

// ── Error helpers ─────────────────────────────────────────────────────────────

fn scylla_err(e: scylla::errors::ExecutionError) -> SocialGraphError {
    SocialGraphError::Storage(ScyllaStorageError::from(e))
}

fn row_err(ctx: &'static str, e: impl ToString) -> SocialGraphError {
    SocialGraphError::DomainViolation {
        field:   ctx.to_owned(),
        message: e.to_string(),
    }
}

fn token_err(field: &'static str, msg: &'static str) -> SocialGraphError {
    SocialGraphError::DomainViolation {
        field:   field.to_owned(),
        message: msg.to_owned(),
    }
}

// ── Repository ────────────────────────────────────────────────────────────────

pub struct ScyllaSocialGraphRepository {
    client: Arc<ScyllaClient>,
}

impl ScyllaSocialGraphRepository {
    pub fn new(client: Arc<ScyllaClient>) -> Self {
        Self { client }
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

    /// Builds an empty **logged** batch carrying the Strict execution profile and
    /// the OTel history listener. A logged batch guarantees that all appended
    /// statements eventually apply atomically even if the coordinator dies
    /// mid-write — the consistency primitive that keeps the three denormalized
    /// follow tables from diverging into a split-brain graph. Callers append the
    /// statements and supply positional `BatchValues`.
    fn strict_batch(&self) -> Batch {
        let mut batch = Batch::new(BatchType::Logged);
        batch.set_execution_profile_handle(Some(
            self.client
                .profiles
                .get(ScyllaProfileKind::Strict)
                .clone()
                .into_handle_with_label("strict-batch".to_string()),
        ));
        batch.set_history_listener(
            Arc::clone(&self.client.history_listener) as Arc<dyn HistoryListener>,
        );
        batch
    }

    fn dt_ms(dt: chrono::DateTime<Utc>) -> CqlTimestamp {
        CqlTimestamp(dt.timestamp_millis())
    }

    fn ms_to_dt(ms: i64) -> Result<chrono::DateTime<Utc>, SocialGraphError> {
        Utc.timestamp_millis_opt(ms).single().ok_or_else(|| SocialGraphError::DomainViolation {
            field:   "timestamp".to_owned(),
            message: format!("invalid millisecond timestamp: {ms}"),
        })
    }

    // ── Point-lookup helpers used by load_relation ────────────────────────────

    async fn get_follow_since(
        &self,
        follower_id: &ProfileId,
        followee_id: &ProfileId,
    ) -> Result<Option<chrono::DateTime<Utc>>, SocialGraphError> {
        #[derive(DeserializeRow)]
        struct Row { followed_at: CqlTimestamp }

        let stmt = self.fast_stmt(
            "SELECT followed_at FROM social_graph.follow_status \
             WHERE follower_id = ? AND followee_id = ?",
        );
        let result = self
            .client
            .session
            .execute_unpaged(stmt, (follower_id.as_uuid(), followee_id.as_uuid()))
            .await
            .map_err(scylla_err)?;

        let row = result
            .into_rows_result()
            .map_err(|e| row_err("get_follow_since:rows", e))?
            .maybe_first_row::<Row>()
            .map_err(|e| row_err("get_follow_since:deser", e))?;

        row.map(|r| Self::ms_to_dt(r.followed_at.0)).transpose()
    }

    async fn get_request_since(
        &self,
        requester_id: &ProfileId,
        target_id:    &ProfileId,
    ) -> Result<Option<chrono::DateTime<Utc>>, SocialGraphError> {
        #[derive(DeserializeRow)]
        struct Row { requested_at: CqlTimestamp }

        let stmt = self.fast_stmt(
            "SELECT requested_at FROM social_graph.follow_request_status \
             WHERE requester_id = ? AND target_id = ?",
        );
        let row = self
            .client
            .session
            .execute_unpaged(stmt, (requester_id.as_uuid(), target_id.as_uuid()))
            .await
            .map_err(scylla_err)?
            .into_rows_result()
            .map_err(|e| row_err("get_request_since:rows", e))?
            .maybe_first_row::<Row>()
            .map_err(|e| row_err("get_request_since:deser", e))?;

        row.map(|r| Self::ms_to_dt(r.requested_at.0)).transpose()
    }

    async fn get_block_exists(
        &self,
        blocker_id: &ProfileId,
        blockee_id: &ProfileId,
    ) -> Result<bool, SocialGraphError> {
        #[derive(DeserializeRow)]
        #[allow(dead_code)]
        struct Row { blocked_at: CqlTimestamp }

        let stmt = self.fast_stmt(
            "SELECT blocked_at FROM social_graph.blocks \
             WHERE blocker_id = ? AND blockee_id = ?",
        );
        let result = self
            .client
            .session
            .execute_unpaged(stmt, (blocker_id.as_uuid(), blockee_id.as_uuid()))
            .await
            .map_err(scylla_err)?;

        let row = result
            .into_rows_result()
            .map_err(|e| row_err("get_block_exists:rows", e))?
            .maybe_first_row::<Row>()
            .map_err(|e| row_err("get_block_exists:deser", e))?;

        Ok(row.is_some())
    }
}

#[async_trait]
impl SocialGraphRepository for ScyllaSocialGraphRepository {
    // ── load_relation ─────────────────────────────────────────────────────────

    async fn load_relation(
        &self,
        actor_id:  &ProfileId,
        target_id: &ProfileId,
    ) -> Result<Relation, SocialGraphError> {
        // Fire six concurrent O(1) ScyllaDB point-lookups.
        let (r1, r2, r3, r4, r5, r6) = tokio::join!(
            self.get_follow_since(actor_id, target_id),
            self.get_follow_since(target_id, actor_id),
            self.get_block_exists(actor_id, target_id),
            self.get_block_exists(target_id, actor_id),
            self.get_request_since(actor_id, target_id),
            self.get_request_since(target_id, actor_id),
        );

        Ok(Relation::from_context(
            *actor_id,
            *target_id,
            RelationContext {
                actor_follows_target_since: r1?,
                target_follows_actor_since: r2?,
                actor_blocks_target:        r3?,
                target_blocks_actor:        r4?,
                actor_requested_target_at:  r5?,
                target_requested_actor_at:  r6?,
            },
        ))
    }

    // ── follow requests ───────────────────────────────────────────────────────

    async fn persist_follow_request(
        &self,
        requester_id: &ProfileId,
        target_id:    &ProfileId,
        requested_at: chrono::DateTime<Utc>,
    ) -> Result<(), SocialGraphError> {
        let ts = Self::dt_ms(requested_at);
        let mut batch = self.strict_batch();
        batch.append_statement(
            "INSERT INTO social_graph.follow_request_status \
             (requester_id, target_id, requested_at) VALUES (?, ?, ?)",
        );
        batch.append_statement(
            "INSERT INTO social_graph.follow_requests \
             (target_id, requested_at, requester_id) VALUES (?, ?, ?)",
        );
        let values = (
            (requester_id.as_uuid(), target_id.as_uuid(), ts),
            (target_id.as_uuid(), ts, requester_id.as_uuid()),
        );
        self.client.session.batch(&batch, values).await.map_err(scylla_err)?;
        Ok(())
    }

    async fn delete_follow_request(
        &self,
        requester_id: &ProfileId,
        target_id:    &ProfileId,
        requested_at: chrono::DateTime<Utc>,
    ) -> Result<(), SocialGraphError> {
        let ts = Self::dt_ms(requested_at);
        let mut batch = self.strict_batch();
        batch.append_statement(
            "DELETE FROM social_graph.follow_request_status \
             WHERE requester_id = ? AND target_id = ?",
        );
        batch.append_statement(
            "DELETE FROM social_graph.follow_requests \
             WHERE target_id = ? AND requested_at = ? AND requester_id = ?",
        );
        let values = (
            (requester_id.as_uuid(), target_id.as_uuid()),
            (target_id.as_uuid(), ts, requester_id.as_uuid()),
        );
        self.client.session.batch(&batch, values).await.map_err(scylla_err)?;
        Ok(())
    }

    async fn approve_follow_request(
        &self,
        requester_id: &ProfileId,
        target_id:    &ProfileId,
        requested_at: chrono::DateTime<Utc>,
        followed_at:  chrono::DateTime<Utc>,
    ) -> Result<(), SocialGraphError> {
        let req_ts = Self::dt_ms(requested_at);
        let ts = Self::dt_ms(followed_at);
        // One logged batch: the request rows go and the three follow rows
        // appear together, so a crash can never leave both (or neither).
        let mut batch = self.strict_batch();
        batch.append_statement(
            "DELETE FROM social_graph.follow_request_status \
             WHERE requester_id = ? AND target_id = ?",
        );
        batch.append_statement(
            "DELETE FROM social_graph.follow_requests \
             WHERE target_id = ? AND requested_at = ? AND requester_id = ?",
        );
        batch.append_statement(
            "INSERT INTO social_graph.follow_status \
             (follower_id, followee_id, followed_at) VALUES (?, ?, ?)",
        );
        batch.append_statement(
            "INSERT INTO social_graph.following \
             (follower_id, followed_at, followee_id) VALUES (?, ?, ?)",
        );
        batch.append_statement(
            "INSERT INTO social_graph.followers \
             (followee_id, followed_at, follower_id) VALUES (?, ?, ?)",
        );
        let values = (
            (requester_id.as_uuid(), target_id.as_uuid()),
            (target_id.as_uuid(), req_ts, requester_id.as_uuid()),
            (requester_id.as_uuid(), target_id.as_uuid(), ts),
            (requester_id.as_uuid(), ts, target_id.as_uuid()),
            (target_id.as_uuid(), ts, requester_id.as_uuid()),
        );
        self.client.session.batch(&batch, values).await.map_err(scylla_err)?;
        Ok(())
    }

    async fn list_follow_requests(
        &self,
        target_id:  &ProfileId,
        limit:      i32,
        page_token: Option<&str>,
    ) -> Result<(Vec<FollowEdge>, Option<String>), SocialGraphError> {
        // Same shape and page token as the followers list (`requested_at`
        // plays `followed_at`).
        let limit = limit.clamp(1, 100);
        let token = decode_follow_token(page_token)?;
        let result = if let Some(ref tok) = token {
            let stmt = self.fast_stmt(
                "SELECT requester_id, requested_at FROM social_graph.follow_requests \
                 WHERE target_id = ? AND requested_at < ? LIMIT ?",
            );
            self.client
                .session
                .execute_unpaged(stmt, (target_id.as_uuid(), CqlTimestamp(tok.followed_at_ms), limit))
                .await
        } else {
            let stmt = self.fast_stmt(
                "SELECT requester_id, requested_at FROM social_graph.follow_requests \
                 WHERE target_id = ? LIMIT ?",
            );
            self.client.session.execute_unpaged(stmt, (target_id.as_uuid(), limit)).await
        };
        let rows: Vec<FollowRow> = result
            .map_err(scylla_err)?
            .into_rows_result()
            .map_err(|e| row_err("list_follow_requests:rows", e))?
            .rows::<FollowRow>()
            .map_err(|e| row_err("list_follow_requests:iter", e))?
            .collect::<Result<Vec<_>, _>>()
            .map_err(|e| row_err("list_follow_requests:deser", e))?;
        build_follow_page(rows, limit)
    }

    // ── persist_follow ────────────────────────────────────────────────────────

    async fn persist_follow(
        &self,
        actor_id:    &ProfileId,
        target_id:   &ProfileId,
        followed_at: chrono::DateTime<Utc>,
    ) -> Result<(), SocialGraphError> {
        let ts = Self::dt_ms(followed_at);

        // All three denormalized rows are written in a single logged batch so they
        // can never diverge: either follow_status, following, AND followers all
        // appear, or none do. The identical `ts` across all three is what makes the
        // later clustering-key DELETE in `delete_follow` able to find the rows.
        //
        // The batch spans two partitions (follow_status + following key on
        // follower_id; followers keys on followee_id), so it is a genuine
        // multi-partition logged batch — heavier than three unlogged writes, but the
        // atomicity is required to keep the follow graph consistent at scale.
        let mut batch = self.strict_batch();
        batch.append_statement(
            "INSERT INTO social_graph.follow_status \
             (follower_id, followee_id, followed_at) VALUES (?, ?, ?)",
        );
        batch.append_statement(
            "INSERT INTO social_graph.following \
             (follower_id, followed_at, followee_id) VALUES (?, ?, ?)",
        );
        batch.append_statement(
            "INSERT INTO social_graph.followers \
             (followee_id, followed_at, follower_id) VALUES (?, ?, ?)",
        );

        let values = (
            (actor_id.as_uuid(), target_id.as_uuid(), ts),
            (actor_id.as_uuid(), ts, target_id.as_uuid()),
            (target_id.as_uuid(), ts, actor_id.as_uuid()),
        );

        self.client
            .session
            .batch(&batch, values)
            .await
            .map_err(scylla_err)?;

        Ok(())
    }

    // ── delete_follow ─────────────────────────────────────────────────────────

    async fn delete_follow(
        &self,
        actor_id:    &ProfileId,
        target_id:   &ProfileId,
        followed_at: chrono::DateTime<Utc>,
    ) -> Result<(), SocialGraphError> {
        // `followed_at` MUST be the exact timestamp stored at follow time — it is the
        // clustering key for the following/followers rows, and a mismatched value
        // makes the DELETE a silent no-op (leaving ghost adjacency rows). Callers
        // source it from `follow_status` via `load_relation`, never re-derive it.
        let ts = Self::dt_ms(followed_at);

        // Remove all three rows atomically in a logged batch (mirrors persist_follow)
        // so an interrupted unfollow can never leave the graph half-deleted.
        let mut batch = self.strict_batch();
        batch.append_statement(
            "DELETE FROM social_graph.follow_status \
             WHERE follower_id = ? AND followee_id = ?",
        );
        batch.append_statement(
            "DELETE FROM social_graph.following \
             WHERE follower_id = ? AND followed_at = ? AND followee_id = ?",
        );
        batch.append_statement(
            "DELETE FROM social_graph.followers \
             WHERE followee_id = ? AND followed_at = ? AND follower_id = ?",
        );

        let values = (
            (actor_id.as_uuid(), target_id.as_uuid()),
            (actor_id.as_uuid(), ts, target_id.as_uuid()),
            (target_id.as_uuid(), ts, actor_id.as_uuid()),
        );

        self.client
            .session
            .batch(&batch, values)
            .await
            .map_err(scylla_err)?;

        Ok(())
    }

    // ── persist_block ─────────────────────────────────────────────────────────

    async fn persist_block(
        &self,
        blocker_id: &ProfileId,
        blockee_id: &ProfileId,
        blocked_at: chrono::DateTime<Utc>,
    ) -> Result<(), SocialGraphError> {
        let stmt = self.strict_stmt(
            "INSERT INTO social_graph.blocks \
             (blocker_id, blockee_id, blocked_at) VALUES (?, ?, ?)",
        );
        self.client
            .session
            .execute_unpaged(
                stmt,
                (blocker_id.as_uuid(), blockee_id.as_uuid(), Self::dt_ms(blocked_at)),
            )
            .await
            .map_err(scylla_err)?;
        Ok(())
    }

    // ── delete_block ──────────────────────────────────────────────────────────

    async fn delete_block(
        &self,
        blocker_id: &ProfileId,
        blockee_id: &ProfileId,
    ) -> Result<(), SocialGraphError> {
        let stmt = self.strict_stmt(
            "DELETE FROM social_graph.blocks \
             WHERE blocker_id = ? AND blockee_id = ?",
        );
        self.client
            .session
            .execute_unpaged(stmt, (blocker_id.as_uuid(), blockee_id.as_uuid()))
            .await
            .map_err(scylla_err)?;
        Ok(())
    }

    // ── list_followers ────────────────────────────────────────────────────────

    async fn list_followers(
        &self,
        followee_id: &ProfileId,
        limit:       i32,
        page_token:  Option<&str>,
    ) -> Result<(Vec<FollowEdge>, Option<String>), SocialGraphError> {
        // LIMIT binds as CQL `int` (32-bit); the old `as i64` cast made Scylla
        // reject every query (SerializationError i64 vs Native(Int)) — the
        // timeline feed cold-rebuild failed 100% under the staging soak.
        let limit = limit.clamp(1, 100);
        let token = decode_follow_token(page_token)?;

        let rows: Vec<FollowRow> = if let Some(ref tok) = token {
            let stmt = self.fast_stmt(
                "SELECT follower_id, followed_at FROM social_graph.followers \
                 WHERE followee_id = ? AND followed_at < ? LIMIT ?",
            );
            self.client
                .session
                .execute_unpaged(
                    stmt,
                    (followee_id.as_uuid(), CqlTimestamp(tok.followed_at_ms), limit),
                )
                .await
                .map_err(scylla_err)?
                .into_rows_result()
                .map_err(|e| row_err("list_followers:rows", e))?
                .rows::<FollowRow>()
                .map_err(|e| row_err("list_followers:iter", e))?
                .collect::<Result<Vec<_>, _>>()
                .map_err(|e| row_err("list_followers:deser", e))?
        } else {
            let stmt = self.fast_stmt(
                "SELECT follower_id, followed_at FROM social_graph.followers \
                 WHERE followee_id = ? LIMIT ?",
            );
            self.client
                .session
                .execute_unpaged(stmt, (followee_id.as_uuid(), limit))
                .await
                .map_err(scylla_err)?
                .into_rows_result()
                .map_err(|e| row_err("list_followers:rows", e))?
                .rows::<FollowRow>()
                .map_err(|e| row_err("list_followers:iter", e))?
                .collect::<Result<Vec<_>, _>>()
                .map_err(|e| row_err("list_followers:deser", e))?
        };

        build_follow_page(rows, limit)
    }

    // ── list_following ────────────────────────────────────────────────────────

    async fn list_following(
        &self,
        follower_id: &ProfileId,
        limit:       i32,
        page_token:  Option<&str>,
    ) -> Result<(Vec<FollowEdge>, Option<String>), SocialGraphError> {
        // LIMIT binds as CQL `int` (32-bit); the old `as i64` cast made Scylla
        // reject every query (SerializationError i64 vs Native(Int)) — the
        // timeline feed cold-rebuild failed 100% under the staging soak.
        let limit = limit.clamp(1, 100);
        let token = decode_follow_token(page_token)?;

        let rows: Vec<FollowRow> = if let Some(ref tok) = token {
            let stmt = self.fast_stmt(
                "SELECT followee_id, followed_at FROM social_graph.following \
                 WHERE follower_id = ? AND followed_at < ? LIMIT ?",
            );
            self.client
                .session
                .execute_unpaged(
                    stmt,
                    (follower_id.as_uuid(), CqlTimestamp(tok.followed_at_ms), limit),
                )
                .await
                .map_err(scylla_err)?
                .into_rows_result()
                .map_err(|e| row_err("list_following:rows", e))?
                .rows::<FollowRow>()
                .map_err(|e| row_err("list_following:iter", e))?
                .collect::<Result<Vec<_>, _>>()
                .map_err(|e| row_err("list_following:deser", e))?
        } else {
            let stmt = self.fast_stmt(
                "SELECT followee_id, followed_at FROM social_graph.following \
                 WHERE follower_id = ? LIMIT ?",
            );
            self.client
                .session
                .execute_unpaged(stmt, (follower_id.as_uuid(), limit))
                .await
                .map_err(scylla_err)?
                .into_rows_result()
                .map_err(|e| row_err("list_following:rows", e))?
                .rows::<FollowRow>()
                .map_err(|e| row_err("list_following:iter", e))?
                .collect::<Result<Vec<_>, _>>()
                .map_err(|e| row_err("list_following:deser", e))?
        };

        build_follow_page(rows, limit)
    }

    // ── list_blocks ───────────────────────────────────────────────────────────

    async fn list_blocks(
        &self,
        blocker_id: &ProfileId,
        limit:      i32,
        page_token: Option<&str>,
    ) -> Result<(Vec<BlockEdge>, Option<String>), SocialGraphError> {
        // LIMIT binds as CQL `int` (32-bit); the old `as i64` cast made Scylla
        // reject every query (SerializationError i64 vs Native(Int)) — the
        // timeline feed cold-rebuild failed 100% under the staging soak.
        let limit = limit.clamp(1, 100);

        let token: Option<BlockPageToken> = page_token
            .map(|t| {
                let bytes = URL_SAFE_NO_PAD
                    .decode(t)
                    .map_err(|_| token_err("page_token", "invalid base64 encoding"))?;
                serde_json::from_slice(&bytes)
                    .map_err(|_| token_err("page_token", "invalid block page token format"))
            })
            .transpose()?;

        let rows_result = if let Some(ref tok) = token {
            let last_id = Uuid::parse_str(&tok.last_blockee_id).map_err(|_| {
                token_err("page_token.last_blockee_id", "invalid UUID in block page token")
            })?;
            let stmt = self.fast_stmt(
                "SELECT blockee_id, blocked_at FROM social_graph.blocks \
                 WHERE blocker_id = ? AND blockee_id > ? LIMIT ?",
            );
            self.client
                .session
                .execute_unpaged(stmt, (blocker_id.as_uuid(), last_id, limit))
                .await
                .map_err(scylla_err)?
                .into_rows_result()
                .map_err(|e| row_err("list_blocks:rows", e))?
        } else {
            let stmt = self.fast_stmt(
                "SELECT blockee_id, blocked_at FROM social_graph.blocks \
                 WHERE blocker_id = ? LIMIT ?",
            );
            self.client
                .session
                .execute_unpaged(stmt, (blocker_id.as_uuid(), limit))
                .await
                .map_err(scylla_err)?
                .into_rows_result()
                .map_err(|e| row_err("list_blocks:rows", e))?
        };

        let rows: Vec<BlockRow> = rows_result
            .rows::<BlockRow>()
            .map_err(|e| row_err("list_blocks:iter", e))?
            .collect::<Result<Vec<_>, _>>()
            .map_err(|e| row_err("list_blocks:deser", e))?;

        let total = rows.len();
        let mut edges = Vec::with_capacity(total);
        let mut last_blockee_id = String::new();

        for row in &rows {
            last_blockee_id = row.blockee_id.to_string();
            let blocked_at = Utc
                .timestamp_millis_opt(row.blocked_at.0)
                .single()
                .ok_or_else(|| SocialGraphError::DomainViolation {
                    field:   "blocked_at".to_owned(),
                    message: format!("invalid timestamp {}", row.blocked_at.0),
                })?;
            edges.push(BlockEdge {
                blockee_id: ProfileId::from_uuid(row.blockee_id),
                blocked_at,
            });
        }

        let next_token = if total == limit as usize {
            let tok  = BlockPageToken { last_blockee_id };
            let json = serde_json::to_vec(&tok).unwrap_or_default();
            Some(URL_SAFE_NO_PAD.encode(json))
        } else {
            None
        };

        Ok((edges, next_token))
    }

    // ── access check ──────────────────────────────────────────────────────────

    async fn load_access_facts(
        &self,
        viewers: &[ProfileId],
        targets: &[ProfileId],
    ) -> Result<AccessFacts, SocialGraphError> {
        let mut facts = AccessFacts::default();
        if targets.is_empty() {
            return Ok(facts);
        }
        let viewer_ids: Vec<Uuid> = viewers.iter().map(ProfileId::as_uuid).collect();
        let target_ids: Vec<Uuid> = targets.iter().map(ProfileId::as_uuid).collect();

        #[derive(DeserializeRow)]
        struct Pair { a: Uuid, b: Uuid }
        #[derive(DeserializeRow)]
        struct Audience { profile_id: Uuid, private: Option<bool>, hidden: Option<bool> }

        let audience = self.fast_stmt(
            "SELECT profile_id, private, hidden FROM social_graph.profile_audience \
             WHERE profile_id IN ?",
        );
        let rows = self
            .client
            .session
            .execute_unpaged(audience, (&target_ids,))
            .await
            .map_err(scylla_err)?
            .into_rows_result()
            .map_err(|e| row_err("load_access_facts:audience", e))?;
        for row in rows.rows::<Audience>().map_err(|e| row_err("load_access_facts:audience", e))? {
            let row = row.map_err(|e| row_err("load_access_facts:audience", e))?;
            let id = ProfileId::from_uuid(row.profile_id);
            if row.private == Some(true) {
                facts.private.insert(id);
            }
            if row.hidden == Some(true) {
                facts.hidden.insert(id);
            }
        }

        // An anonymous viewer has no follows and no blocks.
        if viewer_ids.is_empty() {
            return Ok(facts);
        }

        let pairs = |cql: &'static str, left: &Vec<Uuid>, right: &Vec<Uuid>| {
            let stmt = self.fast_stmt(cql);
            let (left, right) = (left.clone(), right.clone());
            async move {
                let rows = self
                    .client
                    .session
                    .execute_unpaged(stmt, (left, right))
                    .await
                    .map_err(scylla_err)?
                    .into_rows_result()
                    .map_err(|e| row_err("load_access_facts:pairs", e))?;
                rows.rows::<Pair>()
                    .map_err(|e| row_err("load_access_facts:pairs", e))?
                    .map(|r| {
                        r.map(|p| (ProfileId::from_uuid(p.a), ProfileId::from_uuid(p.b)))
                            .map_err(|e| row_err("load_access_facts:pairs", e))
                    })
                    .collect::<Result<Vec<_>, _>>()
            }
        };
        let (follows, blocked_by_viewer, blocked_by_target) = tokio::join!(
            pairs(
                "SELECT follower_id AS a, followee_id AS b FROM social_graph.follow_status \
                 WHERE follower_id IN ? AND followee_id IN ?",
                &viewer_ids,
                &target_ids,
            ),
            pairs(
                "SELECT blocker_id AS a, blockee_id AS b FROM social_graph.blocks \
                 WHERE blocker_id IN ? AND blockee_id IN ?",
                &viewer_ids,
                &target_ids,
            ),
            pairs(
                "SELECT blocker_id AS a, blockee_id AS b FROM social_graph.blocks \
                 WHERE blocker_id IN ? AND blockee_id IN ?",
                &target_ids,
                &viewer_ids,
            ),
        );
        facts.follows.extend(follows?);
        facts.blocks.extend(blocked_by_viewer?);
        facts.blocks.extend(blocked_by_target?);
        Ok(facts)
    }

    async fn set_profile_private(&self, profile_id: &ProfileId, private: bool) -> Result<(), SocialGraphError> {
        let stmt = self.strict_stmt(
            "UPDATE social_graph.profile_audience SET private = ? WHERE profile_id = ?",
        );
        self.client
            .session
            .execute_unpaged(stmt, (private, profile_id.as_uuid()))
            .await
            .map_err(scylla_err)?;
        Ok(())
    }

    async fn set_profile_hidden(&self, profile_id: &ProfileId, hidden: bool) -> Result<(), SocialGraphError> {
        let stmt = self.strict_stmt(
            "UPDATE social_graph.profile_audience SET hidden = ? WHERE profile_id = ?",
        );
        self.client
            .session
            .execute_unpaged(stmt, (hidden, profile_id.as_uuid()))
            .await
            .map_err(scylla_err)?;
        Ok(())
    }
}

// ── Shared helpers ────────────────────────────────────────────────────────────

fn decode_follow_token(
    page_token: Option<&str>,
) -> Result<Option<FollowPageToken>, SocialGraphError> {
    page_token
        .map(|t| {
            let bytes = URL_SAFE_NO_PAD
                .decode(t)
                .map_err(|_| token_err("page_token", "invalid base64 encoding"))?;
            serde_json::from_slice(&bytes)
                .map_err(|_| token_err("page_token", "invalid follow page token format"))
        })
        .transpose()
}

fn build_follow_page(
    rows:  Vec<FollowRow>,
    limit: i32,
) -> Result<(Vec<FollowEdge>, Option<String>), SocialGraphError> {
    let total = rows.len();
    let mut edges = Vec::with_capacity(total);
    let mut last_followed_at_ms = 0i64;

    for row in &rows {
        last_followed_at_ms = row.followed_at.0;
        let followed_at = Utc
            .timestamp_millis_opt(row.followed_at.0)
            .single()
            .ok_or_else(|| SocialGraphError::DomainViolation {
                field:   "followed_at".to_owned(),
                message: format!("invalid timestamp {}", row.followed_at.0),
            })?;
        edges.push(FollowEdge {
            profile_id:  ProfileId::from_uuid(row.profile_id),
            followed_at,
        });
    }

    let next_token = if total == limit as usize {
        let tok  = FollowPageToken { followed_at_ms: last_followed_at_ms };
        let json = serde_json::to_vec(&tok).unwrap_or_default();
        Some(URL_SAFE_NO_PAD.encode(json))
    } else {
        None
    };

    Ok((edges, next_token))
}
