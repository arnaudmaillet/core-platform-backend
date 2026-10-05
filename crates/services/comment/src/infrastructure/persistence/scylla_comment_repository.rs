use std::sync::Arc;

use async_trait::async_trait;
use base64::{engine::general_purpose::URL_SAFE_NO_PAD, Engine as _};
use chrono::{DateTime, TimeZone, Utc};
use scylla::observability::history::HistoryListener;
use scylla::response::PagingState;
use scylla::statement::unprepared::Statement;
use scylla::value::CqlTimestamp;
use scylla_storage::{ProfileKind as ScyllaProfileKind, ScyllaClient, ScyllaStorageError};
use uuid::Uuid;

use crate::application::port::{CommentRepository, CommentSummary};
use crate::domain::aggregate::Comment;
use crate::domain::entity::GifAttachment;
use crate::domain::value_object::{CommentBody, CommentId, CommentStatus, PostId, ProfileId};
use crate::error::CommentError;
use crate::infrastructure::persistence::model::{CommentFeedRow, CommentRow};

/// Sentinel UUID stored in `comments_by_post.parent_id` for top-level comments.
/// Using nil UUID ensures top-level rows sort before all reply slots and allows
/// valid clustering-key prefix scans without ALLOW FILTERING.
const NIL_UUID: Uuid = Uuid::nil();

// ── Page-token ────────────────────────────────────────────────────────────────

#[derive(serde::Serialize, serde::Deserialize)]
struct FeedPageToken {
    created_at_ms: i64,
}

// ── Error helpers ─────────────────────────────────────────────────────────────

fn scylla_err(e: scylla::errors::ExecutionError) -> CommentError {
    CommentError::Storage(ScyllaStorageError::from(e))
}

fn row_err(ctx: &'static str, e: impl ToString) -> CommentError {
    CommentError::DomainViolation {
        field:   ctx.to_owned(),
        message: e.to_string(),
    }
}

fn token_err(msg: &'static str) -> CommentError {
    CommentError::DomainViolation {
        field:   "page_token".to_owned(),
        message: msg.to_owned(),
    }
}

fn ms_to_dt(ms: i64, ctx: &'static str) -> Result<DateTime<Utc>, CommentError> {
    Utc.timestamp_millis_opt(ms)
        .single()
        .ok_or_else(|| CommentError::DomainViolation {
            field:   ctx.to_owned(),
            message: format!("invalid millisecond timestamp: {ms}"),
        })
}

fn dt_ms(dt: DateTime<Utc>) -> CqlTimestamp {
    CqlTimestamp(dt.timestamp_millis())
}

// ── Row → domain ──────────────────────────────────────────────────────────────

fn row_to_comment(row: CommentRow) -> Result<Comment, CommentError> {
    let status  = CommentStatus::try_from(row.status)?;
    let body    = row.body.filter(|s| !s.is_empty()).map(CommentBody::new).transpose()?;
    let gif     = build_gif(row.gif_id, row.gif_url, row.gif_width, row.gif_height);
    let parent  = if row.parent_id == NIL_UUID { None } else { Some(CommentId::from_uuid(row.parent_id)) };

    let created_at = ms_to_dt(row.created_at.0, "created_at")?;
    let updated_at = ms_to_dt(row.updated_at.0, "updated_at")?;
    let deleted_at = row.deleted_at.map(|t| ms_to_dt(t.0, "deleted_at")).transpose()?;

    Ok(Comment::reconstitute(
        CommentId::from_uuid(row.comment_id),
        PostId::from_uuid(row.post_id),
        ProfileId::from_uuid(row.author_id),
        parent,
        status,
        body,
        gif,
        created_at,
        updated_at,
        deleted_at,
    ).with_held(row.held.unwrap_or(false)))
}

fn feed_row_to_summary(row: CommentFeedRow) -> Result<CommentSummary, CommentError> {
    let status     = CommentStatus::try_from(row.status)?;
    let created_at = ms_to_dt(row.created_at.0, "created_at")?;
    Ok(CommentSummary {
        comment_id: CommentId::from_uuid(row.comment_id),
        author_id:  ProfileId::from_uuid(row.author_id),
        status,
        body:       row.body,
        gif_url:    row.gif_url,
        gif_width:  row.gif_width.map(|w| w as u32),
        gif_height: row.gif_height.map(|h| h as u32),
        created_at,
        held:       row.held.unwrap_or(false),
    })
}

fn build_gif(
    gif_id:     Option<String>,
    gif_url:    Option<String>,
    gif_width:  Option<i32>,
    gif_height: Option<i32>,
) -> Option<GifAttachment> {
    match (gif_id, gif_url, gif_width, gif_height) {
        (Some(id), Some(url), Some(w), Some(h)) if !id.is_empty() && !url.is_empty() => {
            Some(GifAttachment {
                gif_id:     id,
                gif_url:    url,
                gif_width:  w as u32,
                gif_height: h as u32,
            })
        }
        _ => None,
    }
}

fn decode_page_token(page_token: Option<&str>) -> Result<Option<FeedPageToken>, CommentError> {
    page_token
        .map(|t| {
            let bytes = URL_SAFE_NO_PAD
                .decode(t)
                .map_err(|_| token_err("invalid base64 encoding"))?;
            serde_json::from_slice(&bytes)
                .map_err(|_| token_err("invalid page token format"))
        })
        .transpose()
}

fn encode_page_token(created_at_ms: i64) -> String {
    let tok  = FeedPageToken { created_at_ms };
    let json = serde_json::to_vec(&tok).unwrap_or_default();
    URL_SAFE_NO_PAD.encode(json)
}

/// Rows per page when scanning `comment.comments` to backfill the author index.
const BACKFILL_PAGE_SIZE: i32 = 500;

/// `(created_at_ms, comment_id)` of the last comment of a by-author page.
#[derive(serde::Serialize, serde::Deserialize)]
struct AuthorCursor {
    created_at_ms: i64,
    comment_id:    Uuid,
}

fn decode_author_cursor(page_token: Option<&str>) -> Result<Option<(i64, Uuid)>, CommentError> {
    page_token
        .map(|t| {
            let bytes = URL_SAFE_NO_PAD.decode(t).map_err(|_| token_err("invalid base64 encoding"))?;
            let cursor: AuthorCursor =
                serde_json::from_slice(&bytes).map_err(|_| token_err("invalid page token format"))?;
            Ok((cursor.created_at_ms, cursor.comment_id))
        })
        .transpose()
}

fn encode_author_cursor(created_at_ms: i64, comment_id: Uuid) -> String {
    URL_SAFE_NO_PAD.encode(serde_json::to_vec(&AuthorCursor { created_at_ms, comment_id }).unwrap_or_default())
}

// ── Repository ────────────────────────────────────────────────────────────────

pub struct ScyllaCommentRepository {
    client: Arc<ScyllaClient>,
}

impl ScyllaCommentRepository {
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

    /// One `comments_by_author` row (an upsert: idempotent).
    async fn index_by_author(
        &self,
        author_id:     Uuid,
        created_at_ms: i64,
        comment_id:    Uuid,
        post_id:       Uuid,
    ) -> Result<(), CommentError> {
        let stmt = self.strict_stmt(
            "INSERT INTO comment.comments_by_author (author_id, created_at, comment_id, post_id) VALUES (?, ?, ?, ?)",
        );
        self.client
            .session
            .execute_unpaged(stmt, (author_id, CqlTimestamp(created_at_ms), comment_id, post_id))
            .await
            .map_err(scylla_err)?;
        Ok(())
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
impl CommentRepository for ScyllaCommentRepository {
    // ── insert ────────────────────────────────────────────────────────────────

    async fn insert(&self, comment: &Comment) -> Result<(), CommentError> {
        let parent_uuid = comment
            .parent_id()
            .map(CommentId::as_uuid)
            .unwrap_or(NIL_UUID);

        let gif_id     = comment.gif().map(|g| g.gif_id.as_str());
        let gif_url    = comment.gif().map(|g| g.gif_url.as_str());
        let gif_width  = comment.gif().map(|g| g.gif_width as i32);
        let gif_height = comment.gif().map(|g| g.gif_height as i32);

        // Write to source-of-truth table first.
        let stmt_main = self.strict_stmt(
            "INSERT INTO comment.comments \
             (comment_id, post_id, author_id, parent_id, status, body, \
              gif_id, gif_url, gif_width, gif_height, created_at, updated_at, deleted_at) \
             VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        );
        self.client
            .session
            .execute_unpaged(
                stmt_main,
                (
                    comment.id().as_uuid(),
                    comment.post_id().as_uuid(),
                    comment.author_id().as_uuid(),
                    parent_uuid,
                    comment.status().as_tinyint(),
                    comment.body().map(CommentBody::as_str),
                    gif_id,
                    gif_url,
                    gif_width,
                    gif_height,
                    dt_ms(comment.created_at()),
                    dt_ms(comment.updated_at()),
                    comment.deleted_at().map(dt_ms),
                ),
            )
            .await
            .map_err(scylla_err)?;

        // Write to feed table.
        let stmt_feed = self.strict_stmt(
            "INSERT INTO comment.comments_by_post \
             (post_id, parent_id, created_at, comment_id, author_id, status, \
              body, gif_url, gif_width, gif_height) \
             VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        );
        self.client
            .session
            .execute_unpaged(
                stmt_feed,
                (
                    comment.post_id().as_uuid(),
                    parent_uuid,
                    dt_ms(comment.created_at()),
                    comment.id().as_uuid(),
                    comment.author_id().as_uuid(),
                    comment.status().as_tinyint(),
                    comment.body().map(CommentBody::as_str),
                    gif_url,
                    gif_width,
                    gif_height,
                ),
            )
            .await
            .map_err(scylla_err)?;

        self.index_by_author(
            comment.author_id().as_uuid(),
            comment.created_at().timestamp_millis(),
            comment.id().as_uuid(),
            comment.post_id().as_uuid(),
        )
        .await?;

        if comment.held() {
            self.set_held(comment, true).await?;
        }

        Ok(())
    }

    // ── find_by_id ────────────────────────────────────────────────────────────

    async fn find_by_id(&self, id: &CommentId) -> Result<Option<Comment>, CommentError> {
        let stmt = self.fast_stmt(
            "SELECT comment_id, post_id, author_id, parent_id, status, body, \
             gif_id, gif_url, gif_width, gif_height, created_at, updated_at, deleted_at, held \
             FROM comment.comments WHERE comment_id = ?",
        );
        let result = self
            .client
            .session
            .execute_unpaged(stmt, (id.as_uuid(),))
            .await
            .map_err(scylla_err)?;

        let row = result
            .into_rows_result()
            .map_err(|e| row_err("find_by_id:rows", e))?
            .maybe_first_row::<CommentRow>()
            .map_err(|e| row_err("find_by_id:deser", e))?;

        row.map(row_to_comment).transpose()
    }

    // ── has_active_replies ────────────────────────────────────────────────────

    async fn has_active_replies(
        &self,
        post_id:    &PostId,
        comment_id: &CommentId,
    ) -> Result<bool, CommentError> {
        let stmt = self.fast_stmt(
            "SELECT comment_id FROM comment.comments_by_post \
             WHERE post_id = ? AND parent_id = ? LIMIT 1",
        );
        let result = self
            .client
            .session
            .execute_unpaged(stmt, (post_id.as_uuid(), comment_id.as_uuid()))
            .await
            .map_err(scylla_err)?;

        let rows = result
            .into_rows_result()
            .map_err(|e| row_err("has_active_replies:rows", e))?;

        Ok(rows.rows_num() > 0)
    }

    // ── soft_delete ───────────────────────────────────────────────────────────

    async fn soft_delete(&self, comment: &Comment) -> Result<(), CommentError> {
        let parent_uuid = comment
            .parent_id()
            .map(CommentId::as_uuid)
            .unwrap_or(NIL_UUID);

        let deleted_at = comment.deleted_at().map(dt_ms);
        let updated_at = dt_ms(comment.updated_at());
        let status     = comment.status().as_tinyint();

        // Null all content in source-of-truth.
        let stmt_main = self.strict_stmt(
            "UPDATE comment.comments SET \
             status = ?, body = null, gif_id = null, gif_url = null, \
             gif_width = null, gif_height = null, \
             updated_at = ?, deleted_at = ? \
             WHERE comment_id = ?",
        );
        self.client
            .session
            .execute_unpaged(
                stmt_main,
                (status, updated_at, deleted_at, comment.id().as_uuid()),
            )
            .await
            .map_err(scylla_err)?;

        // Null content in feed table.
        let stmt_feed = self.strict_stmt(
            "UPDATE comment.comments_by_post SET \
             status = ?, body = null, gif_url = null, gif_width = null, gif_height = null \
             WHERE post_id = ? AND parent_id = ? AND created_at = ? AND comment_id = ?",
        );
        self.client
            .session
            .execute_unpaged(
                stmt_feed,
                (
                    status,
                    comment.post_id().as_uuid(),
                    parent_uuid,
                    dt_ms(comment.created_at()),
                    comment.id().as_uuid(),
                ),
            )
            .await
            .map_err(scylla_err)?;

        Ok(())
    }

    // ── set_held ──────────────────────────────────────────────────────────────

    async fn set_held(&self, comment: &Comment, held: bool) -> Result<(), CommentError> {
        let parent_uuid = comment.parent_id().map(CommentId::as_uuid).unwrap_or(NIL_UUID);
        let stmt_main = self.strict_stmt("UPDATE comment.comments SET held = ? WHERE comment_id = ?");
        self.client
            .session
            .execute_unpaged(stmt_main, (held, comment.id().as_uuid()))
            .await
            .map_err(scylla_err)?;
        let stmt_feed = self.strict_stmt(
            "UPDATE comment.comments_by_post SET held = ? \
             WHERE post_id = ? AND parent_id = ? AND created_at = ? AND comment_id = ?",
        );
        self.client
            .session
            .execute_unpaged(
                stmt_feed,
                (held, comment.post_id().as_uuid(), parent_uuid, dt_ms(comment.created_at()), comment.id().as_uuid()),
            )
            .await
            .map_err(scylla_err)?;
        Ok(())
    }

    // ── purge ─────────────────────────────────────────────────────────────────

    async fn purge(&self, comment: &Comment) -> Result<(), CommentError> {
        let parent_uuid = comment
            .parent_id()
            .map(CommentId::as_uuid)
            .unwrap_or(NIL_UUID);

        let stmt_main = self.strict_stmt(
            "DELETE FROM comment.comments WHERE comment_id = ?",
        );
        self.client
            .session
            .execute_unpaged(stmt_main, (comment.id().as_uuid(),))
            .await
            .map_err(scylla_err)?;

        let stmt_author = self.strict_stmt(
            "DELETE FROM comment.comments_by_author WHERE author_id = ? AND created_at = ? AND comment_id = ?",
        );
        self.client
            .session
            .execute_unpaged(
                stmt_author,
                (comment.author_id().as_uuid(), dt_ms(comment.created_at()), comment.id().as_uuid()),
            )
            .await
            .map_err(scylla_err)?;

        let stmt_feed = self.strict_stmt(
            "DELETE FROM comment.comments_by_post \
             WHERE post_id = ? AND parent_id = ? AND created_at = ? AND comment_id = ?",
        );
        self.client
            .session
            .execute_unpaged(
                stmt_feed,
                (
                    comment.post_id().as_uuid(),
                    parent_uuid,
                    dt_ms(comment.created_at()),
                    comment.id().as_uuid(),
                ),
            )
            .await
            .map_err(scylla_err)?;

        Ok(())
    }

    // ── list_by_author ────────────────────────────────────────────────────────

    async fn list_by_author(
        &self,
        author_id:  &ProfileId,
        limit:      i32,
        page_token: Option<&str>,
    ) -> Result<(Vec<Comment>, Option<String>), CommentError> {
        let limit = limit.clamp(1, 100);
        let cursor = decode_author_cursor(page_token)?;
        let result = match cursor {
            Some((created_at_ms, comment_id)) => {
                let stmt = self.fast_stmt(
                    "SELECT created_at, comment_id FROM comment.comments_by_author \
                     WHERE author_id = ? AND (created_at, comment_id) < (?, ?) LIMIT ?",
                );
                self.client
                    .session
                    .execute_unpaged(stmt, (author_id.as_uuid(), CqlTimestamp(created_at_ms), comment_id, limit))
                    .await
            }
            None => {
                let stmt = self.fast_stmt(
                    "SELECT created_at, comment_id FROM comment.comments_by_author WHERE author_id = ? LIMIT ?",
                );
                self.client.session.execute_unpaged(stmt, (author_id.as_uuid(), limit)).await
            }
        }
        .map_err(scylla_err)?;
        let pointers = result
            .into_rows_result()
            .map_err(|e| row_err("list_by_author:rows", e))?
            .rows::<(CqlTimestamp, Uuid)>()
            .map_err(|e| row_err("list_by_author:iter", e))?
            .collect::<Result<Vec<_>, _>>()
            .map_err(|e| row_err("list_by_author:deser", e))?;

        let next = (pointers.len() == limit as usize)
            .then(|| pointers.last().map(|(at, id)| encode_author_cursor(at.0, *id)))
            .flatten();
        let mut comments = Vec::with_capacity(pointers.len());
        for (_, comment_id) in pointers {
            // A pointer whose comment was purged meanwhile is skipped.
            if let Some(comment) = self.find_by_id(&CommentId::from_uuid(comment_id)).await? {
                comments.push(comment);
            }
        }
        Ok((comments, next))
    }

    async fn backfill_author_index(&self) -> Result<u64, CommentError> {
        let mut stmt = self.fast_stmt("SELECT author_id, created_at, comment_id, post_id FROM comment.comments");
        stmt.set_page_size(BACKFILL_PAGE_SIZE);
        let mut paging = PagingState::start();
        let mut written = 0u64;
        loop {
            let (result, next) = self
                .client
                .session
                .execute_single_page(stmt.clone(), (), paging)
                .await
                .map_err(scylla_err)?;
            let rows = result
                .into_rows_result()
                .map_err(|e| row_err("backfill_author_index:rows", e))?
                .rows::<(Uuid, CqlTimestamp, Uuid, Uuid)>()
                .map_err(|e| row_err("backfill_author_index:iter", e))?
                .collect::<Result<Vec<_>, _>>()
                .map_err(|e| row_err("backfill_author_index:deser", e))?;
            for (author_id, created_at, comment_id, post_id) in rows {
                self.index_by_author(author_id, created_at.0, comment_id, post_id).await?;
                written += 1;
            }
            match next.into_paging_control_flow() {
                std::ops::ControlFlow::Continue(state) => paging = state,
                std::ops::ControlFlow::Break(()) => return Ok(written),
            }
        }
    }

    // ── list_top_level ────────────────────────────────────────────────────────

    async fn list_top_level(
        &self,
        post_id:    &PostId,
        limit:      i32,
        page_token: Option<&str>,
    ) -> Result<(Vec<CommentSummary>, Option<String>), CommentError> {
        // i32, NOT i64: CQL `LIMIT ?` type-checks against int32 and the i64 bind
        // fails serialization — the soak's social-graph finding (#17), same class,
        // never swept here (these list paths had no live traffic until the suites
        // ran in CI).
        let limit = limit.clamp(1, 100);
        let token = decode_page_token(page_token)?;

        let rows: Vec<CommentFeedRow> = if let Some(ref tok) = token {
            let stmt = self.fast_stmt(
                "SELECT created_at, comment_id, author_id, status, body, gif_url, gif_width, gif_height, held \
                 FROM comment.comments_by_post \
                 WHERE post_id = ? AND parent_id = ? AND created_at < ? \
                 LIMIT ?",
            );
            self.client
                .session
                .execute_unpaged(
                    stmt,
                    (post_id.as_uuid(), NIL_UUID, CqlTimestamp(tok.created_at_ms), limit),
                )
                .await
                .map_err(scylla_err)?
                .into_rows_result()
                .map_err(|e| row_err("list_top_level:rows", e))?
                .rows::<CommentFeedRow>()
                .map_err(|e| row_err("list_top_level:iter", e))?
                .collect::<Result<Vec<_>, _>>()
                .map_err(|e| row_err("list_top_level:deser", e))?
        } else {
            let stmt = self.fast_stmt(
                "SELECT created_at, comment_id, author_id, status, body, gif_url, gif_width, gif_height, held \
                 FROM comment.comments_by_post \
                 WHERE post_id = ? AND parent_id = ? \
                 LIMIT ?",
            );
            self.client
                .session
                .execute_unpaged(stmt, (post_id.as_uuid(), NIL_UUID, limit))
                .await
                .map_err(scylla_err)?
                .into_rows_result()
                .map_err(|e| row_err("list_top_level:rows", e))?
                .rows::<CommentFeedRow>()
                .map_err(|e| row_err("list_top_level:iter", e))?
                .collect::<Result<Vec<_>, _>>()
                .map_err(|e| row_err("list_top_level:deser", e))?
        };

        build_page(rows, limit as usize)
    }

    // ── list_replies ──────────────────────────────────────────────────────────

    async fn list_replies(
        &self,
        post_id:    &PostId,
        comment_id: &CommentId,
        limit:      i32,
        page_token: Option<&str>,
    ) -> Result<(Vec<CommentSummary>, Option<String>), CommentError> {
        // i32 for the CQL int32 LIMIT — same fix as list_top_level above.
        let limit  = limit.clamp(1, 100);
        let token  = decode_page_token(page_token)?;
        let parent = comment_id.as_uuid();

        let rows: Vec<CommentFeedRow> = if let Some(ref tok) = token {
            let stmt = self.fast_stmt(
                "SELECT created_at, comment_id, author_id, status, body, gif_url, gif_width, gif_height, held \
                 FROM comment.comments_by_post \
                 WHERE post_id = ? AND parent_id = ? AND created_at < ? \
                 LIMIT ?",
            );
            self.client
                .session
                .execute_unpaged(
                    stmt,
                    (post_id.as_uuid(), parent, CqlTimestamp(tok.created_at_ms), limit),
                )
                .await
                .map_err(scylla_err)?
                .into_rows_result()
                .map_err(|e| row_err("list_replies:rows", e))?
                .rows::<CommentFeedRow>()
                .map_err(|e| row_err("list_replies:iter", e))?
                .collect::<Result<Vec<_>, _>>()
                .map_err(|e| row_err("list_replies:deser", e))?
        } else {
            let stmt = self.fast_stmt(
                "SELECT created_at, comment_id, author_id, status, body, gif_url, gif_width, gif_height, held \
                 FROM comment.comments_by_post \
                 WHERE post_id = ? AND parent_id = ? \
                 LIMIT ?",
            );
            self.client
                .session
                .execute_unpaged(stmt, (post_id.as_uuid(), parent, limit))
                .await
                .map_err(scylla_err)?
                .into_rows_result()
                .map_err(|e| row_err("list_replies:rows", e))?
                .rows::<CommentFeedRow>()
                .map_err(|e| row_err("list_replies:iter", e))?
                .collect::<Result<Vec<_>, _>>()
                .map_err(|e| row_err("list_replies:deser", e))?
        };

        build_page(rows, limit as usize)
    }
}

fn build_page(
    rows:  Vec<CommentFeedRow>,
    limit: usize,
) -> Result<(Vec<CommentSummary>, Option<String>), CommentError> {
    let total = rows.len();
    let mut summaries = Vec::with_capacity(total);
    let mut last_ms   = 0i64;

    for row in rows {
        last_ms = row.created_at.0;
        summaries.push(feed_row_to_summary(row)?);
    }

    let next_token = if total == limit {
        Some(encode_page_token(last_ms))
    } else {
        None
    };

    Ok((summaries, next_token))
}
