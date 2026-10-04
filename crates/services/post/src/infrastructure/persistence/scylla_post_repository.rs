use std::sync::Arc;

use async_trait::async_trait;
use base64::{engine::general_purpose::URL_SAFE_NO_PAD, Engine as _};
use chrono::{DateTime, TimeZone, Utc};
use scylla::observability::history::HistoryListener;
use scylla::statement::unprepared::Statement;
use scylla::value::CqlTimestamp;
use scylla_storage::{ProfileKind as ScyllaProfileKind, ScyllaClient, ScyllaStorageError};

use crate::application::port::{PostRepository, PostSummary};
use crate::domain::aggregate::Post;
use crate::domain::entity::MediaAttachment;
use crate::domain::value_object::{
    AudioId, AudioKind, AudioReference, Caption, GeoPoint, ModerationRestriction, ModerationState,
    PostId, PostKind, PostStatus, ProfileId,
};
use crate::error::PostError;
use crate::infrastructure::persistence::model::{PostProfileRow, PostRow};

// ── Page-token ────────────────────────────────────────────────────────────────

#[derive(serde::Serialize, serde::Deserialize)]
struct ProfilePageToken {
    created_at_ms: i64,
}

// ── Error helpers ─────────────────────────────────────────────────────────────

fn scylla_err(e: scylla::errors::ExecutionError) -> PostError {
    PostError::Storage(ScyllaStorageError::from(e))
}

fn row_err(ctx: &'static str, e: impl ToString) -> PostError {
    PostError::DomainViolation {
        field:   ctx.to_owned(),
        message: e.to_string(),
    }
}

fn token_err(field: &'static str, msg: &'static str) -> PostError {
    PostError::DomainViolation {
        field:   field.to_owned(),
        message: msg.to_owned(),
    }
}

// ── Repository ────────────────────────────────────────────────────────────────

pub struct ScyllaPostRepository {
    client: Arc<ScyllaClient>,
}

impl ScyllaPostRepository {
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

    fn dt_ms(dt: DateTime<Utc>) -> CqlTimestamp {
        CqlTimestamp(dt.timestamp_millis())
    }

    fn ms_to_dt(ms: i64, ctx: &'static str) -> Result<DateTime<Utc>, PostError> {
        Utc.timestamp_millis_opt(ms).single().ok_or_else(|| PostError::DomainViolation {
            field:   ctx.to_owned(),
            message: format!("invalid millisecond timestamp: {ms}"),
        })
    }

    fn ser_attachments(post: &Post) -> Result<String, PostError> {
        serde_json::to_string(post.attachments()).map_err(|e| PostError::AttachmentsCorrupted {
            post_id: post.id().as_str(),
            reason:  e.to_string(),
        })
    }

    fn deser_attachments(json: &str, post_id_str: &str) -> Result<Vec<MediaAttachment>, PostError> {
        serde_json::from_str(json).map_err(|e| PostError::AttachmentsCorrupted {
            post_id: post_id_str.to_owned(),
            reason:  e.to_string(),
        })
    }
}

fn row_to_post(row: PostRow) -> Result<Post, PostError> {
    let post_id_str = row.post_id.to_string();
    let kind        = PostKind::try_from(row.kind)?;
    let status      = PostStatus::try_from(row.status)?;
    let caption     = Caption::new(row.caption)?;
    let attachments = ScyllaPostRepository::deser_attachments(&row.attachments, &post_id_str)?;

    attachments.iter().enumerate().try_for_each(|(i, a)| {
        if a.width == 0 || a.height == 0 {
            Err(PostError::AttachmentsCorrupted {
                post_id: post_id_str.clone(),
                reason:  format!("attachment[{i}] has zero dimension"),
            })
        } else {
            Ok(())
        }
    })?;

    let parent_id = row.parent_id.map(PostId::from_uuid);
    let root_id   = row.root_id.map(PostId::from_uuid);

    let audio_ref: Option<AudioReference> = match (row.audio_id, row.audio_kind) {
        (Some(id), Some(kind_byte)) => {
            Some(AudioReference {
                audio_id:   AudioId::from_uuid(id),
                audio_kind: AudioKind::try_from(kind_byte)?,
            })
        }
        _ => None,
    };

    let location: Option<GeoPoint> = match (row.lat, row.lng) {
        (Some(lat), Some(lng)) => Some(GeoPoint::new(lat, lng)?),
        _                      => None,
    };

    let created_at   = ScyllaPostRepository::ms_to_dt(row.created_at.0, "created_at")?;
    let updated_at   = ScyllaPostRepository::ms_to_dt(row.updated_at.0, "updated_at")?;
    let published_at = row.published_at.map(|t| ScyllaPostRepository::ms_to_dt(t.0, "published_at")).transpose()?;
    let deleted_at   = row.deleted_at.map(|t| ScyllaPostRepository::ms_to_dt(t.0, "deleted_at")).transpose()?;
    let moderation   = ModerationState {
        restriction: row.moderation_restriction.map(ModerationRestriction::try_from).transpose()?.unwrap_or_default(),
        version:     row.moderation_version.unwrap_or(0),
    };

    Ok(Post::reconstitute(
        PostId::from_uuid(row.post_id),
        ProfileId::from_uuid(row.profile_id),
        kind,
        status,
        caption,
        attachments,
        parent_id,
        root_id,
        audio_ref,
        location,
        created_at,
        updated_at,
        published_at,
        deleted_at,
        moderation,
    ))
}

fn profile_row_to_summary(row: PostProfileRow) -> Result<PostSummary, PostError> {
    let kind       = PostKind::try_from(row.kind)?;
    let status     = PostStatus::try_from(row.status)?;
    let moderation = row.moderation_restriction.map(ModerationRestriction::try_from).transpose()?.unwrap_or_default();
    let created_at = ScyllaPostRepository::ms_to_dt(row.created_at.0, "created_at")?;
    Ok(PostSummary {
        post_id: PostId::from_uuid(row.post_id),
        kind,
        status,
        moderation,
        created_at,
    })
}

fn decode_profile_token(page_token: Option<&str>) -> Result<Option<ProfilePageToken>, PostError> {
    page_token
        .map(|t| {
            let bytes = URL_SAFE_NO_PAD
                .decode(t)
                .map_err(|_| token_err("page_token", "invalid base64 encoding"))?;
            serde_json::from_slice(&bytes)
                .map_err(|_| token_err("page_token", "invalid profile page token format"))
        })
        .transpose()
}

#[async_trait]
impl PostRepository for ScyllaPostRepository {
    // ── insert ────────────────────────────────────────────────────────────────

    async fn insert(&self, post: &Post) -> Result<(), PostError> {
        let attachments_json = Self::ser_attachments(post)?;

        let stmt_posts = self.strict_stmt(
            "INSERT INTO post.posts \
             (post_id, profile_id, kind, status, caption, attachments, \
              parent_id, root_id, created_at, updated_at, published_at, deleted_at, \
              audio_id, audio_kind, lat, lng) \
             VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        );
        self.client
            .session
            .execute_unpaged(
                stmt_posts,
                (
                    post.id().as_uuid(),
                    post.profile_id().as_uuid(),
                    post.kind().as_tinyint(),
                    post.status().as_tinyint(),
                    post.caption().as_str(),
                    attachments_json.as_str(),
                    post.parent_id().map(PostId::as_uuid),
                    post.root_id().map(PostId::as_uuid),
                    Self::dt_ms(post.created_at()),
                    Self::dt_ms(post.updated_at()),
                    post.published_at().map(Self::dt_ms),
                    post.deleted_at().map(Self::dt_ms),
                    post.audio_ref().map(|a| a.audio_id.as_uuid()),
                    post.audio_ref().map(|a| a.audio_kind.as_tinyint()),
                    post.location().map(|g| g.lat()),
                    post.location().map(|g| g.lng()),
                ),
            )
            .await
            .map_err(scylla_err)?;

        let stmt_index = self.strict_stmt(
            "INSERT INTO post.posts_by_profile \
             (profile_id, created_at, post_id, kind, status) \
             VALUES (?, ?, ?, ?, ?)",
        );
        self.client
            .session
            .execute_unpaged(
                stmt_index,
                (
                    post.profile_id().as_uuid(),
                    Self::dt_ms(post.created_at()),
                    post.id().as_uuid(),
                    post.kind().as_tinyint(),
                    post.status().as_tinyint(),
                ),
            )
            .await
            .map_err(scylla_err)?;

        Ok(())
    }

    // ── update_content ────────────────────────────────────────────────────────

    async fn update_content(&self, post: &Post) -> Result<(), PostError> {
        let attachments_json = Self::ser_attachments(post)?;

        let stmt = self.strict_stmt(
            "UPDATE post.posts SET caption = ?, attachments = ?, updated_at = ? \
             WHERE post_id = ?",
        );
        self.client
            .session
            .execute_unpaged(
                stmt,
                (
                    post.caption().as_str(),
                    attachments_json.as_str(),
                    Self::dt_ms(post.updated_at()),
                    post.id().as_uuid(),
                ),
            )
            .await
            .map_err(scylla_err)?;

        Ok(())
    }

    // ── update_lifecycle ──────────────────────────────────────────────────────

    async fn update_lifecycle(&self, post: &Post) -> Result<(), PostError> {
        let stmt_posts = self.strict_stmt(
            "UPDATE post.posts \
             SET status = ?, updated_at = ?, published_at = ?, deleted_at = ? \
             WHERE post_id = ?",
        );
        self.client
            .session
            .execute_unpaged(
                stmt_posts,
                (
                    post.status().as_tinyint(),
                    Self::dt_ms(post.updated_at()),
                    post.published_at().map(Self::dt_ms),
                    post.deleted_at().map(Self::dt_ms),
                    post.id().as_uuid(),
                ),
            )
            .await
            .map_err(scylla_err)?;

        let stmt_index = self.strict_stmt(
            "UPDATE post.posts_by_profile SET status = ? \
             WHERE profile_id = ? AND created_at = ? AND post_id = ?",
        );
        self.client
            .session
            .execute_unpaged(
                stmt_index,
                (
                    post.status().as_tinyint(),
                    post.profile_id().as_uuid(),
                    Self::dt_ms(post.created_at()),
                    post.id().as_uuid(),
                ),
            )
            .await
            .map_err(scylla_err)?;

        Ok(())
    }

    // ── update_moderation ─────────────────────────────────────────────────────

    async fn update_moderation(&self, post: &Post) -> Result<(), PostError> {
        let moderation = post.moderation();
        let restriction = moderation.restriction.as_tinyint();

        let stmt_posts = self.strict_stmt(
            "UPDATE post.posts SET moderation_restriction = ?, moderation_version = ? \
             WHERE post_id = ?",
        );
        self.client
            .session
            .execute_unpaged(stmt_posts, (restriction, moderation.version, post.id().as_uuid()))
            .await
            .map_err(scylla_err)?;

        let stmt_index = self.strict_stmt(
            "UPDATE post.posts_by_profile SET moderation_restriction = ?, moderation_version = ? \
             WHERE profile_id = ? AND created_at = ? AND post_id = ?",
        );
        self.client
            .session
            .execute_unpaged(
                stmt_index,
                (
                    restriction,
                    moderation.version,
                    post.profile_id().as_uuid(),
                    Self::dt_ms(post.created_at()),
                    post.id().as_uuid(),
                ),
            )
            .await
            .map_err(scylla_err)?;

        Ok(())
    }

    // ── find_by_id ────────────────────────────────────────────────────────────

    async fn find_by_id(&self, id: &PostId) -> Result<Option<Post>, PostError> {
        let stmt = self.fast_stmt(
            "SELECT post_id, profile_id, kind, status, caption, attachments, \
             parent_id, root_id, created_at, updated_at, published_at, deleted_at, \
             audio_id, audio_kind, lat, lng, moderation_restriction, moderation_version \
             FROM post.posts WHERE post_id = ?",
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
            .maybe_first_row::<PostRow>()
            .map_err(|e| row_err("find_by_id:deser", e))?;

        row.map(row_to_post).transpose()
    }

    // ── list_by_profile ───────────────────────────────────────────────────────

    async fn list_by_profile(
        &self,
        profile_id: &ProfileId,
        limit:      i32,
        page_token: Option<&str>,
    ) -> Result<(Vec<PostSummary>, Option<String>), PostError> {
        // i32, NOT i64: CQL `LIMIT ?` type-checks against int32 and the i64 bind
        // fails serialization — the soak's social-graph finding (#17), same class,
        // never swept here (these list paths had no live traffic until the suites
        // ran in CI).
        let limit = limit.clamp(1, 100);
        let token = decode_profile_token(page_token)?;

        let rows: Vec<PostProfileRow> = if let Some(ref tok) = token {
            let stmt = self.fast_stmt(
                "SELECT created_at, post_id, kind, status, moderation_restriction \
                 FROM post.posts_by_profile \
                 WHERE profile_id = ? AND created_at < ? LIMIT ?",
            );
            self.client
                .session
                .execute_unpaged(
                    stmt,
                    (profile_id.as_uuid(), CqlTimestamp(tok.created_at_ms), limit),
                )
                .await
                .map_err(scylla_err)?
                .into_rows_result()
                .map_err(|e| row_err("list_by_profile:rows", e))?
                .rows::<PostProfileRow>()
                .map_err(|e| row_err("list_by_profile:iter", e))?
                .collect::<Result<Vec<_>, _>>()
                .map_err(|e| row_err("list_by_profile:deser", e))?
        } else {
            let stmt = self.fast_stmt(
                "SELECT created_at, post_id, kind, status, moderation_restriction \
                 FROM post.posts_by_profile \
                 WHERE profile_id = ? LIMIT ?",
            );
            self.client
                .session
                .execute_unpaged(stmt, (profile_id.as_uuid(), limit))
                .await
                .map_err(scylla_err)?
                .into_rows_result()
                .map_err(|e| row_err("list_by_profile:rows", e))?
                .rows::<PostProfileRow>()
                .map_err(|e| row_err("list_by_profile:iter", e))?
                .collect::<Result<Vec<_>, _>>()
                .map_err(|e| row_err("list_by_profile:deser", e))?
        };

        let total = rows.len();
        let mut summaries = Vec::with_capacity(total);
        let mut last_created_at_ms = 0i64;

        for row in rows {
            last_created_at_ms = row.created_at.0;
            summaries.push(profile_row_to_summary(row)?);
        }

        let next_token = if total == limit as usize {
            let tok  = ProfilePageToken { created_at_ms: last_created_at_ms };
            let json = serde_json::to_vec(&tok).unwrap_or_default();
            Some(URL_SAFE_NO_PAD.encode(json))
        } else {
            None
        };

        Ok((summaries, next_token))
    }
}
