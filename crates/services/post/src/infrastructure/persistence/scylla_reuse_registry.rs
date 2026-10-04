use std::sync::Arc;

use async_trait::async_trait;
use scylla::DeserializeRow;
use scylla::statement::unprepared::Statement;
use scylla_storage::{ScyllaClient, ScyllaStorageError};
use uuid::Uuid;

use crate::application::port::{ReuseDefaults, ReuseRegistry};
use crate::domain::value_object::{AudioId, PostId, ProfileId};
use crate::error::PostError;

fn scylla_err(e: scylla::errors::ExecutionError) -> PostError {
    PostError::Storage(ScyllaStorageError::from(e))
}

fn rows_err(ctx: &'static str, e: impl ToString) -> PostError {
    PostError::DomainViolation { field: ctx.to_owned(), message: e.to_string() }
}

/// ScyllaDB adapter for `post.author_reuse_settings` and `post.audio_origins`.
pub struct ScyllaReuseRegistry {
    client: Arc<ScyllaClient>,
}

impl ScyllaReuseRegistry {
    pub fn new(client: Arc<ScyllaClient>) -> Self {
        Self { client }
    }
}

#[async_trait]
impl ReuseRegistry for ScyllaReuseRegistry {
    async fn defaults(&self, author: &ProfileId) -> Result<ReuseDefaults, PostError> {
        #[derive(DeserializeRow)]
        struct Row {
            allow_remix:       Option<bool>,
            allow_sound_reuse: Option<bool>,
        }
        let stmt = Statement::new(
            "SELECT allow_remix, allow_sound_reuse FROM post.author_reuse_settings WHERE profile_id = ?",
        );
        let row = self
            .client
            .session
            .execute_unpaged(stmt, (author.as_uuid(),))
            .await
            .map_err(scylla_err)?
            .into_rows_result()
            .map_err(|e| rows_err("author_reuse_settings", e))?
            .maybe_first_row::<Row>()
            .map_err(|e| rows_err("author_reuse_settings", e))?;
        Ok(row
            .map(|r| ReuseDefaults {
                allow_remix:       r.allow_remix.unwrap_or(true),
                allow_sound_reuse: r.allow_sound_reuse.unwrap_or(true),
            })
            .unwrap_or_default())
    }

    async fn set_defaults(&self, author: &ProfileId, defaults: ReuseDefaults) -> Result<(), PostError> {
        let stmt = Statement::new(
            "INSERT INTO post.author_reuse_settings (profile_id, allow_remix, allow_sound_reuse) VALUES (?, ?, ?)",
        );
        self.client
            .session
            .execute_unpaged(stmt, (author.as_uuid(), defaults.allow_remix, defaults.allow_sound_reuse))
            .await
            .map_err(scylla_err)?;
        Ok(())
    }

    async fn record_origin(&self, audio: &AudioId, post: &PostId, author: &ProfileId) -> Result<(), PostError> {
        // First writer wins: an original sound belongs to the post that made it.
        let stmt = Statement::new(
            "INSERT INTO post.audio_origins (audio_id, post_id, profile_id) VALUES (?, ?, ?) IF NOT EXISTS",
        );
        self.client
            .session
            .execute_unpaged(stmt, (audio.as_uuid(), post.as_uuid(), author.as_uuid()))
            .await
            .map_err(scylla_err)?;
        Ok(())
    }

    async fn origin(&self, audio: &AudioId) -> Result<Option<(PostId, ProfileId)>, PostError> {
        #[derive(DeserializeRow)]
        struct Row {
            post_id:    Uuid,
            profile_id: Uuid,
        }
        let stmt = Statement::new("SELECT post_id, profile_id FROM post.audio_origins WHERE audio_id = ?");
        let row = self
            .client
            .session
            .execute_unpaged(stmt, (audio.as_uuid(),))
            .await
            .map_err(scylla_err)?
            .into_rows_result()
            .map_err(|e| rows_err("audio_origins", e))?
            .maybe_first_row::<Row>()
            .map_err(|e| rows_err("audio_origins", e))?;
        Ok(row.map(|r| (PostId::from_uuid(r.post_id), ProfileId::from_uuid(r.profile_id))))
    }
}
