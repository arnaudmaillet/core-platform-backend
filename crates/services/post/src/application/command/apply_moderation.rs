use std::sync::Arc;

use cqrs::{Command, CommandHandler, Envelope};
use validate_core::{FieldViolation, Validate};

use crate::{
    application::port::PostRepository,
    domain::value_object::{ModerationRestriction, PostId},
    error::PostError,
};

/// Records a moderation outcome on a post: an enforcement applied
/// (`restriction` ≠ None) or reversed (`restriction` = None). Issued by the
/// `moderation.v1.events` consumer, never by a client.
#[derive(Debug, PartialEq, Eq)]
pub struct ApplyModerationCommand {
    pub post_id:     String,
    pub restriction: ModerationRestriction,
    /// Moderation's per-subject `EnforcementVersion`.
    pub version:     i64,
}

impl Command for ApplyModerationCommand {}

impl Validate for ApplyModerationCommand {
    fn validate(&self) -> Result<(), Vec<FieldViolation>> {
        if self.post_id.trim().is_empty() {
            return Err(vec![FieldViolation::new("post_id", "PST-VAL-001", "post_id must not be empty")]);
        }
        Ok(())
    }
}

pub struct ApplyModerationHandler<R> {
    pub repository: Arc<R>,
}

impl<R: PostRepository> CommandHandler<ApplyModerationCommand> for ApplyModerationHandler<R> {
    type Error = PostError;

    /// Idempotent: a post that does not exist (an id moderation knows but post
    /// never stored) or an event no newer than the state held is a no-op.
    async fn handle(&self, envelope: Envelope<ApplyModerationCommand>) -> Result<(), PostError> {
        let cmd     = &envelope.payload;
        let post_id = PostId::try_from(cmd.post_id.as_str())?;

        let Some(mut post) = self.repository.find_by_id(&post_id).await? else {
            tracing::info!(post_id = %cmd.post_id, "moderation outcome for an unknown post; skipped");
            return Ok(());
        };
        if post.apply_moderation(cmd.restriction, cmd.version) {
            self.repository.update_moderation(&post).await?;
        }
        Ok(())
    }
}
