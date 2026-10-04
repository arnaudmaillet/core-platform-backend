use std::sync::Arc;

use chrono::Utc;
use cqrs::{Command, CommandHandler, Envelope};
use validate_core::{FieldViolation, Validate};

use crate::{
    application::port::{AuthorTierStore, EventPublisher, PostRepository, RecentlyDeleted},
    domain::{event::DomainEvent, value_object::{PostId, ProfileId}},
    error::PostError,
};

/// The author brings a deleted post back within 30 days (#663): published
/// again (announced at its original publication time) or a draft again.
pub struct RestorePostCommand {
    pub post_id:    String,
    pub profile_id: String,
}

impl Command for RestorePostCommand {}

impl Validate for RestorePostCommand {
    fn validate(&self) -> Result<(), Vec<FieldViolation>> {
        let mut v = Vec::new();
        if self.post_id.trim().is_empty() {
            v.push(FieldViolation::new("post_id", "PST-VAL-001", "post_id must not be empty"));
        }
        if self.profile_id.trim().is_empty() {
            v.push(FieldViolation::new("profile_id", "PST-VAL-002", "profile_id must not be empty"));
        }
        if v.is_empty() { Ok(()) } else { Err(v) }
    }
}

pub struct RestorePostHandler<R, P> {
    pub repository:        Arc<R>,
    pub publisher:         Arc<P>,
    pub author_tier_store: Arc<dyn AuthorTierStore>,
    pub recently_deleted:  Arc<dyn RecentlyDeleted>,
}

impl<R, P> CommandHandler<RestorePostCommand> for RestorePostHandler<R, P>
where
    R: PostRepository,
    P: EventPublisher,
{
    type Error = PostError;

    async fn handle(&self, envelope: Envelope<RestorePostCommand>) -> Result<(), PostError> {
        let cmd = &envelope.payload;
        let post_id    = PostId::try_from(cmd.post_id.as_str())?;
        let profile_id = ProfileId::try_from(cmd.profile_id.as_str())?;

        let mut post = self.repository.find_by_id(&post_id).await?
            .ok_or_else(|| PostError::PostNotFound { post_id: post_id.as_str() })?;
        if post.profile_id().as_uuid() != profile_id.as_uuid() {
            return Err(PostError::AuthorMismatch { post_id: post_id.as_str(), caller_id: profile_id.as_str() });
        }

        let deleted_at = post.deleted_at();
        post.restore(Utc::now())?;
        self.repository.update_lifecycle(&post).await?;
        if let Some(at) = deleted_at {
            // Best effort: a stale entry is dropped when the list is read.
            if let Err(error) = self.recently_deleted.remove(&profile_id, at, &post_id).await {
                tracing::warn!(%error, "recently-deleted index entry not removed on restore");
            }
        }

        // As on publish: stamp the author's current tier (Standard if unreadable).
        let author_tier = self.author_tier_store.get_tier(&profile_id).await.unwrap_or(0);
        for mut event in post.take_events() {
            if let DomainEvent::PostPublished(ref mut e) = event {
                e.author_tier = author_tier;
            }
            self.publisher.publish(&event).await?;
        }
        Ok(())
    }
}
