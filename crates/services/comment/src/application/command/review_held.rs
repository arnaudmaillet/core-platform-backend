//! The post owner's review of held comments (#669): approve (it shows and is
//! announced) or decline (it goes, silently: it was never announced).

use std::sync::Arc;

use cqrs::{Command, CommandHandler, Envelope};
use validate_core::{FieldViolation, Validate};

use crate::application::port::{CommentEventPublisher, CommentRepository, ReadGate};
use crate::domain::aggregate::{Comment, DeletionStrategy};
use crate::domain::value_object::{CommentId, ProfileId};
use crate::error::CommentError;

/// The post owner approves (`approve`) or declines a held comment.
#[derive(Debug, Clone)]
pub struct ReviewHeldCommentCommand {
    pub comment_id: String,
    /// The reviewing post owner.
    pub owner_id:   String,
    pub approve:    bool,
}

impl Command for ReviewHeldCommentCommand {}

impl Validate for ReviewHeldCommentCommand {
    fn validate(&self) -> Result<(), Vec<FieldViolation>> {
        let mut v = Vec::new();
        if self.comment_id.trim().is_empty() {
            v.push(FieldViolation::new("comment_id", "CMT-VAL-001", "comment_id must not be empty"));
        }
        if self.owner_id.trim().is_empty() {
            v.push(FieldViolation::new("owner_id", "CMT-VAL-003", "owner_id must not be empty"));
        }
        if v.is_empty() { Ok(()) } else { Err(v) }
    }
}

pub struct ReviewHeldCommentHandler<R, P> {
    pub repository: Arc<R>,
    pub publisher:  Arc<P>,
    pub gate:       Arc<dyn ReadGate>,
}

impl<R, P> ReviewHeldCommentHandler<R, P>
where
    R: CommentRepository,
{
    /// The held comment, reviewable by `owner` (the post's author).
    async fn held_for(&self, comment_id: &CommentId, owner: &ProfileId) -> Result<Comment, CommentError> {
        let not_found = || CommentError::CommentNotFound { comment_id: comment_id.as_str() };
        let comment = self.repository.find_by_id(comment_id).await?.filter(Comment::held).ok_or_else(not_found)?;
        let post_owner = self.gate.post_author(comment.post_id()).await?;
        // Not found for anyone but the post's owner, so a held comment's
        // existence does not leak.
        if post_owner.as_ref().map(ProfileId::as_uuid) != Some(owner.as_uuid()) {
            return Err(not_found());
        }
        Ok(comment)
    }
}

impl<R, P> CommandHandler<ReviewHeldCommentCommand> for ReviewHeldCommentHandler<R, P>
where
    R: CommentRepository,
    P: CommentEventPublisher,
{
    type Error = CommentError;

    async fn handle(&self, envelope: Envelope<ReviewHeldCommentCommand>) -> Result<(), CommentError> {
        let cmd = &envelope.payload;
        let comment_id = CommentId::try_from(cmd.comment_id.as_str())?;
        let owner = ProfileId::try_from(cmd.owner_id.as_str())?;
        let mut comment = self.held_for(&comment_id, &owner).await?;

        if cmd.approve {
            comment.release()?;
            // Approving does not lift a restriction (#659): still no
            // notifications for a restricted author's comment.
            if self.gate.restricted_by(&owner, comment.author_id()).await? {
                comment.announce_quietly();
            }
            self.repository.set_held(&comment, false).await?;
            for event in comment.take_events() {
                self.publisher.publish(&event).await?;
            }
            return Ok(());
        }

        let has_replies = self.repository.has_active_replies(comment.post_id(), &comment_id).await?;
        match comment.delete(has_replies)? {
            DeletionStrategy::Tombstone => self.repository.soft_delete(&comment).await?,
            DeletionStrategy::Purge => self.repository.purge(&comment).await?,
        }
        // Never announced, so its deletion is not either.
        comment.take_events();
        Ok(())
    }
}
