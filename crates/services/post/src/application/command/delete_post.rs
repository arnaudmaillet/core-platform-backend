use std::sync::Arc;

use cqrs::{Command, CommandHandler, Envelope};
use validate_core::{FieldViolation, Validate};

use crate::{
    application::port::{EventPublisher, PostRepository, RecentlyDeleted},
    domain::value_object::{PostId, ProfileId},
    error::PostError,
};

pub struct DeletePostCommand {
    pub post_id:    String,
    pub profile_id: String,
}

impl Command for DeletePostCommand {}

impl Validate for DeletePostCommand {
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

pub struct DeletePostHandler<R, P> {
    pub repository:       Arc<R>,
    pub publisher:        Arc<P>,
    /// "Recently deleted" (#663): the post can be restored for 30 days.
    pub recently_deleted: Arc<dyn RecentlyDeleted>,
}

impl<R, P> CommandHandler<DeletePostCommand> for DeletePostHandler<R, P>
where
    R: PostRepository,
    P: EventPublisher,
{
    type Error = PostError;

    async fn handle(&self, envelope: Envelope<DeletePostCommand>) -> Result<(), PostError> {
        let cmd = &envelope.payload;

        let post_id    = PostId::try_from(cmd.post_id.as_str())?;
        let profile_id = ProfileId::try_from(cmd.profile_id.as_str())?;

        let mut post = self.repository.find_by_id(&post_id).await?
            .ok_or_else(|| PostError::PostNotFound { post_id: post_id.as_str() })?;

        if post.profile_id().as_uuid() != profile_id.as_uuid() {
            return Err(PostError::AuthorMismatch {
                post_id:   post_id.as_str(),
                caller_id: profile_id.as_str(),
            });
        }

        let deleted_at = post.delete()?;
        // Indexed first: a listed entry whose post is not deleted is dropped on
        // read, while a deleted post missing from the list could not be restored.
        self.recently_deleted.add(&profile_id, deleted_at, &post_id).await?;
        self.repository.update_lifecycle(&post).await?;

        for event in post.take_events() {
            self.publisher.publish(&event).await?;
        }
        Ok(())
    }
}
