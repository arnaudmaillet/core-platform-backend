use std::sync::Arc;

use cqrs::{Command, CommandHandler, Envelope};
use validate_core::{FieldViolation, Validate};

use crate::{
    application::{
        command::create_post::{AttachmentInput, parse_attachments},
        port::{check_mentions, AudienceGate, EventPublisher, PostRepository},
    },
    domain::value_object::{Caption, PostId, ProfileId},
    error::PostError,
};

pub struct UpdatePostCommand {
    pub post_id:     String,
    pub profile_id:  String,
    pub caption:     String,
    pub attachments: Vec<AttachmentInput>,
}

impl Command for UpdatePostCommand {}

impl Validate for UpdatePostCommand {
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

pub struct UpdatePostHandler<R, P> {
    pub repository: Arc<R>,
    pub publisher:  Arc<P>,
    /// Who takes mentions from whom (#656).
    pub audience:   Arc<dyn AudienceGate>,
}

impl<R, P> CommandHandler<UpdatePostCommand> for UpdatePostHandler<R, P>
where
    R: PostRepository,
    P: EventPublisher,
{
    type Error = PostError;

    async fn handle(&self, envelope: Envelope<UpdatePostCommand>) -> Result<(), PostError> {
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

        let caption     = Caption::new(&cmd.caption)?;
        let attachments = parse_attachments(&cmd.attachments)?;
        // Every mentioned profile must take mentions from the author (#656).
        check_mentions(self.audience.as_ref(), &profile_id, &caption).await?;

        post.update(caption, attachments)?;
        self.repository.update_content(&post).await?;

        for event in post.take_events() {
            self.publisher.publish(&event).await?;
        }
        Ok(())
    }
}
