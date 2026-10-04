use std::sync::Arc;

use cqrs::{Command, CommandHandler, Envelope};
use validate_core::{FieldViolation, Validate};

use crate::{
    application::port::{CommentAdmission, CommentEventPublisher, CommentRepository, ReadGate},
    domain::{
        aggregate::Comment,
        entity::GifAttachment,
        value_object::{CommentBody, CommentId, CommentStatus, PostId, ProfileId},
    },
    error::CommentError,
};

pub struct CreateCommentCommand {
    pub comment_id: String,
    pub post_id:    String,
    pub author_id:  String,
    /// `None` or empty string means top-level; non-empty means reply.
    pub parent_id:  Option<String>,
    pub body:       Option<String>,
    pub gif_id:     Option<String>,
    pub gif_url:    Option<String>,
    pub gif_width:  Option<u32>,
    pub gif_height: Option<u32>,
}

impl Command for CreateCommentCommand {}

impl Validate for CreateCommentCommand {
    fn validate(&self) -> Result<(), Vec<FieldViolation>> {
        let mut v = Vec::new();
        if self.comment_id.trim().is_empty() {
            v.push(FieldViolation::new("comment_id", "CMT-VAL-001", "comment_id must not be empty"));
        }
        if self.post_id.trim().is_empty() {
            v.push(FieldViolation::new("post_id", "CMT-VAL-002", "post_id must not be empty"));
        }
        if self.author_id.trim().is_empty() {
            v.push(FieldViolation::new("author_id", "CMT-VAL-003", "author_id must not be empty"));
        }
        let has_body = self.body.as_deref().map(|s| !s.trim().is_empty()).unwrap_or(false);
        let has_gif  = self.gif_url.as_deref().map(|s| !s.is_empty()).unwrap_or(false);
        if !has_body && !has_gif {
            v.push(FieldViolation::new(
                "body",
                "CMT-VAL-004",
                "a comment must have text, a GIF, or both",
            ));
        }
        if v.is_empty() { Ok(()) } else { Err(v) }
    }
}

pub struct CreateCommentHandler<R, P> {
    pub repository: Arc<R>,
    pub publisher:  Arc<P>,
    /// The post must be readable to the commenter and its author must take
    /// their comments (interaction settings, blocks).
    pub gate:       Arc<dyn ReadGate>,
}

impl<R, P> CommandHandler<CreateCommentCommand> for CreateCommentHandler<R, P>
where
    R: CommentRepository,
    P: CommentEventPublisher,
{
    type Error = CommentError;

    async fn handle(&self, envelope: Envelope<CreateCommentCommand>) -> Result<(), CommentError> {
        let cmd = &envelope.payload;

        let comment_id = CommentId::try_from(cmd.comment_id.as_str())?;
        let post_id    = PostId::try_from(cmd.post_id.as_str())?;
        let author_id  = ProfileId::try_from(cmd.author_id.as_str())?;

        let body = cmd.body
            .as_deref()
            .filter(|s| !s.trim().is_empty())
            .map(CommentBody::new)
            .transpose()?;

        let gif = parse_gif(
            cmd.gif_id.as_deref(),
            cmd.gif_url.as_deref(),
            cmd.gif_width,
            cmd.gif_height,
        )?;

        let held = match self.gate.may_comment(&author_id, &post_id).await? {
            CommentAdmission::Allowed => false,
            CommentAdmission::Held => true,
            CommentAdmission::PostUnavailable => {
                return Err(CommentError::PostNotFound { post_id: cmd.post_id.clone() });
            }
            CommentAdmission::Restricted => {
                return Err(CommentError::CommentsRestricted { post_id: cmd.post_id.clone() });
            }
        };

        let (parent_id, parent_is_top_level) = resolve_parent(
            cmd.parent_id.as_deref(),
            self.repository.as_ref(),
        ).await?;

        let mut comment = Comment::create(
            comment_id,
            post_id,
            author_id,
            parent_id,
            parent_is_top_level,
            body,
            gif,
        )?;
        if held {
            // Held for the post owner's review: stored, not announced (#669).
            comment.hold();
        }

        self.repository.insert(&comment).await?;

        for event in comment.take_events() {
            self.publisher.publish(&event).await?;
        }

        tracing::debug!(
            comment_id = %cmd.comment_id,
            post_id    = %cmd.post_id,
            author_id  = %cmd.author_id,
            is_reply   = cmd.parent_id.is_some(),
            "comment created"
        );

        Ok(())
    }
}

async fn resolve_parent<R: CommentRepository>(
    parent_id_str: Option<&str>,
    repo:          &R,
) -> Result<(Option<CommentId>, bool), CommentError> {
    let raw = match parent_id_str.filter(|s| !s.is_empty()) {
        None      => return Ok((None, false)),
        Some(raw) => raw,
    };

    let pid = CommentId::try_from(raw)?;

    let parent = repo.find_by_id(&pid).await?
        .ok_or_else(|| CommentError::ParentNotFound { parent_id: pid.as_str() })?;

    if parent.status() == CommentStatus::Deleted {
        return Err(CommentError::ParentDeleted { parent_id: pid.as_str() });
    }

    let is_top = parent.is_top_level();
    Ok((Some(pid), is_top))
}

fn parse_gif(
    gif_id:     Option<&str>,
    gif_url:    Option<&str>,
    gif_width:  Option<u32>,
    gif_height: Option<u32>,
) -> Result<Option<GifAttachment>, CommentError> {
    let any = gif_id.is_some()
        || gif_url.map(|s| !s.is_empty()).unwrap_or(false);
    if !any {
        return Ok(None);
    }
    match (gif_id, gif_url, gif_width, gif_height) {
        (Some(id), Some(url), Some(w), Some(h))
            if !id.is_empty() && !url.is_empty() =>
        {
            Ok(Some(GifAttachment {
                gif_id:     id.to_owned(),
                gif_url:    url.to_owned(),
                gif_width:  w,
                gif_height: h,
            }))
        }
        _ => Err(CommentError::IncompleteGifMetadata),
    }
}
