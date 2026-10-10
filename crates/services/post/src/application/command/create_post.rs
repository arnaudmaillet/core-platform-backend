use std::sync::Arc;

use async_trait::async_trait;
use cqrs::{Command, CommandHandler, Envelope};
use validate_core::{FieldViolation, Validate};

use crate::{
    application::port::{check_mentions, AudienceGate, CreateClaim, CreateKeys, EventPublisher, PostRepository, ReuseRegistry},
    domain::{
        aggregate::{Post, ReuseOverrides},
        entity::MediaAttachment,
        value_object::{
            AudioId, AudioKind, AudioReference, Caption, CdnUrl, GeoPoint, IdempotencyKey, MimeType, PostId, PostKind,
            ProfileId,
        },
    },
    error::PostError,
};

pub struct AttachmentInput {
    pub cdn_url:          String,
    pub mime_type:        String,
    pub width:            u32,
    pub height:           u32,
    pub thumbnail_url:    Option<String>,
    pub duration_seconds: Option<f32>,
}

pub struct CreatePostCommand {
    pub post_id:     String,
    pub profile_id:  String,
    pub kind:        i32,
    pub caption:     String,
    pub attachments: Vec<AttachmentInput>,
    pub parent_id:   Option<String>,
    pub root_id:     Option<String>,
    pub audio_ref:   Option<AudioReference>,
    /// Optional client-supplied post location as `(lat, lng)`. Validated into a
    /// `GeoPoint` during handling. Absent → the post carries no location and is
    /// not geo-indexed downstream.
    pub location:    Option<(f64, f64)>,
    /// The post's own remix / original-sound reuse permission (#669); `None`
    /// follows the author's default.
    pub reuse:       ReuseOverrides,
    /// The client's key for this post, reused on its retries (#876).
    pub idempotency_key: Option<String>,
}

impl Command for CreatePostCommand {}

impl Validate for CreatePostCommand {
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

pub(crate) fn parse_attachments(inputs: &[AttachmentInput]) -> Result<Vec<MediaAttachment>, PostError> {
    inputs
        .iter()
        .enumerate()
        .map(|(i, a)| {
            let cdn_url   = CdnUrl::new(&a.cdn_url)?;
            let mime_type = MimeType::new(&a.mime_type)?;
            if a.width == 0 || a.height == 0 {
                return Err(PostError::InvalidDimensions { index: i, width: a.width, height: a.height });
            }
            let thumbnail_url = a.thumbnail_url.as_deref()
                .filter(|s| !s.is_empty())
                .map(CdnUrl::new)
                .transpose()?;
            Ok(MediaAttachment {
                cdn_url,
                mime_type,
                width:            a.width,
                height:           a.height,
                thumbnail_url,
                duration_seconds: a.duration_seconds,
            })
        })
        .collect()
}

/// The post a CreatePost answers.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CreatedPost {
    /// The command's own id, or on a replay the first call's post.
    pub post_id:  PostId,
    /// The idempotency key was already used (#876): nothing was written.
    pub replayed: bool,
}

/// Creates a post and says which (the gRPC layer answers its id).
#[async_trait]
pub trait CreatePosts: Send + Sync + 'static {
    async fn create(&self, cmd: &CreatePostCommand) -> Result<CreatedPost, PostError>;
}

pub struct CreatePostHandler<R, P> {
    pub repository: Arc<R>,
    pub publisher:  Arc<P>,
    /// Who may reuse whose original sound (#669).
    pub reuse:      Arc<dyn ReuseRegistry>,
    /// Who takes mentions from whom (#656).
    pub audience:   Arc<dyn AudienceGate>,
    /// CreatePost's idempotency keys (#876).
    pub keys:       Arc<dyn CreateKeys>,
}

/// May `author` reuse `audio`? Allowed unless the sound's original post (by
/// someone else) or, failing a post override, its author forbids it. A sound
/// the platform does not know (a library track) is free to use.
async fn may_reuse_sound<R: PostRepository>(
    repository: &R,
    reuse: &dyn ReuseRegistry,
    author: &ProfileId,
    audio: &AudioId,
) -> Result<bool, PostError> {
    let Some((origin_post, origin_author)) = reuse.origin(audio).await? else {
        return Ok(true);
    };
    if origin_author.as_uuid() == author.as_uuid() {
        return Ok(true);
    }
    let override_ = repository.find_by_id(&origin_post).await?.and_then(|p| p.reuse().allow_sound_reuse);
    match override_ {
        Some(allowed) => Ok(allowed),
        None => Ok(reuse.defaults(&origin_author).await?.allow_sound_reuse),
    }
}

impl<R, P> CommandHandler<CreatePostCommand> for CreatePostHandler<R, P>
where
    R: PostRepository,
    P: EventPublisher,
{
    type Error = PostError;

    async fn handle(&self, envelope: Envelope<CreatePostCommand>) -> Result<(), PostError> {
        self.create(&envelope.payload).await.map(drop)
    }
}

#[async_trait]
impl<R, P> CreatePosts for CreatePostHandler<R, P>
where
    R: PostRepository,
    P: EventPublisher,
{
    /// A keyed call (#876) claims its key first: a key already used answers
    /// its post and writes nothing; one still in flight is refused, retryably;
    /// a call that fails before its post is stored frees the key for the retry.
    async fn create(&self, cmd: &CreatePostCommand) -> Result<CreatedPost, PostError> {
        let post_id    = PostId::try_from(cmd.post_id.as_str())?;
        let profile_id = ProfileId::try_from(cmd.profile_id.as_str())?;
        let key = cmd
            .idempotency_key
            .as_deref()
            .filter(|k| !k.is_empty())
            .map(IdempotencyKey::try_from)
            .transpose()?;

        if let Some(key) = &key {
            match self.keys.claim(&profile_id, key, post_id.clone()).await? {
                CreateClaim::Fresh => {}
                CreateClaim::Created(first) => return Ok(CreatedPost { post_id: first, replayed: true }),
                CreateClaim::InFlight => return Err(PostError::CreateInFlight),
            }
        }

        let created = self.write(cmd, post_id.clone(), profile_id.clone(), key.as_ref()).await;
        if let (Err(_), Some(key)) = (&created, &key)
            && let Err(e) = self.keys.release(&profile_id, key, post_id.clone()).await
        {
            tracing::warn!(error = %e, "create key release failed; it expires on its own");
        }
        created.map(|()| CreatedPost { post_id, replayed: false })
    }
}

impl<R, P> CreatePostHandler<R, P>
where
    R: PostRepository,
    P: EventPublisher,
{
    /// Checks and stores the post (a draft).
    async fn write(
        &self,
        cmd:        &CreatePostCommand,
        post_id:    PostId,
        profile_id: ProfileId,
        claimed:    Option<&IdempotencyKey>,
    ) -> Result<(), PostError> {

        let kind = match cmd.kind {
            1 => PostKind::TextOnly,
            2 => PostKind::Carousel,
            3 => PostKind::MainVideo,
            v => return Err(PostError::DomainViolation {
                field:   "kind".into(),
                message: format!("unknown proto PostKind value: {v}"),
            }),
        };

        let caption     = Caption::new(&cmd.caption)?;
        let attachments = parse_attachments(&cmd.attachments)?;
        // Every mentioned profile must take mentions from the author (#656).
        check_mentions(self.audience.as_ref(), &profile_id, &caption).await?;

        let parent_id = cmd.parent_id
            .as_deref()
            .filter(|s| !s.is_empty())
            .map(PostId::try_from)
            .transpose()?;
        let root_id = cmd.root_id
            .as_deref()
            .filter(|s| !s.is_empty())
            .map(PostId::try_from)
            .transpose()?;

        let location = cmd.location
            .map(|(lat, lng)| GeoPoint::new(lat, lng))
            .transpose()?;

        // Using someone else's original sound needs their permission (#669),
        // whatever the request calls it.
        if let Some(audio) = cmd.audio_ref.as_ref()
            && !may_reuse_sound(self.repository.as_ref(), self.reuse.as_ref(), &profile_id, &audio.audio_id).await?
        {
            return Err(PostError::SoundReuseNotAllowed { audio_id: audio.audio_id.as_str() });
        }

        let post = Post::create(post_id, profile_id, kind, caption, attachments, parent_id, root_id, cmd.audio_ref.clone(), location)?
            .with_reuse(cmd.reuse);
        self.repository.insert(&post).await?;
        // Stored: from here a retry answers this post, even if a later step
        // fails. A failed completion leaves the claim pending until it expires.
        if let Some(key) = claimed
            && let Err(e) = self.keys.complete(post.profile_id(), key, post.id().clone()).await
        {
            tracing::warn!(error = %e, "create key completion failed; retries are refused until it expires");
        }
        // An original sound belongs to the post that made it.
        if let Some(audio) = cmd.audio_ref.as_ref().filter(|a| a.audio_kind == AudioKind::OriginalSound) {
            self.reuse.record_origin(&audio.audio_id, post.id(), post.profile_id()).await?;
        }
        Ok(())
    }
}
