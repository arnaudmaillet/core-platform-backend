use chrono::{DateTime, Utc};
use crate::{
    domain::{
        entity::MediaAttachment,
        event::{DomainEvent, PostDeletedEvent, PostPublishedEvent, PostUpdatedEvent},
        value_object::{
            AudioReference, Caption, GeoPoint, ModerationRestriction, ModerationState, PostId,
            PostKind, PostStatus, ProfileId, Viewer,
        },
    },
    error::PostError,
};

const MAX_CAROUSEL_ITEMS: usize = 10;
const MAX_CAROUSEL_VIDEO_SECS: f32 = 15.0;

pub struct Post {
    id:             PostId,
    profile_id:     ProfileId,
    kind:           PostKind,
    status:         PostStatus,
    caption:        Caption,
    attachments:    Vec<MediaAttachment>,
    parent_id:      Option<PostId>,
    root_id:        Option<PostId>,
    audio_ref:      Option<AudioReference>,
    location:       Option<GeoPoint>,
    created_at:     DateTime<Utc>,
    updated_at:     DateTime<Utc>,
    published_at:   Option<DateTime<Utc>>,
    deleted_at:     Option<DateTime<Utc>>,
    /// What moderation imposes on the post (from `moderation.v1.events`).
    moderation:     ModerationState,
    pending_events: Vec<DomainEvent>,
}

impl Post {
    #[allow(clippy::too_many_arguments)] // aggregate/worker constructor — same precedent as chat
    pub fn create(
        id:          PostId,
        profile_id:  ProfileId,
        kind:        PostKind,
        caption:     Caption,
        attachments: Vec<MediaAttachment>,
        parent_id:   Option<PostId>,
        root_id:     Option<PostId>,
        audio_ref:   Option<AudioReference>,
        location:    Option<GeoPoint>,
    ) -> Result<Self, PostError> {
        validate_threading(&parent_id, &root_id)?;
        validate_attachments(kind, &attachments)?;

        let now = Utc::now();
        Ok(Self {
            id,
            profile_id,
            kind,
            status: PostStatus::Draft,
            caption,
            attachments,
            parent_id,
            root_id,
            audio_ref,
            location,
            created_at: now,
            updated_at: now,
            published_at: None,
            deleted_at: None,
            moderation: ModerationState::default(),
            pending_events: Vec::new(),
        })
    }

    #[allow(clippy::too_many_arguments)] // aggregate/worker constructor — same precedent as chat
    pub fn reconstitute(
        id:           PostId,
        profile_id:   ProfileId,
        kind:         PostKind,
        status:       PostStatus,
        caption:      Caption,
        attachments:  Vec<MediaAttachment>,
        parent_id:    Option<PostId>,
        root_id:      Option<PostId>,
        audio_ref:    Option<AudioReference>,
        location:     Option<GeoPoint>,
        created_at:   DateTime<Utc>,
        updated_at:   DateTime<Utc>,
        published_at: Option<DateTime<Utc>>,
        deleted_at:   Option<DateTime<Utc>>,
        moderation:   ModerationState,
    ) -> Self {
        Self {
            id,
            profile_id,
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
            pending_events: Vec::new(),
        }
    }

    pub fn publish(&mut self) -> Result<DateTime<Utc>, PostError> {
        match self.status {
            PostStatus::Published => return Err(PostError::PostAlreadyPublished {
                post_id: self.id.as_str(),
            }),
            PostStatus::Deleted => return Err(PostError::PostAlreadyDeleted {
                post_id: self.id.as_str(),
            }),
            PostStatus::Draft => {}
        }

        let now = Utc::now();
        self.status = PostStatus::Published;
        self.published_at = Some(now);
        self.updated_at = now;

        self.pending_events.push(DomainEvent::PostPublished(PostPublishedEvent {
            post_id:         self.id.as_str(),
            profile_id:      self.profile_id.as_str(),
            kind:            self.kind.to_string(),
            published_at_ms: now.timestamp_millis(),
            // Placeholder; the publish handler stamps the author's current tier from
            // the projection (the aggregate owns no denormalized profile state).
            author_tier:     0,
            audio_id:        self.audio_ref.as_ref().map(|a| a.audio_id.as_str()),
            audio_kind:      self.audio_ref.as_ref().map(|a| a.audio_kind.as_tinyint() as u8),
            // Denormalized for geo-discovery. caption + cover thumbnail are owned by
            // the aggregate; location is client-supplied at create. Absent location
            // → geo-discovery does not spatially index the post.
            caption:         self.caption.as_str().to_owned(),
            thumbnail_url:   self.attachments.first()
                                 .and_then(|a| a.thumbnail_url.as_ref())
                                 .map(|u| u.as_str().to_owned()),
            lat:             self.location.map(|g| g.lat()),
            lng:             self.location.map(|g| g.lng()),
        }));

        Ok(now)
    }

    pub fn update(
        &mut self,
        caption:     Caption,
        attachments: Vec<MediaAttachment>,
    ) -> Result<(), PostError> {
        if self.status == PostStatus::Deleted {
            return Err(PostError::PostAlreadyDeleted { post_id: self.id.as_str() });
        }

        validate_attachments(self.kind, &attachments)?;

        let now = Utc::now();
        self.caption = caption;
        self.attachments = attachments;
        self.updated_at = now;

        self.pending_events.push(DomainEvent::PostUpdated(PostUpdatedEvent {
            post_id:       self.id.as_str(),
            profile_id:    self.profile_id.as_str(),
            updated_at_ms: now.timestamp_millis(),
        }));

        Ok(())
    }

    pub fn delete(&mut self) -> Result<DateTime<Utc>, PostError> {
        if self.status == PostStatus::Deleted {
            return Err(PostError::PostAlreadyDeleted { post_id: self.id.as_str() });
        }

        let now = Utc::now();
        self.status = PostStatus::Deleted;
        self.deleted_at = Some(now);
        self.updated_at = now;

        self.pending_events.push(DomainEvent::PostDeleted(PostDeletedEvent {
            post_id:       self.id.as_str(),
            profile_id:    self.profile_id.as_str(),
            deleted_at_ms: now.timestamp_millis(),
        }));

        Ok(now)
    }

    /// Whether `viewer` may read this post: drafts, deleted posts and posts
    /// moderation removed are their author's alone.
    pub fn is_visible_to(&self, viewer: &Viewer) -> bool {
        viewer.may_see(&self.profile_id, self.status, self.moderation.restriction)
    }

    /// Records a moderation outcome (an enforcement applied or reversed). Ignored
    /// unless `version` is newer than the one held, so redelivered or reordered
    /// events converge. Returns whether the post changed. Emits no post event:
    /// downstream services read moderation's stream themselves.
    pub fn apply_moderation(&mut self, restriction: ModerationRestriction, version: i64) -> bool {
        self.moderation.apply(restriction, version)
    }

    pub fn take_events(&mut self) -> Vec<DomainEvent> {
        std::mem::take(&mut self.pending_events)
    }

    pub fn id(&self)           -> &PostId    { &self.id }
    pub fn profile_id(&self)   -> &ProfileId { &self.profile_id }
    pub fn kind(&self)         -> PostKind   { self.kind }
    pub fn status(&self)       -> PostStatus { self.status }
    pub fn moderation(&self)   -> ModerationState { self.moderation }
    pub fn caption(&self)      -> &Caption   { &self.caption }
    pub fn attachments(&self)  -> &[MediaAttachment] { &self.attachments }
    pub fn parent_id(&self)    -> Option<&PostId>    { self.parent_id.as_ref() }
    pub fn root_id(&self)      -> Option<&PostId>    { self.root_id.as_ref() }
    pub fn audio_ref(&self)    -> Option<&AudioReference> { self.audio_ref.as_ref() }
    pub fn location(&self)     -> Option<GeoPoint>   { self.location }

    /// Replaces the location with what a given reader may see of it (read
    /// path only — a post read this way is never saved).
    pub fn show_location(&mut self, shown: Option<GeoPoint>) {
        self.location = shown;
    }
    pub fn created_at(&self)   -> DateTime<Utc>      { self.created_at }
    pub fn updated_at(&self)   -> DateTime<Utc>      { self.updated_at }
    pub fn published_at(&self) -> Option<DateTime<Utc>> { self.published_at }
    pub fn deleted_at(&self)   -> Option<DateTime<Utc>> { self.deleted_at }
}

fn validate_threading(parent_id: &Option<PostId>, root_id: &Option<PostId>) -> Result<(), PostError> {
    match (parent_id, root_id) {
        (Some(_), Some(_)) | (None, None) => Ok(()),
        _ => Err(PostError::DomainViolation {
            field:   "parent_id/root_id".into(),
            message: "parent_id and root_id must both be present or both absent".into(),
        }),
    }
}

fn validate_attachments(kind: PostKind, attachments: &[MediaAttachment]) -> Result<(), PostError> {
    match kind {
        PostKind::TextOnly => {}

        PostKind::Carousel => {
            if attachments.len() < 2 {
                return Err(PostError::CarouselTooFewItems);
            }
            if attachments.len() > MAX_CAROUSEL_ITEMS {
                return Err(PostError::CarouselTooManyItems { count: attachments.len() });
            }
            for (i, a) in attachments.iter().enumerate() {
                if a.is_video() {
                    if a.thumbnail_url.is_none() {
                        return Err(PostError::MissingVideoThumbnail { index: i });
                    }
                    if let Some(d) = a.duration_seconds
                        && d > MAX_CAROUSEL_VIDEO_SECS {
                            return Err(PostError::CarouselVideoTooLong { index: i, duration: d });
                        }
                }
                if a.width == 0 || a.height == 0 {
                    return Err(PostError::InvalidDimensions {
                        index: i, width: a.width, height: a.height,
                    });
                }
            }
        }

        PostKind::MainVideo => {
            if let Some(a) = attachments.first() {
                if a.is_video() && a.thumbnail_url.is_none() {
                    return Err(PostError::MissingVideoThumbnail { index: 0 });
                }
                if a.width == 0 || a.height == 0 {
                    return Err(PostError::InvalidDimensions {
                        index: 0, width: a.width, height: a.height,
                    });
                }
            }
        }
    }

    Ok(())
}
