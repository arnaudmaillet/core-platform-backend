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

/// A post's own remix / original-sound reuse permissions (#669); `None`
/// follows the author's default.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct ReuseOverrides {
    pub allow_remix:       Option<bool>,
    pub allow_sound_reuse: Option<bool>,
}
/// How long a deleted post stays in "Recently deleted" and can be restored.
pub const RESTORE_WINDOW: chrono::Duration = chrono::Duration::days(30);
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
    /// The post's own remix / original-sound reuse permission (#669); `None`
    /// follows its author's default.
    reuse:          ReuseOverrides,
    /// Read path only: older than its author's post window (#664). Set for a
    /// mesh read, which still gets the post; never stored.
    outside_window: bool,
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
            reuse: ReuseOverrides::default(),
            outside_window: false,
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
            reuse: ReuseOverrides::default(),
            outside_window: false,
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

        let event = self.published_event(now);
        self.pending_events.push(event);

        Ok(now)
    }

    /// A deleted post comes back within [`RESTORE_WINDOW`] of its deletion, as
    /// it was: published (announced again, at its original publication time —
    /// unless moderation removed or limited it) or a draft.
    pub fn restore(&mut self, now: DateTime<Utc>) -> Result<(), PostError> {
        let Some(deleted_at) = self.deleted_at.filter(|_| self.status == PostStatus::Deleted) else {
            return Err(PostError::PostNotDeleted { post_id: self.id.as_str() });
        };
        if now - deleted_at > RESTORE_WINDOW {
            return Err(PostError::RestoreWindowExpired { post_id: self.id.as_str() });
        }
        self.deleted_at = None;
        self.updated_at = now;
        match self.published_at {
            Some(published_at) => {
                self.status = PostStatus::Published;
                // A post moderation removed or limited comes back to its author
                // but is not re-announced: a takedown survives delete → restore.
                if !matches!(
                    self.moderation.restriction,
                    ModerationRestriction::Removed | ModerationRestriction::Limited
                ) {
                    let event = self.published_event(published_at);
                    self.pending_events.push(event);
                }
            }
            None => self.status = PostStatus::Draft,
        }
        Ok(())
    }

    /// The `PostPublished` event for this post, published at `published_at`.
    fn published_event(&self, published_at: DateTime<Utc>) -> DomainEvent {
        DomainEvent::PostPublished(PostPublishedEvent {
            post_id:         self.id.as_str(),
            profile_id:      self.profile_id.as_str(),
            kind:            self.kind.to_string(),
            published_at_ms: published_at.timestamp_millis(),
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
        })
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
    pub fn reuse(&self)        -> ReuseOverrides     { self.reuse }

    /// Sets the post's own remix / sound-reuse permissions (at creation, or
    /// restored from storage).
    pub fn with_reuse(mut self, reuse: ReuseOverrides) -> Self {
        self.reuse = reuse;
        self
    }

    /// Replaces the location with what a given reader may see of it (read
    /// path only — a post read this way is never saved).
    pub fn show_location(&mut self, shown: Option<GeoPoint>) {
        self.location = shown;
    }

    /// Marks the post as older than its author's post window (#664), for a
    /// mesh reader that must withhold it from clients (read path only).
    pub fn mark_outside_window(&mut self) {
        self.outside_window = true;
    }
    pub fn outside_window(&self) -> bool { self.outside_window }
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

#[cfg(test)]
mod restore_tests {
    use chrono::Duration;
    use uuid::Uuid;

    use super::*;

    fn post() -> Post {
        Post::create(
            PostId::from_uuid(Uuid::now_v7()),
            ProfileId::try_from(Uuid::now_v7().to_string().as_str()).unwrap(),
            PostKind::TextOnly,
            Caption::new("hello").unwrap(),
            Vec::new(),
            None,
            None,
            None,
            None,
        )
        .unwrap()
    }

    #[test]
    fn a_deleted_published_post_comes_back_published_and_announced_at_its_publication_time() {
        let mut p = post();
        let published_at = p.publish().unwrap();
        p.delete().unwrap();
        p.take_events();

        p.restore(Utc::now()).unwrap();
        assert_eq!(p.status(), PostStatus::Published);
        assert_eq!(p.deleted_at(), None);
        let events = p.take_events();
        assert!(matches!(events.as_slice(),
            [DomainEvent::PostPublished(e)] if e.published_at_ms == published_at.timestamp_millis()));
    }

    #[test]
    fn a_post_moderation_removed_or_limited_is_restored_but_not_re_announced() {
        for restriction in [ModerationRestriction::Removed, ModerationRestriction::Limited] {
            let mut p = post();
            p.publish().unwrap();
            assert!(p.apply_moderation(restriction, 1));
            p.delete().unwrap();
            p.take_events();
            p.restore(Utc::now()).unwrap();
            assert_eq!(p.status(), PostStatus::Published);
            assert!(p.take_events().is_empty(), "{restriction:?}: the takedown survives");
        }
    }

    #[test]
    fn a_deleted_draft_comes_back_a_draft_silently() {
        let mut p = post();
        p.delete().unwrap();
        p.take_events();
        p.restore(Utc::now()).unwrap();
        assert_eq!(p.status(), PostStatus::Draft);
        assert!(p.take_events().is_empty());
    }

    #[test]
    fn only_a_post_deleted_within_30_days_can_be_restored() {
        let mut live = post();
        assert!(matches!(live.restore(Utc::now()), Err(PostError::PostNotDeleted { .. })));

        let mut p = post();
        let deleted_at = p.delete().unwrap();
        assert!(matches!(
            p.restore(deleted_at + RESTORE_WINDOW + Duration::seconds(1)),
            Err(PostError::RestoreWindowExpired { .. })
        ));
        assert!(p.restore(deleted_at + RESTORE_WINDOW).is_ok(), "the 30th day still counts");
    }
}
