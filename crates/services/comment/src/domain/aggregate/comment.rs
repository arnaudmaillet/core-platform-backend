use chrono::{DateTime, Utc};

use crate::{
    domain::{
        entity::GifAttachment,
        event::{
            CommentCreatedEvent, CommentDeletedEvent, DomainEvent,
        },
        value_object::{CommentBody, CommentId, CommentStatus, PostId, ProfileId},
    },
    error::CommentError,
};

/// Controls the persistence strategy chosen by the delete command handler.
///
/// - `Tombstone`: the comment has active replies; the row is kept with content
///   nulled and status set to Deleted so the reply thread remains navigable.
/// - `Purge`: the comment is a leaf node; both table rows are physically deleted.
pub enum DeletionStrategy {
    Tombstone,
    Purge,
}

pub struct Comment {
    id:             CommentId,
    post_id:        PostId,
    author_id:      ProfileId,
    /// `None` for top-level comments. `Some` only for 1-level replies.
    parent_id:      Option<CommentId>,
    status:         CommentStatus,
    body:           Option<CommentBody>,
    gif:            Option<GifAttachment>,
    created_at:     DateTime<Utc>,
    updated_at:     DateTime<Utc>,
    deleted_at:     Option<DateTime<Utc>>,
    /// Held for the post owner's review (a temporary limit, #669): seen by its
    /// author and the owner only, announced on approval.
    held:           bool,
    pending_events: Vec<DomainEvent>,
}

impl Comment {
    /// Creates a new comment and appends a `CommentCreated` domain event.
    ///
    /// Enforces:
    /// - At least one of `body` or `gif` must be `Some`.
    /// - If `parent_id` is `Some`, `parent_is_top_level` must be `true` — the
    ///   command handler resolves this flag via a point-read before calling here.
    pub fn create(
        id:                  CommentId,
        post_id:             PostId,
        author_id:           ProfileId,
        parent_id:           Option<CommentId>,
        parent_is_top_level: bool,
        body:                Option<CommentBody>,
        gif:                 Option<GifAttachment>,
    ) -> Result<Self, CommentError> {
        if parent_id.is_some() && !parent_is_top_level {
            return Err(CommentError::NestingDepthExceeded);
        }
        if body.is_none() && gif.is_none() {
            return Err(CommentError::EmptyContent);
        }

        let now = Utc::now();

        let event = DomainEvent::CommentCreated(CommentCreatedEvent {
            comment_id:    id.as_str(),
            post_id:       post_id.as_str(),
            author_id:     author_id.as_str(),
            parent_id:     parent_id.as_ref().map(CommentId::as_str),
            created_at_ms: now.timestamp_millis(),
        });

        Ok(Self {
            id,
            post_id,
            author_id,
            parent_id,
            status: CommentStatus::Published,
            body,
            gif,
            created_at: now,
            updated_at: now,
            deleted_at: None,
            held: false,
            pending_events: vec![event],
        })
    }

    /// Reconstitutes a `Comment` from its persisted ScyllaDB state.
    /// Does not emit domain events — use only for read/mutation paths.
    #[allow(clippy::too_many_arguments)]
    pub fn reconstitute(
        id:         CommentId,
        post_id:    PostId,
        author_id:  ProfileId,
        parent_id:  Option<CommentId>,
        status:     CommentStatus,
        body:       Option<CommentBody>,
        gif:        Option<GifAttachment>,
        created_at: DateTime<Utc>,
        updated_at: DateTime<Utc>,
        deleted_at: Option<DateTime<Utc>>,
    ) -> Self {
        Self {
            id,
            post_id,
            author_id,
            parent_id,
            status,
            body,
            gif,
            created_at,
            updated_at,
            deleted_at,
            held: false,
            pending_events: Vec::new(),
        }
    }

    /// Marks the comment as deleted and returns the persistence strategy.
    ///
    /// - `has_active_replies = true`  → `DeletionStrategy::Tombstone`:
    ///   content fields are nulled in-place so the reply tree remains navigable.
    /// - `has_active_replies = false` → `DeletionStrategy::Purge`:
    ///   the caller must physically delete from both ScyllaDB tables.
    ///
    /// Either way a `CommentDeleted` domain event is appended for Kafka.
    pub fn delete(
        &mut self,
        has_active_replies: bool,
    ) -> Result<DeletionStrategy, CommentError> {
        if self.status == CommentStatus::Deleted {
            return Err(CommentError::CommentAlreadyDeleted {
                comment_id: self.id.as_str(),
            });
        }

        let now = Utc::now();
        self.status     = CommentStatus::Deleted;
        self.deleted_at = Some(now);
        self.updated_at = now;

        self.pending_events.push(DomainEvent::CommentDeleted(CommentDeletedEvent {
            comment_id:    self.id.as_str(),
            post_id:       self.post_id.as_str(),
            author_id:     self.author_id.as_str(),
            deleted_at_ms: now.timestamp_millis(),
        }));

        if has_active_replies {
            self.body = None;
            self.gif  = None;
            Ok(DeletionStrategy::Tombstone)
        } else {
            Ok(DeletionStrategy::Purge)
        }
    }

    /// Holds a just-created comment for the post owner's review: it is not
    /// announced (its `CommentCreated` waits for the approval).
    pub fn hold(&mut self) {
        self.held = true;
        self.pending_events.clear();
    }

    /// Restores the stored held flag.
    pub fn with_held(mut self, held: bool) -> Self {
        self.held = held;
        self
    }

    /// The post owner approves a held comment: it shows and is announced.
    pub fn release(&mut self) -> Result<(), CommentError> {
        if !self.held || self.status == CommentStatus::Deleted {
            return Err(CommentError::DomainViolation {
                field:   "held".into(),
                message: "the comment is not held".into(),
            });
        }
        self.held = false;
        self.pending_events.push(DomainEvent::CommentCreated(CommentCreatedEvent {
            comment_id:    self.id.as_str(),
            post_id:       self.post_id.as_str(),
            author_id:     self.author_id.as_str(),
            parent_id:     self.parent_id.as_ref().map(CommentId::as_str),
            created_at_ms: self.created_at.timestamp_millis(),
        }));
        Ok(())
    }

    pub fn held(&self) -> bool {
        self.held
    }

    /// Drains the pending domain event queue. Must be called after each mutation.
    pub fn take_events(&mut self) -> Vec<DomainEvent> {
        std::mem::take(&mut self.pending_events)
    }

    pub fn id(&self)         -> &CommentId          { &self.id }
    pub fn post_id(&self)    -> &PostId              { &self.post_id }
    pub fn author_id(&self)  -> &ProfileId           { &self.author_id }
    pub fn parent_id(&self)  -> Option<&CommentId>  { self.parent_id.as_ref() }
    pub fn status(&self)     -> CommentStatus        { self.status }
    pub fn body(&self)       -> Option<&CommentBody> { self.body.as_ref() }
    pub fn gif(&self)        -> Option<&GifAttachment> { self.gif.as_ref() }
    pub fn created_at(&self) -> DateTime<Utc>        { self.created_at }
    pub fn updated_at(&self) -> DateTime<Utc>        { self.updated_at }
    pub fn deleted_at(&self) -> Option<DateTime<Utc>> { self.deleted_at }

    pub fn is_top_level(&self) -> bool {
        self.parent_id.is_none()
    }
}

fn _assert_send_sync() {
    fn _check<T: Send + Sync>() {}
    _check::<CommentError>();
}

#[cfg(test)]
mod held_tests {
    use uuid::Uuid;

    use super::*;

    fn comment() -> Comment {
        Comment::create(
            CommentId::from_uuid(Uuid::now_v7()),
            PostId::from_uuid(Uuid::now_v7()),
            ProfileId::from_uuid(Uuid::now_v7()),
            None,
            true,
            Some(CommentBody::new("hi").unwrap()),
            None,
        )
        .unwrap()
    }

    #[test]
    fn a_held_comment_is_announced_only_on_release() {
        let mut c = comment();
        c.hold();
        assert!(c.held());
        assert!(c.take_events().is_empty(), "not announced while held");
        c.release().unwrap();
        assert!(!c.held());
        assert!(matches!(c.take_events().as_slice(), [DomainEvent::CommentCreated(_)]));
        assert!(c.release().is_err(), "only a held comment is released");
    }
}
