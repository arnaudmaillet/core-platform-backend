//! The domain's view of the **inbound** source events search consumes.
//!
//! Search produces no events of its own — it is a terminal read-model. These types
//! are the *input contract* of the projector: the Phase-4 decode layer maps the
//! real wire payloads (`post.v1.events`, `profile.v1.events`, `moderation.v1.events`,
//! the derived hashtag stream, the GDPR erasure signal) onto these minimal,
//! already-distilled shapes. Keeping the distillation in the decode layer (e.g.
//! "which moderation actions count as a content hide", "which entity types map to a
//! search index") leaves the projector a pure, exhaustively-testable transform.

use chrono::{DateTime, Utc};

use super::value_object::EntityKind;

/// The union of everything search ingests. One decoded event in, one
/// [`super::mutation::IndexMutation`] out.
#[derive(Debug, Clone, PartialEq)]
pub enum SourceEvent {
    Post(PostEvent),
    Profile(ProfileEvent),
    Hashtag(HashtagEvent),
    Moderation(ModerationEvent),
    Compliance(ComplianceEvent),
}

// ── Content / identity lifecycle ──────────────────────────────────────────────

#[derive(Debug, Clone, PartialEq)]
pub enum PostEvent {
    /// Post became visible — index it.
    Published(PostSnapshot),
    /// Post edited — full re-projection (idempotent upsert), never a partial patch.
    Updated(PostSnapshot),
    /// Post hard-deleted — remove from the index.
    Deleted(EntityDeletion),
}

#[derive(Debug, Clone, PartialEq)]
pub enum ProfileEvent {
    /// Profile created or edited — full re-projection.
    Upserted(ProfileSnapshot),
    Deleted(EntityDeletion),
    /// The owner masked their own profile — make it non-searchable under the
    /// **owner** authority (independent of any moderation hide).
    OwnerHidden {
        profile_id: String,
        occurred_at: DateTime<Utc>,
    },
    /// The owner un-masked their profile — restore owner-authority visibility.
    OwnerRestored {
        profile_id: String,
        occurred_at: DateTime<Utc>,
    },
    /// The owner chose whether the profile is findable by search (#661), under
    /// the **discovery** authority.
    DiscoverabilityChanged {
        profile_id: String,
        searchable: bool,
        occurred_at: DateTime<Utc>,
    },
    /// The author's post history window (#664): their posts older than
    /// `window_days` leave search; `None` shows them all.
    PostWindowChanged {
        profile_id: String,
        window_days: Option<u32>,
        occurred_at: DateTime<Utc>,
    },
}

#[derive(Debug, Clone, PartialEq)]
pub enum HashtagEvent {
    /// A refreshed view of a tag from the derived hashtag stream.
    Observed(HashtagSnapshot),
}

/// A complete, current snapshot of a post (full-document projection). `revision` is
/// the source's monotonic version → the document's [`super::value_object::DocVersion`].
#[derive(Debug, Clone, PartialEq)]
pub struct PostSnapshot {
    pub post_id: String,
    pub author_id: String,
    pub author_handle: String,
    pub caption: String,
    pub hashtags: Vec<String>,
    pub thumbnail_key: String,
    pub created_at: DateTime<Utc>,
    /// When the post leaves its author's post window (#664); `None` without one.
    pub visible_until: Option<DateTime<Utc>>,
    pub revision: u64,
}

#[derive(Debug, Clone, PartialEq)]
pub struct ProfileSnapshot {
    pub profile_id: String,
    pub handle: String,
    pub display_name: String,
    pub bio: String,
    pub avatar_key: String,
    pub verified: bool,
    pub created_at: DateTime<Utc>,
    pub revision: u64,
}

#[derive(Debug, Clone, PartialEq)]
pub struct HashtagSnapshot {
    pub tag: String,
    pub post_count: i64,
    pub revision: u64,
}

/// A hard delete of an entity by id.
#[derive(Debug, Clone, PartialEq)]
pub struct EntityDeletion {
    pub id: String,
}

// ── Moderation visibility (already distilled by the decode layer) ─────────────

#[derive(Debug, Clone, PartialEq)]
pub enum ModerationEvent {
    /// A content hide — the target must become non-searchable (document retained).
    VisibilityRevoked(VisibilityChange),
    /// An appeal reversal — the target becomes searchable again.
    VisibilityRestored(VisibilityChange),
}

/// The target of a visibility change. `kind` is `None` when the moderated entity
/// does not map to a search index (e.g. an account- or chat-message-level action) —
/// the projector then skips it.
#[derive(Debug, Clone, PartialEq)]
pub struct VisibilityChange {
    pub kind: Option<EntityKind>,
    pub id: String,
    pub occurred_at: DateTime<Utc>,
}

// ── Compliance / GDPR ─────────────────────────────────────────────────────────

#[derive(Debug, Clone, PartialEq)]
pub enum ComplianceEvent {
    /// A GDPR erasure for an actor — deep-purge everything they authored across all
    /// indices (`delete_by_query` on `author_id`), with no retained tombstone.
    ActorPurged { author_id: String },
}
