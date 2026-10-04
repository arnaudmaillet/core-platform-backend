//! Search-owned deserialization DTOs for the **thin** wire events it consumes.
//!
//! Search must not depend on `post` / `moderation` crates (a sideways services→
//! services edge the tiering forbids), so it owns its read schema: minimal structs
//! that match the published JSON. They are intentionally lenient — extra fields are
//! ignored, so an additive change upstream never breaks the consumer.
//!
//! Note the events are **notifications, not snapshots**: `post.v1.events` carries
//! ids + timestamps, not the caption/hashtags/thumbnail needed to build a document.
//! Content-bearing events are therefore decoded to a [`super::decoder::Decoded::NeedsContent`]
//! and hydrated from the source service before projection.

use serde::Deserialize;

// ── post.v1.events (internally tagged on `type`) ──────────────────────────────

#[derive(Debug, Clone, Deserialize)]
#[serde(tag = "type")]
pub enum PostWireEvent {
    PostPublished(PostPublishedWire),
    PostUpdated(PostUpdatedWire),
    PostDeleted(PostDeletedWire),
}

#[derive(Debug, Clone, Deserialize)]
pub struct PostPublishedWire {
    pub post_id: String,
    pub profile_id: String,
    pub published_at_ms: i64,
}

#[derive(Debug, Clone, Deserialize)]
pub struct PostUpdatedWire {
    pub post_id: String,
    pub profile_id: String,
    pub updated_at_ms: i64,
}

#[derive(Debug, Clone, Deserialize)]
pub struct PostDeletedWire {
    pub post_id: String,
    #[allow(dead_code)] // present on the wire; not needed to delete by id
    pub profile_id: String,
    pub deleted_at_ms: i64,
}

// ── moderation.v1.events (internally tagged on `type`, snake_case) ────────────

#[derive(Debug, Clone, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum ModerationWireEvent {
    EnforcementApplied(EnforcementWire),
    EnforcementReversed(EnforcementWire),
    // Every other moderation event (case_opened, appeal_resolved, …) deserializes
    // here and is ignored — search only cares about visibility transitions.
    #[serde(other)]
    Other,
}

#[derive(Debug, Clone, Deserialize)]
pub struct EnforcementWire {
    pub subject: SubjectWire,
    /// Moderation's `ActionType`, snake_case on the wire (e.g. "remove_content").
    #[serde(default)]
    pub action: Option<String>,
    pub occurred_at: chrono::DateTime<chrono::Utc>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct SubjectWire {
    /// Moderation's `EntityType`, snake_case on the wire (e.g. "post", "profile").
    pub entity_type: String,
    pub entity_id: String,
}

// ── profile.v1.events (internally tagged on `type`, PascalCase) ───────────────

#[derive(Debug, Clone, Deserialize)]
#[serde(tag = "type")]
pub enum ProfileWireEvent {
    ProfileCreated {
        profile_id: String,
        occurred_at_ms: i64,
    },
    ProfileUpdated {
        profile_id: String,
        occurred_at_ms: i64,
    },
    HandleChanged {
        profile_id: String,
        occurred_at_ms: i64,
    },
    ProfileVerified {
        profile_id: String,
        occurred_at_ms: i64,
    },
    ProfileHidden {
        profile_id: String,
        occurred_at_ms: i64,
    },
    ProfileRestored {
        profile_id: String,
        occurred_at_ms: i64,
    },
    ProfileDeleted {
        profile_id: String,
        occurred_at_ms: i64,
    },
    #[serde(other)]
    Unknown,
}
