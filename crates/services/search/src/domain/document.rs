//! The indexable document model — what search actually stores per entity.
//!
//! Every field here must pass the litmus test: it is reconstructable by replaying
//! events or scanning the source-of-record. Documents hold only what is needed to
//! **match**, **rank**, and render a result row — never an authoritative copy of
//! the entity. Volatile/authoritative fields (live counts, signed URLs,
//! follow-state) are deliberately absent; the caller hydrates those.

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

use super::value_object::{AuthorId, DocVersion, EntityKind, PopularityScore, Searchable};

/// A document in one of the per-kind indices. The `version` drives external
/// versioning at write time; `searchable` is the moderation-visibility filter.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum IndexDocument {
    Profile(ProfileDoc),
    Post(PostDoc),
    Hashtag(HashtagDoc),
}

impl IndexDocument {
    pub fn kind(&self) -> EntityKind {
        match self {
            IndexDocument::Profile(_) => EntityKind::Profile,
            IndexDocument::Post(_) => EntityKind::Post,
            IndexDocument::Hashtag(_) => EntityKind::Hashtag,
        }
    }

    /// The document id (its `_id` in the engine) — the authoritative entity id, or
    /// the tag itself for hashtags.
    pub fn id(&self) -> &str {
        match self {
            IndexDocument::Profile(d) => &d.profile_id,
            IndexDocument::Post(d) => &d.post_id,
            IndexDocument::Hashtag(d) => &d.tag,
        }
    }

    pub fn version(&self) -> DocVersion {
        match self {
            IndexDocument::Profile(d) => d.version,
            IndexDocument::Post(d) => d.version,
            IndexDocument::Hashtag(d) => d.version,
        }
    }

    /// The responsible account, when the kind has one (profiles and posts). Drives
    /// query-time exclusion and GDPR purge; hashtags have no author.
    pub fn author_id(&self) -> Option<&AuthorId> {
        match self {
            IndexDocument::Profile(d) => Some(&d.author_id),
            IndexDocument::Post(d) => Some(&d.author_id),
            IndexDocument::Hashtag(_) => None,
        }
    }

    pub fn searchable(&self) -> Searchable {
        match self {
            IndexDocument::Profile(d) => d.searchable,
            IndexDocument::Post(d) => d.searchable,
            IndexDocument::Hashtag(d) => d.searchable,
        }
    }
}

/// A profile document. Matchable text: `handle`, `display_name`, `bio`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ProfileDoc {
    pub profile_id: String,
    /// A profile is its own author (its id), so block/mute exclusion and purge work
    /// uniformly across kinds.
    pub author_id: AuthorId,
    pub handle: String,
    pub display_name: String,
    pub bio: String,
    pub avatar_key: String,
    pub verified: bool,
    pub searchable: Searchable,
    pub popularity: PopularityScore,
    pub created_at: DateTime<Utc>,
    pub indexed_at: DateTime<Utc>,
    pub version: DocVersion,
}

/// A post document. Matchable text: `caption` and `hashtags`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct PostDoc {
    pub post_id: String,
    pub author_id: AuthorId,
    pub author_handle: String,
    pub caption: String,
    pub hashtags: Vec<String>,
    pub thumbnail_key: String,
    pub searchable: Searchable,
    pub popularity: PopularityScore,
    pub created_at: DateTime<Utc>,
    pub indexed_at: DateTime<Utc>,
    pub version: DocVersion,
    /// When the post leaves its author's post window (#664): it is not
    /// searchable from then on. `None` without a window.
    #[serde(default)]
    pub visible_until: Option<DateTime<Utc>>,
}

/// A hashtag document — the discoverable tag entity, maintained from the derived
/// hashtag stream. `post_count` is the coarse, periodic signal, not a live counter.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct HashtagDoc {
    pub tag: String,
    pub post_count: i64,
    pub searchable: Searchable,
    pub popularity: PopularityScore,
    pub indexed_at: DateTime<Utc>,
    pub version: DocVersion,
}
