//! The output of the projector: a single, engine-agnostic index mutation.
//!
//! The Phase-4 adapter translates each variant into a concrete OpenSearch
//! operation (`Upsert` → external-versioned index; `Delete` → delete-by-id;
//! `PurgeByAuthor` → delete-by-query; `SetSearchable` → versioned partial update).
//! [`IndexMutation::Skip`] carries *intentional no-ops* (a non-indexable entity, an
//! irrelevant event) so the consumer folds them into `Ok` and commits the offset
//! rather than dead-lettering — per the Consumer Runtime Standard.

use super::document::IndexDocument;
use super::value_object::{AuthorId, DocVersion, EntityKind, Searchable, VisibilityAuthority};

#[derive(Debug, Clone, PartialEq)]
pub enum IndexMutation {
    /// Index or replace the document at `version` (external versioning rejects a
    /// non-newer write at the engine — the idempotency guard).
    Upsert(IndexDocument),

    /// Flip one authority's visibility flag on a document. Carries the `authority`
    /// (moderation vs owner) so the two flip independent fields, and a `version` so a
    /// stale flip can't overwrite a newer state of that same authority.
    SetSearchable {
        authority: VisibilityAuthority,
        kind: EntityKind,
        id: String,
        searchable: Searchable,
        version: DocVersion,
    },

    /// Hard-delete a single document by id.
    Delete { kind: EntityKind, id: String },

    /// Deep GDPR purge of everything an actor authored, across all indices.
    PurgeByAuthor { author_id: AuthorId },

    /// Recompute when each of the author's posts leaves their post window
    /// (#664): `created_at + window_days`, or never (`None`). Guarded by
    /// `version` so a stale window change can't overwrite a newer one.
    SetPostWindow {
        author_id: AuthorId,
        window_days: Option<u32>,
        version: DocVersion,
    },

    /// An intentional no-op — commit the offset, index nothing.
    Skip(SkipReason),
}

/// Why a source event produced no index change. All are benign and committed.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SkipReason {
    /// The moderated entity does not map to any search index.
    NotIndexable,
}
