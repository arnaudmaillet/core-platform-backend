//! The pure domain layer for search — the projection model and the transform that
//! turns inbound source events into index mutations.
//!
//! Search has no stateful aggregates (it is a derived read-model, not a
//! System-of-Record), so the centre of gravity is the [`projector`]: a clock-injected,
//! I/O-free `SourceEvent → IndexMutation` function. Everything here is deterministic
//! and unit-testable without containers.

pub mod audience;
pub mod document;
pub mod event;
pub mod mutation;
pub mod projector;
pub mod query;
pub mod recent_search;
pub mod value_object;

pub use audience::{ContentAccess, Viewer};
pub use document::{HashtagDoc, IndexDocument, PostDoc, ProfileDoc};
pub use event::{
    ComplianceEvent, EntityDeletion, HashtagEvent, HashtagSnapshot, ModerationEvent, PostEvent,
    PostSnapshot, ProfileEvent, ProfileSnapshot, SourceEvent, VisibilityChange,
};
pub use mutation::{IndexMutation, SkipReason};
pub use projector::project;
pub use query::{
    HitDisplay, SearchHit, SearchQuery, SearchResults, SuggestQuery, Suggestion, Suggestions,
};
pub use value_object::{
    AuthorId, DocVersion, EntityKind, PopularityScore, Searchable, SortStrategy, VisibilityAuthority,
};
