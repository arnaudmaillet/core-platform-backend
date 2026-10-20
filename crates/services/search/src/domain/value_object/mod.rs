//! Pure value objects for the search projection model. No I/O, no clock reads
//! (time is injected as `DateTime<Utc>` parameters), no engine awareness.

pub mod doc_version;
pub mod entity_kind;
pub mod ids;
pub mod popularity;
pub mod post_format;
pub mod searchable;
pub mod sort;
pub mod visibility_authority;

pub use doc_version::DocVersion;
pub use entity_kind::EntityKind;
pub use ids::AuthorId;
pub use popularity::PopularityScore;
pub use post_format::{PostFormat, PostKindFilter};
pub use searchable::Searchable;
pub use sort::SortStrategy;
pub use visibility_authority::VisibilityAuthority;
