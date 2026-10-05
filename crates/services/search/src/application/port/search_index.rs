use async_trait::async_trait;

use crate::domain::{
    AuthorId, DocVersion, EntityKind, IndexDocument, Searchable, SearchQuery, SearchResults,
    SuggestQuery, Suggestions, VisibilityAuthority,
};
use crate::error::SearchError;

/// The result of a version-guarded write. A `RejectedStale` is **not** an error —
/// it means the engine's external-version guard saw a newer revision already
/// stored and declined the write. The consumer commits the offset regardless.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WriteOutcome {
    Applied,
    RejectedStale,
}

/// The inverted index — the one port the read and write paths share.
///
/// Independent version namespaces guard one document. `upsert` carries the
/// **content** version (from the source revision) and replaces the matchable /
/// display fields; it MUST preserve already-stored visibility (a content
/// re-projection never un-hides a document). `set_searchable` carries a
/// **visibility** version (from the event time) and updates only that authority's
/// flag — `moderation` and `owner` are separate fields with separate guards, so a
/// document is searchable only when *both* permit it, and neither authority can
/// override the other. Keeping the guards separate is what lets content edits and
/// visibility flips — which arrive on different topics with unrelated version
/// timelines — interleave correctly, in any order.
#[async_trait]
pub trait SearchIndex: Send + Sync + 'static {
    /// Index or replace a document's content, guarded by its content version.
    /// First index seeds both visibility flags from `document.searchable()`; a
    /// re-index preserves the stored visibility.
    async fn upsert(&self, document: &IndexDocument) -> Result<WriteOutcome, SearchError>;

    /// Update one `authority`'s visibility flag, guarded by that authority's
    /// version. If the document is not yet indexed (a flip that raced ahead of the
    /// content event), the intent is recorded so a later `upsert` honours it.
    async fn set_searchable(
        &self,
        authority: VisibilityAuthority,
        kind: EntityKind,
        id: &str,
        searchable: Searchable,
        version: DocVersion,
    ) -> Result<WriteOutcome, SearchError>;

    /// Hard-delete a single document by id. Idempotent — deleting an absent id is a
    /// no-op success.
    async fn delete(&self, kind: EntityKind, id: &str) -> Result<(), SearchError>;

    /// GDPR deep purge: remove every document authored by `author_id` across all
    /// indices, with no retained tombstone. Returns the number removed.
    async fn purge_by_author(&self, author_id: &AuthorId) -> Result<u64, SearchError>;

    /// Apply an author's post window (#664) to every post of theirs already
    /// indexed: each leaves search at `created_at + window_days` (never with
    /// `None`). Posts are kept, so widening the window brings them back. Guarded
    /// per document by `version`. Returns the number of posts updated.
    async fn set_post_window(
        &self,
        author_id: &AuthorId,
        window_days: Option<u32>,
        version: DocVersion,
    ) -> Result<u64, SearchError>;

    /// Run a federated query. Implementations fail OPEN — on partial/unavailable
    /// shards they return what they can with `SearchResults.degraded = true` rather
    /// than erroring.
    async fn search(&self, query: &SearchQuery) -> Result<SearchResults, SearchError>;

    /// Prefix autocomplete.
    async fn suggest(&self, query: &SuggestQuery) -> Result<Suggestions, SearchError>;
}
