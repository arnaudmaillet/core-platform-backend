//! Read-side value objects: the validated query inputs and the ranked result
//! shapes the application layer (Phase 3) and the engine adapter (Phase 4) speak,
//! mapped to/from proto at the edge (Phase 5).

use chrono::{DateTime, Utc};

use super::value_object::{AuthorId, EntityKind, SortStrategy};
use crate::error::SearchError;

/// Hard ceiling on a page so a caller can't ask the engine for an unbounded scan.
pub const MAX_PAGE_SIZE: u32 = 50;
/// Applied when the caller passes `0`.
pub const DEFAULT_PAGE_SIZE: u32 = 20;
/// Ceiling on autocomplete fan-out.
pub const MAX_SUGGEST_LIMIT: u32 = 10;

/// A validated federated search request.
#[derive(Debug, Clone, PartialEq)]
pub struct SearchQuery {
    pub text: String,
    /// Empty ⇒ all kinds (federated).
    pub kinds: Vec<EntityKind>,
    pub sort: SortStrategy,
    pub page_size: u32,
    pub page_token: Option<String>,
    /// Caller-resolved block/mute exclusions — search never indexes these.
    pub exclude_author_ids: Vec<AuthorId>,
}

impl SearchQuery {
    /// Validate and normalize. Empty query text is rejected (`SCH-1001`); page size
    /// is clamped to `[1, MAX_PAGE_SIZE]` with `0` defaulting.
    pub fn new(
        text: impl Into<String>,
        kinds: Vec<EntityKind>,
        sort: SortStrategy,
        page_size: u32,
        page_token: Option<String>,
        exclude_author_ids: Vec<AuthorId>,
    ) -> Result<Self, SearchError> {
        let text = text.into();
        if text.trim().is_empty() {
            return Err(SearchError::InvalidQuery {
                reason: "query text must not be empty".to_owned(),
            });
        }
        Ok(Self {
            text,
            kinds,
            sort,
            page_size: clamp_page(page_size),
            page_token,
            exclude_author_ids,
        })
    }
}

/// A validated autocomplete request.
#[derive(Debug, Clone, PartialEq)]
pub struct SuggestQuery {
    pub prefix: String,
    pub kinds: Vec<EntityKind>,
    pub limit: u32,
}

impl SuggestQuery {
    pub fn new(
        prefix: impl Into<String>,
        kinds: Vec<EntityKind>,
        limit: u32,
    ) -> Result<Self, SearchError> {
        let prefix = prefix.into();
        if prefix.trim().is_empty() {
            return Err(SearchError::InvalidQuery {
                reason: "suggest prefix must not be empty".to_owned(),
            });
        }
        let limit = match limit {
            0 => MAX_SUGGEST_LIMIT,
            n => n.min(MAX_SUGGEST_LIMIT),
        };
        Ok(Self {
            prefix,
            kinds,
            limit,
        })
    }
}

fn clamp_page(requested: u32) -> u32 {
    match requested {
        0 => DEFAULT_PAGE_SIZE,
        n => n.min(MAX_PAGE_SIZE),
    }
}

// ── Results ───────────────────────────────────────────────────────────────────

/// A ranked result — a reference plus a minimal display projection, never a
/// hydrated entity.
#[derive(Debug, Clone, PartialEq)]
pub struct SearchHit {
    pub kind: EntityKind,
    pub id: String,
    pub score: f32,
    pub snippet: String,
    pub display: HitDisplay,
}

#[derive(Debug, Clone, PartialEq)]
pub enum HitDisplay {
    Profile {
        handle: String,
        display_name: String,
        avatar_key: String,
        /// #913; empty: none.
        avatar_frame_id: String,
        verified: bool,
    },
    Post {
        author_id: String,
        author_handle: String,
        thumbnail_key: String,
        created_at: DateTime<Utc>,
    },
    Hashtag {
        tag: String,
        post_count: i64,
    },
}

/// A page of results. `degraded` is the fail-open marker: the engine returned what
/// it could under partial availability rather than erroring the page.
#[derive(Debug, Clone, PartialEq)]
pub struct SearchResults {
    pub hits: Vec<SearchHit>,
    pub next_page_token: Option<String>,
    pub estimated_total: u64,
    pub degraded: bool,
}

#[derive(Debug, Clone, PartialEq)]
pub struct Suggestion {
    pub kind: EntityKind,
    pub text: String,
    pub id: Option<String>,
    pub score: f32,
}

#[derive(Debug, Clone, PartialEq, Default)]
pub struct Suggestions {
    pub suggestions: Vec<Suggestion>,
}

#[cfg(test)]
mod tests {
    use error::AppError;

    use super::*;

    #[test]
    fn rejects_empty_query_text() {
        let err = SearchQuery::new("  ", vec![], SortStrategy::Relevance, 10, None, vec![])
            .unwrap_err();
        assert_eq!(err.error_code(), "SCH-1001");
    }

    #[test]
    fn page_size_zero_defaults() {
        let q = SearchQuery::new("rust", vec![], SortStrategy::Relevance, 0, None, vec![]).unwrap();
        assert_eq!(q.page_size, DEFAULT_PAGE_SIZE);
    }

    #[test]
    fn page_size_is_clamped_to_max() {
        let q =
            SearchQuery::new("rust", vec![], SortStrategy::Relevance, 9999, None, vec![]).unwrap();
        assert_eq!(q.page_size, MAX_PAGE_SIZE);
    }

    #[test]
    fn suggest_limit_clamps_and_defaults() {
        assert_eq!(
            SuggestQuery::new("al", vec![], 0).unwrap().limit,
            MAX_SUGGEST_LIMIT
        );
        assert_eq!(
            SuggestQuery::new("al", vec![], 100).unwrap().limit,
            MAX_SUGGEST_LIMIT
        );
        assert_eq!(SuggestQuery::new("al", vec![], 3).unwrap().limit, 3);
    }

    #[test]
    fn rejects_empty_prefix() {
        let err = SuggestQuery::new("", vec![], 5).unwrap_err();
        assert_eq!(err.error_code(), "SCH-1001");
    }
}
