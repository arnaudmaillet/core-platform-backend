//! A profile's recent searches (#663): what it searched for, kept server-side
//! so every device shows the same list, and clearable.

use chrono::{DateTime, Duration, Utc};

/// Most searches kept per profile.
pub const MAX_RECENT_SEARCHES: usize = 50;
/// How long one is kept.
pub const RECENT_SEARCH_RETENTION: Duration = Duration::days(90);
/// Longest query kept (characters).
pub const MAX_QUERY_CHARS: usize = 100;

/// One recent search: the query as typed (normalized), and when.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RecentSearch {
    pub query: String,
    pub at:    DateTime<Utc>,
}

/// The query as kept: trimmed, inner whitespace collapsed, at most
/// [`MAX_QUERY_CHARS`]; `None` when nothing is left.
pub fn normalize_query(raw: &str) -> Option<String> {
    let collapsed = raw.split_whitespace().collect::<Vec<_>>().join(" ");
    let kept: String = collapsed.chars().take(MAX_QUERY_CHARS).collect();
    (!kept.is_empty()).then_some(kept)
}

/// The key a query is deduplicated by: case-insensitive.
pub fn query_key(query: &str) -> String {
    query.to_lowercase()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn queries_are_trimmed_collapsed_capped_and_keyed_case_insensitively() {
        assert_eq!(normalize_query("  hello   world \n").as_deref(), Some("hello world"));
        assert_eq!(normalize_query("   "), None);
        assert_eq!(normalize_query(&"é".repeat(150)).unwrap().chars().count(), MAX_QUERY_CHARS);
        assert_eq!(query_key("Café Paris"), query_key("café paris"));
    }
}
