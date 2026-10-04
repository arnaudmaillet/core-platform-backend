//! Viewer-aware results: hits whose author the reader may not see are dropped,
//! using social-graph's access check on the page's authors (one bulk call).
//!
//! - a **post** hit stays only when its author is `Visible` (not a private
//!   author the reader does not follow, no block, not hidden);
//! - a **profile** hit stays unless it is `Hidden` (a private profile is still
//!   findable by its header, as on its own page);
//! - hashtags have no author and always stay.
//!
//! If the check fails, search stays up (TIER-1 fail-open) but shows nothing it
//! could not check: profile and post hits are dropped, hashtags kept, and the
//! response is marked `degraded`. The filter can shorten a page; the cursor is
//! unchanged.

use std::collections::{BTreeSet, HashMap};

use crate::application::port::AudienceGate;
use crate::domain::{ContentAccess, EntityKind, HitDisplay, SearchHit, SearchResults, Suggestions, Viewer};

fn author_of(hit: &SearchHit) -> Option<&str> {
    match &hit.display {
        HitDisplay::Post { author_id, .. } => Some(author_id),
        HitDisplay::Profile { .. } => Some(&hit.id),
        HitDisplay::Hashtag { .. } => None,
    }
}

fn keeps(kind: EntityKind, access: Option<ContentAccess>) -> bool {
    match kind {
        EntityKind::Hashtag => true,
        EntityKind::Post => access == Some(ContentAccess::Visible),
        EntityKind::Profile => matches!(access, Some(ContentAccess::Visible | ContentAccess::HeaderOnly)),
    }
}

/// The access answers for `targets`, or `None` when the check is unavailable.
async fn answers(
    gate: &dyn AudienceGate,
    viewers: &[String],
    targets: BTreeSet<String>,
) -> Option<HashMap<String, ContentAccess>> {
    if targets.is_empty() {
        return Some(HashMap::new());
    }
    let targets: Vec<String> = targets.into_iter().collect();
    match gate.access(viewers, &targets).await {
        Ok(answers) => Some(answers),
        Err(error) => {
            tracing::warn!(%error, "search audience check unavailable; dropping unchecked hits");
            None
        }
    }
}

/// Filters a page of search hits for `viewer` (see the module docs).
pub async fn filter_results(gate: &dyn AudienceGate, viewer: &Viewer, mut results: SearchResults) -> SearchResults {
    let Viewer::Profiles(viewers) = viewer else {
        return results;
    };
    let targets = results.hits.iter().filter_map(author_of).map(str::to_owned).collect();
    match answers(gate, viewers, targets).await {
        Some(answers) => results.hits.retain(|hit| {
            let access = author_of(hit).and_then(|a| answers.get(a).copied());
            keeps(hit.kind, access)
        }),
        None => {
            results.hits.retain(|hit| hit.kind == EntityKind::Hashtag);
            results.degraded = true;
        }
    }
    results
}

/// Filters suggestions for `viewer`: a profile suggestion follows the profile
/// rule; hashtags stay; any other suggestion (it names no author) is dropped
/// for a client, as unverifiable.
pub async fn filter_suggestions(gate: &dyn AudienceGate, viewer: &Viewer, mut suggestions: Suggestions) -> Suggestions {
    let Viewer::Profiles(viewers) = viewer else {
        return suggestions;
    };
    let targets = suggestions
        .suggestions
        .iter()
        .filter(|s| s.kind == EntityKind::Profile)
        .filter_map(|s| s.id.clone())
        .collect();
    let answers = answers(gate, viewers, targets).await;
    suggestions.suggestions.retain(|s| match s.kind {
        EntityKind::Hashtag => true,
        EntityKind::Profile => match (&answers, &s.id) {
            (Some(answers), Some(id)) => keeps(EntityKind::Profile, answers.get(id).copied()),
            _ => false,
        },
        EntityKind::Post => false,
    });
    suggestions
}

#[cfg(test)]
mod tests {
    use async_trait::async_trait;
    use chrono::Utc;

    use super::*;
    use crate::domain::Suggestion;
    use crate::error::SearchError;

    struct Gate(Option<HashMap<String, ContentAccess>>);

    #[async_trait]
    impl AudienceGate for Gate {
        async fn access(&self, _: &[String], _: &[String]) -> Result<HashMap<String, ContentAccess>, SearchError> {
            self.0.clone().ok_or(SearchError::EngineUnavailable)
        }
    }

    fn post(id: &str, author: &str) -> SearchHit {
        SearchHit {
            kind: EntityKind::Post,
            id: id.into(),
            score: 1.0,
            snippet: String::new(),
            display: HitDisplay::Post {
                author_id: author.into(),
                author_handle: String::new(),
                thumbnail_key: String::new(),
                created_at: Utc::now(),
            },
        }
    }

    fn profile(id: &str) -> SearchHit {
        SearchHit {
            kind: EntityKind::Profile,
            id: id.into(),
            score: 1.0,
            snippet: String::new(),
            display: HitDisplay::Profile {
                handle: id.into(),
                display_name: String::new(),
                avatar_key: String::new(),
                verified: false,
            },
        }
    }

    fn hashtag() -> SearchHit {
        SearchHit {
            kind: EntityKind::Hashtag,
            id: "rust".into(),
            score: 1.0,
            snippet: String::new(),
            display: HitDisplay::Hashtag { tag: "rust".into(), post_count: 3 },
        }
    }

    fn results(hits: Vec<SearchHit>) -> SearchResults {
        SearchResults { hits, next_page_token: Some("next".into()), estimated_total: 9, degraded: false }
    }

    fn ids(r: &SearchResults) -> Vec<&str> {
        r.hits.iter().map(|h| h.id.as_str()).collect()
    }

    fn page() -> Vec<SearchHit> {
        vec![
            post("p-open", "open"),
            post("p-private", "private"),
            post("p-blocked", "blocked"),
            post("p-unknown", "unknown"),
            profile("open"),
            profile("private"),
            profile("blocked"),
            hashtag(),
        ]
    }

    fn gate_up() -> Gate {
        Gate(Some(HashMap::from([
            ("open".into(), ContentAccess::Visible),
            ("private".into(), ContentAccess::HeaderOnly),
            ("blocked".into(), ContentAccess::Hidden),
        ])))
    }

    #[tokio::test]
    async fn only_visible_authors_posts_and_non_hidden_profiles_stay() {
        let filtered = filter_results(&gate_up(), &Viewer::Profiles(vec!["me".into()]), results(page())).await;
        assert_eq!(ids(&filtered), vec!["p-open", "open", "private", "rust"]);
        assert!(!filtered.degraded);
        assert_eq!(filtered.next_page_token.as_deref(), Some("next"), "cursor unchanged");
    }

    #[tokio::test]
    async fn the_mesh_is_not_filtered() {
        let filtered = filter_results(&Gate(None), &Viewer::Internal, results(page())).await;
        assert_eq!(filtered.hits.len(), 8);
    }

    #[tokio::test]
    async fn an_unavailable_check_keeps_only_hashtags_and_flags_degraded() {
        let filtered = filter_results(&Gate(None), &Viewer::Profiles(vec![]), results(page())).await;
        assert_eq!(ids(&filtered), vec!["rust"]);
        assert!(filtered.degraded);
    }

    #[tokio::test]
    async fn suggestions_follow_the_profile_rule() {
        let s = |kind, id: Option<&str>| Suggestion { kind, text: "t".into(), id: id.map(str::to_owned), score: 1.0 };
        let input = Suggestions {
            suggestions: vec![
                s(EntityKind::Profile, Some("private")),
                s(EntityKind::Profile, Some("blocked")),
                s(EntityKind::Hashtag, None),
                s(EntityKind::Post, Some("p-1")),
            ],
        };
        let out = filter_suggestions(&gate_up(), &Viewer::Profiles(vec![]), input.clone()).await;
        let kept: Vec<_> = out.suggestions.iter().map(|s| (s.kind, s.id.clone())).collect();
        assert_eq!(kept, vec![(EntityKind::Profile, Some("private".into())), (EntityKind::Hashtag, None)]);

        let down = filter_suggestions(&Gate(None), &Viewer::Profiles(vec![]), input).await;
        assert_eq!(down.suggestions.len(), 1, "hashtags only");
    }
}
