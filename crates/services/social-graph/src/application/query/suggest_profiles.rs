//! People one may know (#661): friends of friends — the profiles that the
//! profiles one follows follow — ranked by how many of them do.
//!
//! Left out: oneself, the profiles one already follows, a block either way,
//! private and hidden profiles (one does not follow them, so their content is
//! not visible), and anyone not announced suggestible — fails closed: a
//! profile whose settings were never projected is not suggested either. A
//! profile not known to be 18+ never turns it on (profile locks it), so a
//! teen is never suggested.
//!
//! Paged (#834) by a cursor on the ranking itself — `(mutual_count,
//! profile_id)` of the last profile examined — so each page resumes strictly
//! after it, with the same exclusions, however the ranking moved since.

use std::collections::HashMap;
use std::sync::Arc;

use base64::{engine::general_purpose::URL_SAFE_NO_PAD, Engine as _};
use cqrs::{Envelope, Query, QueryHandler};
use futures::{StreamExt, TryStreamExt};
use uuid::Uuid;

use crate::application::port::SocialGraphRepository;
use crate::domain::access::ContentAccess;
use crate::domain::value_object::ProfileId;
use crate::error::SocialGraphError;

/// Followees read to find friends of friends.
const MAX_SOURCES: i32 = 100;
/// Followees read per source.
const PER_SOURCE: i32 = 200;
/// Candidates per access check (social-graph's bulk target cap).
const CHECK_BATCH: usize = 100;
/// Access checks per page: a page stops short (with a token) rather than
/// check more, when most candidates are filtered out.
const MAX_CHECKS: usize = 5;
/// Source reads in flight.
const READS_IN_FLIGHT: usize = 16;
/// Most suggestions per call.
pub const MAX_SUGGESTIONS: i32 = 50;

pub struct SuggestProfilesQuery {
    pub profile_id: String,
    pub limit:      i32,
    /// The previous page's `next_page_token`; `None` for the first page.
    pub page_token: Option<String>,
}

/// A suggested profile and how many of the profiles one follows follow it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Suggestion {
    pub profile_id:   ProfileId,
    pub mutual_count: u32,
}

/// One page of suggestions.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct SuggestionPage {
    pub suggestions:     Vec<Suggestion>,
    /// `None` on the last page.
    pub next_page_token: Option<String>,
}

impl Query for SuggestProfilesQuery {
    type Response = SuggestionPage;
}

/// A position in the ranking: most mutual followees first, then by id.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct Cursor {
    mutual_count: u32,
    profile_id:   Uuid,
}

impl Cursor {
    fn of(&(profile_id, mutual_count): &(ProfileId, u32)) -> Self {
        Self { mutual_count, profile_id: profile_id.as_uuid() }
    }

    /// Whether `candidate` ranks strictly after this position.
    fn precedes(&self, candidate: &(ProfileId, u32)) -> bool {
        candidate.1 < self.mutual_count
            || (candidate.1 == self.mutual_count && candidate.0.as_uuid() > self.profile_id)
    }

    fn encode(&self) -> String {
        URL_SAFE_NO_PAD.encode(format!("{}:{}", self.mutual_count, self.profile_id))
    }

    fn decode(token: &str) -> Result<Self, SocialGraphError> {
        let invalid = || SocialGraphError::DomainViolation {
            field:   "page_token".to_owned(),
            message: "invalid suggestions page token".to_owned(),
        };
        let raw = String::from_utf8(URL_SAFE_NO_PAD.decode(token).map_err(|_| invalid())?).map_err(|_| invalid())?;
        let (count, id) = raw.split_once(':').ok_or_else(invalid)?;
        Ok(Self {
            mutual_count: count.parse().map_err(|_| invalid())?,
            profile_id:   Uuid::parse_str(id).map_err(|_| invalid())?,
        })
    }
}

/// Ranks the candidates: most mutual followees first, ties by id.
fn rank(counts: HashMap<ProfileId, u32>) -> Vec<(ProfileId, u32)> {
    let mut ranked: Vec<(ProfileId, u32)> = counts.into_iter().collect();
    ranked.sort_by(|a, b| b.1.cmp(&a.1).then(a.0.as_uuid().cmp(&b.0.as_uuid())));
    ranked
}

pub struct SuggestProfilesHandler {
    repo: Arc<dyn SocialGraphRepository>,
}

impl SuggestProfilesHandler {
    pub fn new(repo: Arc<dyn SocialGraphRepository>) -> Self {
        Self { repo }
    }
}

impl QueryHandler<SuggestProfilesQuery> for SuggestProfilesHandler {
    type Error = SocialGraphError;

    async fn handle(&self, envelope: Envelope<SuggestProfilesQuery>) -> Result<SuggestionPage, SocialGraphError> {
        let q = &envelope.payload;
        let me = ProfileId::try_from(q.profile_id.as_str())?;
        let limit = q.limit.clamp(1, MAX_SUGGESTIONS) as usize;
        let after = q.page_token.as_deref().filter(|t| !t.is_empty()).map(Cursor::decode).transpose()?;

        let (following, _) = self.repo.list_following(&me, MAX_SOURCES, None).await?;
        let sources: Vec<ProfileId> = following.iter().map(|source| source.profile_id).collect();
        let repo = &self.repo;
        let lists: Vec<_> = futures::stream::iter(sources)
            .map(|source| async move { repo.list_following(&source, PER_SOURCE, None).await.map(|(edges, _)| edges) })
            .buffer_unordered(READS_IN_FLIGHT)
            .try_collect()
            .await?;
        let mut counts: HashMap<ProfileId, u32> = HashMap::new();
        for edge in lists.into_iter().flatten() {
            *counts.entry(edge.profile_id).or_default() += 1;
        }
        counts.remove(&me);
        for followed in &following {
            counts.remove(&followed.profile_id);
        }

        let ranked = rank(counts);
        let start = after.map_or(0, |cursor| ranked.partition_point(|c| !cursor.precedes(c)));
        let remaining = &ranked[start..];

        let mut page = SuggestionPage::default();
        let mut examined = 0;
        for batch in remaining.chunks(CHECK_BATCH).take(MAX_CHECKS) {
            let candidates: Vec<ProfileId> = batch.iter().map(|(id, _)| *id).collect();
            let facts = self.repo.load_access_facts(std::slice::from_ref(&me), &candidates).await?;
            for &(id, mutual_count) in batch {
                examined += 1;
                let shown = facts.access(std::slice::from_ref(&me), &id) == ContentAccess::Visible
                    && !facts.private.contains(&id)
                    && facts.suggestible.contains(&id)
                    && !facts.follows.contains(&(me, id));
                if shown {
                    page.suggestions.push(Suggestion { profile_id: id, mutual_count });
                    if page.suggestions.len() == limit {
                        break;
                    }
                }
            }
            if page.suggestions.len() == limit {
                break;
            }
        }
        // More ranked after the last profile examined: the next page resumes there.
        if examined < remaining.len() {
            page.next_page_token = Some(Cursor::of(&remaining[examined - 1]).encode());
        }
        Ok(page)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn profile(n: u128) -> ProfileId {
        ProfileId::from_uuid(Uuid::from_u128(n))
    }

    #[test]
    fn ranking_is_mutual_count_then_id() {
        let counts = HashMap::from([(profile(3), 1), (profile(1), 2), (profile(2), 1), (profile(4), 5)]);
        let ranked: Vec<_> = rank(counts).into_iter().map(|(id, _)| id).collect();
        assert_eq!(ranked, vec![profile(4), profile(1), profile(2), profile(3)]);
    }

    #[test]
    fn a_cursor_resumes_strictly_after_its_position() {
        let ranked = rank(HashMap::from([(profile(4), 5), (profile(1), 2), (profile(2), 1), (profile(3), 1)]));
        let cursor = Cursor::of(&(profile(2), 1));
        let start = ranked.partition_point(|c| !cursor.precedes(c));
        assert_eq!(ranked[start..].iter().map(|(id, _)| *id).collect::<Vec<_>>(), vec![profile(3)]);

        // The cursor's profile moved up since: it is not shown again, and
        // nothing ranked after its old position is skipped.
        let moved = rank(HashMap::from([(profile(2), 9), (profile(3), 1), (profile(5), 1)]));
        let start = moved.partition_point(|c| !cursor.precedes(c));
        assert_eq!(moved[start..].iter().map(|(id, _)| *id).collect::<Vec<_>>(), vec![profile(3), profile(5)]);
    }

    #[test]
    fn tokens_round_trip_and_garbage_is_refused() {
        let cursor = Cursor { mutual_count: 7, profile_id: Uuid::now_v7() };
        assert_eq!(Cursor::decode(&cursor.encode()).unwrap(), cursor);
        for bad in ["%%%", "bm9wZQ", &URL_SAFE_NO_PAD.encode("x:y"), &URL_SAFE_NO_PAD.encode("1:not-a-uuid")] {
            assert!(Cursor::decode(bad).is_err(), "{bad:?}");
        }
    }
}
