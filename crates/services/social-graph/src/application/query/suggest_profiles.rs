//! People one may know (#661): friends of friends — the profiles that the
//! profiles one follows follow — ranked by how many of them do.
//!
//! Left out: oneself, the profiles one already follows, a block either way,
//! private and hidden profiles (one does not follow them, so their content is
//! not visible), and anyone not announced suggestible — fails closed: a
//! profile whose settings were never projected is not suggested either. A
//! profile not known to be 18+ never turns it on (profile locks it), so a
//! teen is never suggested.

use std::collections::HashMap;
use std::sync::Arc;

use cqrs::{Envelope, Query, QueryHandler};
use futures::{StreamExt, TryStreamExt};

use crate::application::port::SocialGraphRepository;
use crate::domain::access::ContentAccess;
use crate::domain::value_object::ProfileId;
use crate::error::SocialGraphError;

/// Followees read to find friends of friends.
const MAX_SOURCES: i32 = 100;
/// Followees read per source.
const PER_SOURCE: i32 = 200;
/// Candidates checked (one bulk access check: social-graph's target cap).
const MAX_CANDIDATES: usize = 100;
/// Source reads in flight.
const READS_IN_FLIGHT: usize = 16;
/// Most suggestions per call.
pub const MAX_SUGGESTIONS: i32 = 50;

pub struct SuggestProfilesQuery {
    pub profile_id: String,
    pub limit:      i32,
}

/// A suggested profile and how many of the profiles one follows follow it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Suggestion {
    pub profile_id:   ProfileId,
    pub mutual_count: u32,
}

impl Query for SuggestProfilesQuery {
    type Response = Vec<Suggestion>;
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

    async fn handle(&self, envelope: Envelope<SuggestProfilesQuery>) -> Result<Vec<Suggestion>, SocialGraphError> {
        let q = &envelope.payload;
        let me = ProfileId::try_from(q.profile_id.as_str())?;
        let limit = q.limit.clamp(1, MAX_SUGGESTIONS) as usize;

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

        let mut ranked: Vec<(ProfileId, u32)> = counts.into_iter().collect();
        ranked.sort_by(|a, b| b.1.cmp(&a.1).then(a.0.as_uuid().cmp(&b.0.as_uuid())));
        ranked.truncate(MAX_CANDIDATES);
        if ranked.is_empty() {
            return Ok(Vec::new());
        }

        let candidates: Vec<ProfileId> = ranked.iter().map(|(id, _)| *id).collect();
        let facts = self.repo.load_access_facts(std::slice::from_ref(&me), &candidates).await?;
        Ok(ranked
            .into_iter()
            .filter(|(id, _)| {
                facts.access(std::slice::from_ref(&me), id) == ContentAccess::Visible
                    && !facts.private.contains(id)
                    && facts.suggestible.contains(id)
                    && !facts.follows.contains(&(me, *id))
            })
            .take(limit)
            .map(|(profile_id, mutual_count)| Suggestion { profile_id, mutual_count })
            .collect())
    }
}
