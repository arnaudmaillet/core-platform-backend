use std::collections::HashSet;

use async_trait::async_trait;
use chrono::{DateTime, Utc};

use crate::domain::value_object::ProfileId;
use crate::error::SocialGraphError;

/// At most this many candidates per `restricted_among` call.
pub const MAX_RESTRICTION_CANDIDATES: usize = 100;

/// Persistence port for restrictions (`social_graph.restrictions`).
#[async_trait]
pub trait RestrictionRepository: Send + Sync + 'static {
    /// Records that `owner` restricts `restricted` (re-recording is fine).
    async fn add(&self, owner: &ProfileId, restricted: &ProfileId, at: DateTime<Utc>) -> Result<(), SocialGraphError>;

    /// Lifts it (absent is fine).
    async fn remove(&self, owner: &ProfileId, restricted: &ProfileId) -> Result<(), SocialGraphError>;

    /// The profiles `owner` restricts, in profile-id order, with when. The
    /// page token is the last profile id returned.
    async fn list(
        &self,
        owner: &ProfileId,
        limit: i32,
        page_token: Option<&str>,
    ) -> Result<(Vec<(ProfileId, DateTime<Utc>)>, Option<String>), SocialGraphError>;

    /// The subset of `candidates` (≤ [`MAX_RESTRICTION_CANDIDATES`]) that
    /// `owner` restricts.
    async fn restricted_among(&self, owner: &ProfileId, candidates: &[ProfileId]) -> Result<HashSet<ProfileId>, SocialGraphError>;
}
