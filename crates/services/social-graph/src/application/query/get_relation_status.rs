use std::sync::Arc;

use cqrs::{Envelope, Query, QueryHandler};

use crate::application::port::{
    MuteRepository, RelationCounts, RestrictionRepository, SocialGraphCache, SocialGraphRepository,
};
use crate::domain::mute::MuteScopes;
use crate::domain::value_object::{ProfileId, RelationStatus};
use crate::error::SocialGraphError;

/// Query: retrieve the full bidirectional relationship status between two profiles,
/// together with the target profile's follower and following counts.
///
/// Read path:
///   1. ScyllaDB `load_relation` (4 concurrent point-lookups) → follow/block state.
///   2. Redis `get_counts` → target's follower/following counters.
///
/// ScyllaDB is the authoritative source for relationship state. Redis serves
/// the counts, which degrade gracefully to zero on cold cache (Redis restart).
#[derive(Debug, Clone)]
pub struct GetRelationStatusQuery {
    pub actor_id:  String,
    pub target_id: String,
}

impl Query for GetRelationStatusQuery {
    type Response = RelationStatusView;
}

#[derive(Debug, Clone)]
pub struct RelationStatusView {
    pub actor_id:               ProfileId,
    pub target_id:              ProfileId,
    pub status:                 RelationStatus,
    pub target_followers_count: i64,
    pub target_following_count: i64,
    /// How the actor mutes the target (empty when it does not).
    pub muted:                  MuteScopes,
    /// The actor restricts the target.
    pub restricted:             bool,
}

pub struct GetRelationStatusHandler {
    repo:  Arc<dyn SocialGraphRepository>,
    cache: Arc<dyn SocialGraphCache>,
    mutes: Arc<dyn MuteRepository>,
    restrictions: Arc<dyn RestrictionRepository>,
}

impl GetRelationStatusHandler {
    pub fn new(
        repo: Arc<dyn SocialGraphRepository>,
        cache: Arc<dyn SocialGraphCache>,
        mutes: Arc<dyn MuteRepository>,
        restrictions: Arc<dyn RestrictionRepository>,
    ) -> Self {
        Self { repo, cache, mutes, restrictions }
    }
}

impl QueryHandler<GetRelationStatusQuery> for GetRelationStatusHandler {
    type Error = SocialGraphError;

    async fn handle(
        &self,
        envelope: Envelope<GetRelationStatusQuery>,
    ) -> Result<RelationStatusView, Self::Error> {
        let q = &envelope.payload;

        let actor_id  = ProfileId::try_from(q.actor_id.as_str())?;
        let target_id = ProfileId::try_from(q.target_id.as_str())?;

        // Fire ScyllaDB and Redis queries concurrently.
        let target = [target_id];
        let (relation, counts, muted, restricted) = tokio::join!(
            self.repo.load_relation(&actor_id, &target_id),
            self.cache.get_counts(&target_id),
            self.mutes.scopes(&actor_id, &target_id),
            self.restrictions.restricted_among(&actor_id, &target),
        );

        let relation = relation?;
        let muted = muted?;
        let restricted = !restricted?.is_empty();
        let counts: RelationCounts = counts.unwrap_or_default();

        Ok(RelationStatusView {
            actor_id,
            target_id,
            status:                 relation.status(),
            target_followers_count: counts.followers,
            target_following_count: counts.following,
            muted,
            restricted,
        })
    }
}
