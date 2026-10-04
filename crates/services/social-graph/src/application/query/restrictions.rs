use std::collections::HashSet;
use std::sync::Arc;

use chrono::{DateTime, Utc};
use cqrs::{Envelope, Query, QueryHandler};

use crate::application::port::{RestrictionRepository, MAX_RESTRICTION_CANDIDATES};
use crate::domain::value_object::ProfileId;
use crate::error::SocialGraphError;

/// The profiles the owner restricts, in profile-id order (its owner asks).
#[derive(Debug, Clone)]
pub struct ListRestrictedQuery {
    pub profile_id: String,
    pub limit:      u32,
    pub page_token: Option<String>,
}

impl Query for ListRestrictedQuery {
    type Response = (Vec<(ProfileId, DateTime<Utc>)>, Option<String>);
}

pub struct ListRestrictedHandler {
    restrictions: Arc<dyn RestrictionRepository>,
}

impl ListRestrictedHandler {
    pub fn new(restrictions: Arc<dyn RestrictionRepository>) -> Self {
        Self { restrictions }
    }
}

impl QueryHandler<ListRestrictedQuery> for ListRestrictedHandler {
    type Error = SocialGraphError;

    async fn handle(
        &self,
        envelope: Envelope<ListRestrictedQuery>,
    ) -> Result<(Vec<(ProfileId, DateTime<Utc>)>, Option<String>), Self::Error> {
        let q = &envelope.payload;
        let owner = ProfileId::try_from(q.profile_id.as_str())?;
        self.restrictions
            .list(&owner, q.limit.clamp(1, 100) as i32, q.page_token.as_deref())
            .await
    }
}

/// Which of `candidate_ids` the owner restricts (mesh: comment's read gate,
/// with a page's commenters on the owner's post).
#[derive(Debug, Clone)]
pub struct RestrictedAmongQuery {
    pub owner_id:      String,
    pub candidate_ids: Vec<String>,
}

impl Query for RestrictedAmongQuery {
    type Response = HashSet<ProfileId>;
}

pub struct RestrictedAmongHandler {
    restrictions: Arc<dyn RestrictionRepository>,
}

impl RestrictedAmongHandler {
    pub fn new(restrictions: Arc<dyn RestrictionRepository>) -> Self {
        Self { restrictions }
    }
}

impl QueryHandler<RestrictedAmongQuery> for RestrictedAmongHandler {
    type Error = SocialGraphError;

    async fn handle(&self, envelope: Envelope<RestrictedAmongQuery>) -> Result<HashSet<ProfileId>, Self::Error> {
        let q = &envelope.payload;
        if q.candidate_ids.len() > MAX_RESTRICTION_CANDIDATES {
            return Err(SocialGraphError::DomainViolation {
                field:   "candidate_ids".into(),
                message: format!("at most {MAX_RESTRICTION_CANDIDATES} candidates"),
            });
        }
        let owner = ProfileId::try_from(q.owner_id.as_str())?;
        let candidates = q
            .candidate_ids
            .iter()
            .map(|id| ProfileId::try_from(id.as_str()))
            .collect::<Result<Vec<_>, _>>()?;
        self.restrictions.restricted_among(&owner, &candidates).await
    }
}
