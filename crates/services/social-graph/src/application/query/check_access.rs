use std::sync::Arc;

use cqrs::{Envelope, Query, QueryHandler};

use crate::application::port::SocialGraphRepository;
use crate::domain::access::{ContentAccess, Relationship};
use crate::domain::value_object::ProfileId;
use crate::error::SocialGraphError;

/// Upper bound on a caller's profiles (an account's `pids`).
pub const MAX_VIEWERS: usize = 20;
/// Upper bound on targets per call: a page of authors.
pub const MAX_TARGETS: usize = 100;

/// What `viewer_profile_ids` (the profiles a caller's account owns; empty for
/// an anonymous caller) may see of each target's content. Mesh-only: services
/// answering a client read call it with the reader they took from the edge.
#[derive(Debug, Clone)]
pub struct CheckAccessQuery {
    pub viewer_profile_ids: Vec<String>,
    pub target_profile_ids: Vec<String>,
}

impl Query for CheckAccessQuery {
    type Response = Vec<TargetAnswer>;
}

/// One target's answer: the content access, and how the viewers relate to
/// it (an author's location audience, #657).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TargetAnswer {
    pub target:   ProfileId,
    pub access:   ContentAccess,
    pub relation: Relationship,
}

pub struct CheckAccessHandler {
    repo: Arc<dyn SocialGraphRepository>,
}

impl CheckAccessHandler {
    pub fn new(repo: Arc<dyn SocialGraphRepository>) -> Self {
        Self { repo }
    }
}

fn parse(ids: &[String], field: &str, max: usize) -> Result<Vec<ProfileId>, SocialGraphError> {
    if ids.len() > max {
        return Err(SocialGraphError::DomainViolation {
            field:   field.to_owned(),
            message: format!("at most {max} ids per call"),
        });
    }
    ids.iter().map(|id| ProfileId::try_from(id.as_str())).collect()
}

impl QueryHandler<CheckAccessQuery> for CheckAccessHandler {
    type Error = SocialGraphError;

    async fn handle(
        &self,
        envelope: Envelope<CheckAccessQuery>,
    ) -> Result<Vec<TargetAnswer>, Self::Error> {
        let q = &envelope.payload;
        let viewers = parse(&q.viewer_profile_ids, "viewer_profile_ids", MAX_VIEWERS)?;
        let mut targets = parse(&q.target_profile_ids, "target_profile_ids", MAX_TARGETS)?;
        targets.sort_by_key(ProfileId::as_uuid);
        targets.dedup();

        let facts = self.repo.load_access_facts(&viewers, &targets).await?;
        Ok(targets
            .iter()
            .map(|t| TargetAnswer { target: *t, access: facts.access(&viewers, t), relation: facts.relation(&viewers, t) })
            .collect())
    }
}
