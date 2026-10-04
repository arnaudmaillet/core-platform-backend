use std::sync::Arc;

use cqrs::{Envelope, Query, QueryHandler};

use crate::application::port::SocialGraphRepository;
use crate::domain::interaction::{may_interact, InteractionKind};
use crate::domain::value_object::ProfileId;
use crate::error::SocialGraphError;

/// May `actor` do `kind` to `target` (comment on its posts, mention it,
/// message it)? Mesh-only: the service owning the interaction asks before
/// writing it. A profile always may interact with itself.
#[derive(Debug, Clone)]
pub struct CheckInteractionQuery {
    pub actor_id:  String,
    pub target_id: String,
    pub kind:      InteractionKind,
}

impl Query for CheckInteractionQuery {
    type Response = bool;
}

pub struct CheckInteractionHandler {
    repo: Arc<dyn SocialGraphRepository>,
}

impl CheckInteractionHandler {
    pub fn new(repo: Arc<dyn SocialGraphRepository>) -> Self {
        Self { repo }
    }
}

impl QueryHandler<CheckInteractionQuery> for CheckInteractionHandler {
    type Error = SocialGraphError;

    async fn handle(&self, envelope: Envelope<CheckInteractionQuery>) -> Result<bool, Self::Error> {
        let q = &envelope.payload;
        let actor = ProfileId::try_from(q.actor_id.as_str())?;
        let target = ProfileId::try_from(q.target_id.as_str())?;
        if actor == target {
            return Ok(true);
        }
        let (relation, policy) = tokio::join!(
            self.repo.load_relation(&actor, &target),
            self.repo.load_interaction_policy(&target),
        );
        Ok(may_interact(&relation?, &policy?, q.kind))
    }
}
