use std::collections::HashSet;
use std::sync::Arc;

use cqrs::{Envelope, Query, QueryHandler};

use crate::application::port::MuteRepository;
use crate::domain::mute::{Mute, MuteScope};
use crate::domain::value_object::ProfileId;
use crate::error::SocialGraphError;

/// At most this many reader profiles per `MutedProfilesQuery`.
const MAX_MUTERS: usize = 10;

/// The profile's mutes (its owner asks), in profile-id order.
#[derive(Debug, Clone)]
pub struct ListMutesQuery {
    pub profile_id: String,
    pub limit:      u32,
    pub page_token: Option<String>,
}

impl Query for ListMutesQuery {
    type Response = (Vec<Mute>, Option<String>);
}

pub struct ListMutesHandler {
    mutes: Arc<dyn MuteRepository>,
}

impl ListMutesHandler {
    pub fn new(mutes: Arc<dyn MuteRepository>) -> Self {
        Self { mutes }
    }
}

impl QueryHandler<ListMutesQuery> for ListMutesHandler {
    type Error = SocialGraphError;

    async fn handle(&self, envelope: Envelope<ListMutesQuery>) -> Result<(Vec<Mute>, Option<String>), Self::Error> {
        let q = &envelope.payload;
        let profile_id = ProfileId::try_from(q.profile_id.as_str())?;
        self.mutes
            .list(&profile_id, q.limit.clamp(1, 100) as i32, q.page_token.as_deref())
            .await
    }
}

/// The profiles a reader's profiles mute for `scope` (mesh: the feeds drop
/// their posts).
#[derive(Debug, Clone)]
pub struct MutedProfilesQuery {
    pub profile_ids: Vec<String>,
    pub scope:       MuteScope,
}

impl Query for MutedProfilesQuery {
    type Response = HashSet<ProfileId>;
}

pub struct MutedProfilesHandler {
    mutes: Arc<dyn MuteRepository>,
}

impl MutedProfilesHandler {
    pub fn new(mutes: Arc<dyn MuteRepository>) -> Self {
        Self { mutes }
    }
}

impl QueryHandler<MutedProfilesQuery> for MutedProfilesHandler {
    type Error = SocialGraphError;

    async fn handle(&self, envelope: Envelope<MutedProfilesQuery>) -> Result<HashSet<ProfileId>, Self::Error> {
        let q = &envelope.payload;
        if q.profile_ids.len() > MAX_MUTERS {
            return Err(SocialGraphError::DomainViolation {
                field:   "profile_ids".into(),
                message: format!("at most {MAX_MUTERS} profiles"),
            });
        }
        let muters = q
            .profile_ids
            .iter()
            .map(|id| ProfileId::try_from(id.as_str()))
            .collect::<Result<Vec<_>, _>>()?;
        self.mutes.muted_by(&muters, q.scope).await
    }
}
