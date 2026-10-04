use std::sync::Arc;

use cqrs::{Envelope, Query, QueryHandler};

use crate::application::port::SocialGraphRepository;
use crate::domain::list_privacy::ListPrivacy;
use crate::domain::value_object::ProfileId;
use crate::error::SocialGraphError;

/// Who may see the profile's follower / following lists (its owner asks).
#[derive(Debug, Clone)]
pub struct GetListPrivacyQuery {
    pub profile_id: String,
}

impl Query for GetListPrivacyQuery {
    type Response = ListPrivacy;
}

pub struct GetListPrivacyHandler {
    repo: Arc<dyn SocialGraphRepository>,
}

impl GetListPrivacyHandler {
    pub fn new(repo: Arc<dyn SocialGraphRepository>) -> Self {
        Self { repo }
    }
}

impl QueryHandler<GetListPrivacyQuery> for GetListPrivacyHandler {
    type Error = SocialGraphError;

    async fn handle(&self, envelope: Envelope<GetListPrivacyQuery>) -> Result<ListPrivacy, Self::Error> {
        let profile_id = ProfileId::try_from(envelope.payload.profile_id.as_str())?;
        self.repo.load_list_privacy(&profile_id).await
    }
}
