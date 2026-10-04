use std::sync::Arc;

use cqrs::{Envelope, Query, QueryHandler};

use crate::application::port::{ProfileCache, ProfileRepository, ProfileView};
use crate::domain::value_object::{ProfileId, Viewer};
use crate::error::ProfileError;

#[derive(Debug, Clone)]
pub struct GetProfileByIdQuery {
    pub profile_id: String,
    /// Who is reading: decides whether the profile is found and which fields
    /// come back (see [`ProfileView::for_viewer`]).
    pub viewer: Viewer,
}

impl Query for GetProfileByIdQuery {
    type Response = Option<ProfileView>;
}

pub struct GetProfileByIdHandler {
    repo: Arc<dyn ProfileRepository>,
    cache: Arc<dyn ProfileCache>,
}

impl GetProfileByIdHandler {
    pub fn new(repo: Arc<dyn ProfileRepository>, cache: Arc<dyn ProfileCache>) -> Self {
        Self { repo, cache }
    }
}

impl QueryHandler<GetProfileByIdQuery> for GetProfileByIdHandler {
    type Error = ProfileError;

    async fn handle(&self, envelope: Envelope<GetProfileByIdQuery>) -> Result<Option<ProfileView>, Self::Error> {
        let id = ProfileId::try_from(envelope.payload.profile_id.as_str())?;

        if let Some(view) = self.cache.get_by_id(&id).await? {
            return Ok(view.for_viewer(&envelope.payload.viewer));
        }

        let profile = match self.repo.find_by_id(&id).await? {
            Some(p) => p,
            None => return Ok(None),
        };

        let view = ProfileView::from(&profile);
        let _ = self.cache.set_by_id(&view).await;

        Ok(view.for_viewer(&envelope.payload.viewer))
    }
}
