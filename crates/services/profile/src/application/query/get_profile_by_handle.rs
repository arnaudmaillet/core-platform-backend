use std::sync::Arc;

use cqrs::{Envelope, Query, QueryHandler};

use crate::application::port::{ProfileCache, ProfileRepository, ProfileView};
use crate::domain::value_object::{Handle, Viewer};
use crate::error::ProfileError;

#[derive(Debug, Clone)]
pub struct GetProfileByHandleQuery {
    pub handle: String,
    /// Who is reading: decides whether the profile is found and which fields
    /// come back (see [`ProfileView::for_viewer`]).
    pub viewer: Viewer,
}

impl Query for GetProfileByHandleQuery {
    type Response = Option<ProfileView>;
}

pub struct GetProfileByHandleHandler {
    repo: Arc<dyn ProfileRepository>,
    cache: Arc<dyn ProfileCache>,
}

impl GetProfileByHandleHandler {
    pub fn new(repo: Arc<dyn ProfileRepository>, cache: Arc<dyn ProfileCache>) -> Self {
        Self { repo, cache }
    }
}

impl QueryHandler<GetProfileByHandleQuery> for GetProfileByHandleHandler {
    type Error = ProfileError;

    async fn handle(&self, envelope: Envelope<GetProfileByHandleQuery>) -> Result<Option<ProfileView>, Self::Error> {
        let handle = Handle::new(&envelope.payload.handle)?;

        // Two-hop cache path: handle → profile_id → full view.
        if let Some(profile_id) = self.cache.get_profile_id_by_handle(handle.as_str()).await?
            && let Some(view) = self.cache.get_by_id(&profile_id).await? {
                return Ok(view.for_viewer(&envelope.payload.viewer));
            }

        let profile = match self.repo.find_by_handle(&handle).await? {
            Some(p) => p,
            None => return Ok(None),
        };

        let view = ProfileView::from(&profile);
        let id = profile.id();
        let _ = self.cache.set_by_id(&view).await;
        let _ = self.cache.set_handle_mapping(handle.as_str(), id).await;

        Ok(view.for_viewer(&envelope.payload.viewer))
    }
}
