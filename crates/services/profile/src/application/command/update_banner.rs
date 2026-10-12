use std::sync::Arc;

use cqrs::{Command, CommandHandler, Envelope};
use validate_core::{FieldViolation, Validate};

use crate::application::port::{checked_image, EventPublisher, MediaImages, ProfileCache, ProfileRepository};
use crate::domain::value_object::{BannerUrl, ProfileId};
use crate::error::ProfileError;

#[derive(Debug, Clone)]
pub struct UpdateBannerCommand {
    pub profile_id: String,
    /// Empty string or None clears the banner.
    pub banner_url: Option<String>,
    /// The media asset it shows (#907); checked against media.
    pub banner_asset_id: Option<String>,
}

impl Command for UpdateBannerCommand {}

impl Validate for UpdateBannerCommand {
    fn validate(&self) -> Result<(), Vec<FieldViolation>> {
        let mut violations = Vec::new();
        if self.profile_id.trim().is_empty() {
            violations.push(FieldViolation::new("profile_id", "VAL-3040", "profile_id must not be empty"));
        }
        if violations.is_empty() { Ok(()) } else { Err(violations) }
    }
}

pub struct UpdateBannerHandler {
    repo: Arc<dyn ProfileRepository>,
    cache: Arc<dyn ProfileCache>,
    publisher: Arc<dyn EventPublisher>,
    /// media's images (#907); `None` when not configured.
    images: Option<Arc<dyn MediaImages>>,
}

impl UpdateBannerHandler {
    pub fn new(repo: Arc<dyn ProfileRepository>, cache: Arc<dyn ProfileCache>, publisher: Arc<dyn EventPublisher>) -> Self {
        Self { repo, cache, publisher, images: None }
    }

    /// Checks the banner's media asset (#907).
    pub fn with_images(mut self, images: Option<Arc<dyn MediaImages>>) -> Self {
        self.images = images;
        self
    }
}

impl CommandHandler<UpdateBannerCommand> for UpdateBannerHandler {
    type Error = ProfileError;

    async fn handle(&self, envelope: Envelope<UpdateBannerCommand>) -> Result<(), Self::Error> {
        let cmd = &envelope.payload;
        let id = ProfileId::try_from(cmd.profile_id.as_str())?;

        let mut profile = self
            .repo
            .find_by_id(&id)
            .await?
            .ok_or_else(|| ProfileError::ProfileNotFound { id: cmd.profile_id.clone() })?;

        let url = match &cmd.banner_url {
            Some(u) if !u.is_empty() => Some(BannerUrl::new(u)?),
            _ => None,
        };

        let asset = match &url {
            Some(_) => checked_image(self.images.as_deref(), cmd.banner_asset_id.as_deref(), &profile.account_id()).await?,
            None => None,
        };
        profile.update_banner(url, asset, envelope.correlation_id)?;
        self.repo.save(&profile).await?;

        for event in profile.drain_events() {
            self.publisher.publish(&event).await?;
        }
        let _ = self.cache.invalidate_by_id(&id).await;
        let _ = self.cache.invalidate_account_profiles(&profile.account_id()).await;

        Ok(())
    }
}
