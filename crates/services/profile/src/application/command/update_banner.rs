use std::sync::Arc;

use cqrs::{Command, CommandHandler, Envelope};
use validate_core::{FieldViolation, Validate};

use crate::application::port::{checked_image, EventPublisher, GemWallet, MediaImages, ProfileCache, ProfileRepository};
use crate::domain::value_object::{BannerUrl, MediaAssetId, ProfileId};
use crate::error::ProfileError;

#[derive(Debug, Clone)]
pub struct UpdateBannerCommand {
    pub profile_id: String,
    /// Empty string or None clears the banner (with no asset either).
    pub banner_url: Option<String>,
    /// The media asset it shows (#907); checked against media. Without a URL
    /// it is an animated banner (#915, #947): a video of the account's own,
    /// for an account owning [`ANIMATED_BANNER_ITEM`].
    pub banner_asset_id: Option<String>,
}

/// The catalogue item an animated banner needs (#915).
pub const ANIMATED_BANNER_ITEM: &str = "banner.animated";

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
    /// Says whether the account owns the animated banner (#915); `None`: an
    /// animated banner cannot be set.
    wallet: Option<Arc<dyn GemWallet>>,
}

impl UpdateBannerHandler {
    pub fn new(repo: Arc<dyn ProfileRepository>, cache: Arc<dyn ProfileCache>, publisher: Arc<dyn EventPublisher>) -> Self {
        Self { repo, cache, publisher, images: None, wallet: None }
    }

    /// Checks the banner's media asset (#907).
    pub fn with_images(mut self, images: Option<Arc<dyn MediaImages>>) -> Self {
        self.images = images;
        self
    }

    /// Animated banners (#915): the wallet says who owns the item.
    pub fn with_wallet(mut self, wallet: Option<Arc<dyn GemWallet>>) -> Self {
        self.wallet = wallet;
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

        // An asset without a URL: an animated banner (#915, #947).
        if let (None, Some(asset)) = (&url, cmd.banner_asset_id.as_deref().filter(|a| !a.is_empty())) {
            let asset = MediaAssetId::try_from(asset)?;
            let account = profile.account_id();
            let wallet = self.wallet.as_ref().ok_or_else(|| ProfileError::WalletUnavailable { reason: "not configured".into() })?;
            if !wallet.owns(&account, ANIMATED_BANNER_ITEM).await? {
                return Err(ProfileError::ItemNotOwned { item: ANIMATED_BANNER_ITEM.to_owned() });
            }
            let images = self.images.as_ref().ok_or_else(|| ProfileError::MediaUnavailable { reason: "not configured".into() })?;
            images.check_video(&asset, &account).await?;
            profile.update_banner_video(asset, envelope.correlation_id)?;
            return self.saved(&id, profile).await;
        }

        let asset = match &url {
            Some(_) => checked_image(self.images.as_deref(), cmd.banner_asset_id.as_deref(), &profile.account_id()).await?,
            None => None,
        };
        profile.update_banner(url, asset, envelope.correlation_id)?;
        self.saved(&id, profile).await
    }
}

impl UpdateBannerHandler {
    async fn saved(&self, id: &ProfileId, mut profile: crate::domain::aggregate::Profile) -> Result<(), ProfileError> {
        self.repo.save(&profile).await?;
        for event in profile.drain_events() {
            self.publisher.publish(&event).await?;
        }
        let _ = self.cache.invalidate_by_id(id).await;
        let _ = self.cache.invalidate_account_profiles(&profile.account_id()).await;
        Ok(())
    }
}
