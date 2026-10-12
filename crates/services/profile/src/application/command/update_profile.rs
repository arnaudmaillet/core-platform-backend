use std::sync::Arc;

use cqrs::{Command, CommandHandler, Envelope};
use validate_core::{FieldViolation, Validate};

use crate::application::port::{EventPublisher, ProfileCache, ProfileRepository};
use crate::domain::entity::ProfileLink;
use crate::domain::value_object::{Bio, DisplayName, Locale, ProfileId, WebsiteUrl};
use crate::error::ProfileError;

#[derive(Debug, Clone)]
pub struct UpdateProfileCommand {
    pub profile_id: String,
    pub display_name: Option<String>,
    pub bio: Option<String>,
    pub website_url: Option<String>,
    pub locale: Option<String>,
    /// Each tuple is `(label, url)`.
    pub custom_links: Vec<(String, String)>,
}

impl Command for UpdateProfileCommand {}

impl Validate for UpdateProfileCommand {
    fn validate(&self) -> Result<(), Vec<FieldViolation>> {
        let mut violations = Vec::new();
        if self.profile_id.trim().is_empty() {
            violations.push(FieldViolation::new("profile_id", "VAL-3010", "profile_id must not be empty"));
        }
        if self.custom_links.len() > 5 {
            violations.push(FieldViolation::new("custom_links", "VAL-3011", "at most 5 custom links allowed"));
        }
        if violations.is_empty() { Ok(()) } else { Err(violations) }
    }
}

pub struct UpdateProfileHandler {
    repo: Arc<dyn ProfileRepository>,
    cache: Arc<dyn ProfileCache>,
    publisher: Arc<dyn EventPublisher>,
}

impl UpdateProfileHandler {
    pub fn new(repo: Arc<dyn ProfileRepository>, cache: Arc<dyn ProfileCache>, publisher: Arc<dyn EventPublisher>) -> Self {
        Self { repo, cache, publisher }
    }
}

impl CommandHandler<UpdateProfileCommand> for UpdateProfileHandler {
    type Error = ProfileError;

    async fn handle(&self, envelope: Envelope<UpdateProfileCommand>) -> Result<(), Self::Error> {
        let cmd = &envelope.payload;

        let id = ProfileId::try_from(cmd.profile_id.as_str())?;

        let mut profile = self
            .repo
            .find_by_id(&id)
            .await?
            .ok_or_else(|| ProfileError::ProfileNotFound { id: cmd.profile_id.clone() })?;

        let display_name = cmd.display_name.as_deref().map(DisplayName::new).transpose()?;
        let bio = cmd.bio.as_deref().map(Bio::new).transpose()?;
        let website_url = match &cmd.website_url {
            Some(u) if u.is_empty() => Some(None),
            Some(u) => Some(Some(WebsiteUrl::new(u)?)),
            None => None,
        };
        let locale = cmd.locale.as_deref().map(Locale::new).transpose()?;

        let custom_links: Result<Vec<ProfileLink>, ProfileError> = cmd
            .custom_links
            .iter()
            .map(|(label, url)| {
                let wu = WebsiteUrl::new(url)?;
                ProfileLink::new(label.clone(), wu)
            })
            .collect();
        let custom_links = custom_links?;

        profile.update(display_name, bio, website_url, locale, custom_links, envelope.correlation_id)?;

        self.repo.save(&profile).await?;
        // The account's profile list reads its own index: keep it in step.
        self.repo.save_account_index(&profile).await?;

        for event in profile.drain_events() {
            self.publisher.publish(&event).await?;
        }
        let _ = self.cache.invalidate_by_id(&id).await;
        let _ = self.cache.invalidate_account_profiles(&profile.account_id()).await;

        Ok(())
    }
}
