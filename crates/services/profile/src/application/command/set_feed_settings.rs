use std::sync::Arc;

use cqrs::{Command, CommandHandler, Envelope};
use validate_core::{FieldViolation, Validate};

use crate::application::port::{ProfileCache, ProfileRepository};
use crate::domain::value_object::{FeedSettings, ProfileId};
use crate::error::ProfileError;

/// The owner sets their feed controls (#662). No event: only the owner's
/// clients read them (from the profile view).
#[derive(Debug, Clone)]
pub struct SetFeedSettingsCommand {
    pub profile_id: String,
    pub settings:   FeedSettings,
}

impl Command for SetFeedSettingsCommand {}

impl Validate for SetFeedSettingsCommand {
    fn validate(&self) -> Result<(), Vec<FieldViolation>> {
        if self.profile_id.trim().is_empty() {
            return Err(vec![FieldViolation::new("profile_id", "VAL-3061", "profile_id must not be empty")]);
        }
        Ok(())
    }
}

pub struct SetFeedSettingsHandler {
    repo:  Arc<dyn ProfileRepository>,
    cache: Arc<dyn ProfileCache>,
}

impl SetFeedSettingsHandler {
    pub fn new(repo: Arc<dyn ProfileRepository>, cache: Arc<dyn ProfileCache>) -> Self {
        Self { repo, cache }
    }
}

impl CommandHandler<SetFeedSettingsCommand> for SetFeedSettingsHandler {
    type Error = ProfileError;

    async fn handle(&self, envelope: Envelope<SetFeedSettingsCommand>) -> Result<(), Self::Error> {
        let cmd = &envelope.payload;
        let id = ProfileId::try_from(cmd.profile_id.as_str())?;
        let mut profile = self
            .repo
            .find_by_id(&id)
            .await?
            .ok_or_else(|| ProfileError::ProfileNotFound { id: cmd.profile_id.clone() })?;
        if !profile.set_feed_settings(cmd.settings)? {
            return Ok(());
        }
        self.repo.save(&profile).await?;
        let _ = self.cache.invalidate_by_id(&id).await;
        let _ = self.cache.invalidate_account_profiles(&profile.account_id()).await;
        Ok(())
    }
}
