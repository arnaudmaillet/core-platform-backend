use std::sync::Arc;

use cqrs::{Command, CommandHandler, Envelope};
use validate_core::{FieldViolation, Validate};

use crate::application::port::{EventPublisher, ProfileCache, ProfileRepository};
use crate::domain::value_object::{InteractionSettings, ProfileId};
use crate::error::ProfileError;

/// The owner sets who may comment, mention and message, downloads and like
/// counts — the whole set at once.
#[derive(Debug, Clone)]
pub struct SetInteractionSettingsCommand {
    pub profile_id: String,
    pub settings:   InteractionSettings,
    /// `None` keeps the stored value (a client unaware of the field).
    pub allow_remix: Option<bool>,
    /// `None` keeps the stored value.
    pub allow_sound_reuse: Option<bool>,
}

impl Command for SetInteractionSettingsCommand {}

impl Validate for SetInteractionSettingsCommand {
    fn validate(&self) -> Result<(), Vec<FieldViolation>> {
        if self.profile_id.trim().is_empty() {
            return Err(vec![FieldViolation::new("profile_id", "VAL-3060", "profile_id must not be empty")]);
        }
        Ok(())
    }
}

pub struct SetInteractionSettingsHandler {
    repo:      Arc<dyn ProfileRepository>,
    cache:     Arc<dyn ProfileCache>,
    publisher: Arc<dyn EventPublisher>,
}

impl SetInteractionSettingsHandler {
    pub fn new(
        repo: Arc<dyn ProfileRepository>,
        cache: Arc<dyn ProfileCache>,
        publisher: Arc<dyn EventPublisher>,
    ) -> Self {
        Self { repo, cache, publisher }
    }
}

impl CommandHandler<SetInteractionSettingsCommand> for SetInteractionSettingsHandler {
    type Error = ProfileError;

    async fn handle(&self, envelope: Envelope<SetInteractionSettingsCommand>) -> Result<(), Self::Error> {
        let cmd = &envelope.payload;
        let id = ProfileId::try_from(cmd.profile_id.as_str())?;
        let mut profile = self
            .repo
            .find_by_id(&id)
            .await?
            .ok_or_else(|| ProfileError::ProfileNotFound { id: cmd.profile_id.clone() })?;

        let current = profile.interaction();
        let settings = InteractionSettings {
            allow_remix: cmd.allow_remix.unwrap_or(current.allow_remix),
            allow_sound_reuse: cmd.allow_sound_reuse.unwrap_or(current.allow_sound_reuse),
            ..cmd.settings
        };
        if !profile.set_interaction_settings(settings, envelope.correlation_id)? {
            return Ok(());
        }
        self.repo.save(&profile).await?;
        for event in profile.drain_events() {
            self.publisher.publish(&event).await?;
        }
        let _ = self.cache.invalidate_by_id(&id).await;
        let _ = self.cache.invalidate_account_profiles(&profile.account_id()).await;
        Ok(())
    }
}
