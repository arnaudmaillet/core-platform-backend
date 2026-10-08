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
    /// Supervision floors (#670): a setting below its floor is refused.
    floors:    Option<Arc<dyn crate::application::port::SupervisionFloors>>,
}

impl SetInteractionSettingsHandler {
    pub fn new(
        repo: Arc<dyn ProfileRepository>,
        cache: Arc<dyn ProfileCache>,
        publisher: Arc<dyn EventPublisher>,
    ) -> Self {
        Self { repo, cache, publisher, floors: None }
    }

    /// Enforces supervision floors (#670).
    pub fn with_floors(mut self, floors: Arc<dyn crate::application::port::SupervisionFloors>) -> Self {
        self.floors = Some(floors);
        self
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
            // The temporary limit has its own commands.
            limit: current.limit,
            ..cmd.settings
        };
        if let Some(floors) = &self.floors
            && let Some(floor) = floors.get(&profile.account_id()).await?
            && !floor.allows_interaction(&settings)
        {
            return Err(ProfileError::SupervisionLocked { setting: "who may message or comment".into() });
        }
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

/// Turns a temporary interaction limit on (`Some`) or off (`None`) (#669).
#[derive(Debug, Clone)]
pub struct SetInteractionLimitCommand {
    pub profile_id: String,
    pub limit:      Option<crate::domain::value_object::InteractionLimit>,
}

impl Command for SetInteractionLimitCommand {}

impl Validate for SetInteractionLimitCommand {
    fn validate(&self) -> Result<(), Vec<FieldViolation>> {
        if self.profile_id.trim().is_empty() {
            return Err(vec![FieldViolation::new("profile_id", "VAL-3061", "profile_id must not be empty")]);
        }
        Ok(())
    }
}

pub struct SetInteractionLimitHandler {
    pub repo:      Arc<dyn ProfileRepository>,
    pub cache:     Arc<dyn ProfileCache>,
    pub publisher: Arc<dyn EventPublisher>,
}

impl CommandHandler<SetInteractionLimitCommand> for SetInteractionLimitHandler {
    type Error = ProfileError;

    async fn handle(&self, envelope: Envelope<SetInteractionLimitCommand>) -> Result<(), Self::Error> {
        let cmd = &envelope.payload;
        let id = ProfileId::try_from(cmd.profile_id.as_str())?;
        let mut profile = self
            .repo
            .find_by_id(&id)
            .await?
            .ok_or_else(|| ProfileError::ProfileNotFound { id: cmd.profile_id.clone() })?;
        let settings = InteractionSettings { limit: cmd.limit, ..profile.interaction() };
        if !profile.set_interaction_settings(settings, envelope.correlation_id)? {
            return Ok(());
        }
        self.repo.save(&profile).await?;
        for event in profile.drain_events() {
            self.publisher.publish(&event).await?;
        }
        let _ = self.cache.invalidate_by_id(&id).await;
        Ok(())
    }
}
