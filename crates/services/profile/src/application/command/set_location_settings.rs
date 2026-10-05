use std::sync::Arc;

use cqrs::{Command, CommandHandler, Envelope};
use validate_core::{FieldViolation, Validate};

use crate::application::port::{EventPublisher, ProfileCache, ProfileRepository};
use crate::domain::value_object::{LocationAudience, LocationPrecision, LocationSettings, ProfileId};
use crate::error::ProfileError;

/// The owner sets ghost mode and location precision (#657); the audience and
/// the new-posts preference when given (absent keeps the stored value).
#[derive(Debug, Clone)]
pub struct SetLocationSettingsCommand {
    pub profile_id:   String,
    pub ghost:        bool,
    pub precision:    LocationPrecision,
    pub audience:     Option<LocationAudience>,
    pub on_new_posts: Option<bool>,
}

impl Command for SetLocationSettingsCommand {}

impl Validate for SetLocationSettingsCommand {
    fn validate(&self) -> Result<(), Vec<FieldViolation>> {
        if self.profile_id.trim().is_empty() {
            return Err(vec![FieldViolation::new("profile_id", "VAL-3061", "profile_id must not be empty")]);
        }
        Ok(())
    }
}

pub struct SetLocationSettingsHandler {
    repo:      Arc<dyn ProfileRepository>,
    cache:     Arc<dyn ProfileCache>,
    publisher: Arc<dyn EventPublisher>,
}

impl SetLocationSettingsHandler {
    pub fn new(
        repo: Arc<dyn ProfileRepository>,
        cache: Arc<dyn ProfileCache>,
        publisher: Arc<dyn EventPublisher>,
    ) -> Self {
        Self { repo, cache, publisher }
    }
}

impl CommandHandler<SetLocationSettingsCommand> for SetLocationSettingsHandler {
    type Error = ProfileError;

    async fn handle(&self, envelope: Envelope<SetLocationSettingsCommand>) -> Result<(), Self::Error> {
        let cmd = &envelope.payload;
        let id = ProfileId::try_from(cmd.profile_id.as_str())?;
        let mut profile = self
            .repo
            .find_by_id(&id)
            .await?
            .ok_or_else(|| ProfileError::ProfileNotFound { id: cmd.profile_id.clone() })?;

        let current = profile.location();
        let settings = LocationSettings {
            ghost:        cmd.ghost,
            precision:    cmd.precision,
            audience:     cmd.audience.unwrap_or(current.audience),
            on_new_posts: cmd.on_new_posts.unwrap_or(current.on_new_posts),
        };
        if !profile.set_location_settings(settings, envelope.correlation_id)? {
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
