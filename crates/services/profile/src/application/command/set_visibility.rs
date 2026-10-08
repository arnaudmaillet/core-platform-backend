use std::sync::Arc;

use cqrs::{Command, CommandHandler, Envelope};
use validate_core::{FieldViolation, Validate};

use crate::application::port::{EventPublisher, ProfileCache, ProfileRepository};
use crate::domain::value_object::{ProfileId, ProfileVisibility};
use crate::error::ProfileError;

#[derive(Debug, Clone)]
pub struct SetVisibilityCommand {
    pub profile_id: String,
    pub visibility: String,
}

impl Command for SetVisibilityCommand {}

impl Validate for SetVisibilityCommand {
    fn validate(&self) -> Result<(), Vec<FieldViolation>> {
        let mut violations = Vec::new();
        if self.profile_id.trim().is_empty() {
            violations.push(FieldViolation::new("profile_id", "VAL-3050", "profile_id must not be empty"));
        }
        if self.visibility.trim().is_empty() {
            violations.push(FieldViolation::new("visibility", "VAL-3051", "visibility must not be empty"));
        }
        if violations.is_empty() { Ok(()) } else { Err(violations) }
    }
}

pub struct SetVisibilityHandler {
    repo: Arc<dyn ProfileRepository>,
    cache: Arc<dyn ProfileCache>,
    publisher: Arc<dyn EventPublisher>,
    /// Supervision floors (#670): a setting below its floor is refused.
    floors:    Option<Arc<dyn crate::application::port::SupervisionFloors>>,
}

impl SetVisibilityHandler {
    pub fn new(repo: Arc<dyn ProfileRepository>, cache: Arc<dyn ProfileCache>, publisher: Arc<dyn EventPublisher>) -> Self {
        Self { repo, cache, publisher, floors: None }
    }

    /// Enforces supervision floors (#670).
    pub fn with_floors(mut self, floors: Arc<dyn crate::application::port::SupervisionFloors>) -> Self {
        self.floors = Some(floors);
        self
    }
}

impl CommandHandler<SetVisibilityCommand> for SetVisibilityHandler {
    type Error = ProfileError;

    async fn handle(&self, envelope: Envelope<SetVisibilityCommand>) -> Result<(), Self::Error> {
        let cmd = &envelope.payload;
        let id = ProfileId::try_from(cmd.profile_id.as_str())?;
        let visibility = ProfileVisibility::try_from(cmd.visibility.as_str())?;

        let mut profile = self
            .repo
            .find_by_id(&id)
            .await?
            .ok_or_else(|| ProfileError::ProfileNotFound { id: cmd.profile_id.clone() })?;

        if let Some(floors) = &self.floors
            && let Some(floor) = floors.get(&profile.account_id()).await?
            && !floor.allows_visibility(visibility)
        {
            return Err(ProfileError::SupervisionLocked { setting: "visibility".into() });
        }
        profile.set_visibility(visibility, envelope.correlation_id)?;
        self.repo.save(&profile).await?;

        for event in profile.drain_events() {
            self.publisher.publish(&event).await?;
        }
        let _ = self.cache.invalidate_by_id(&id).await;
        let _ = self.cache.invalidate_account_profiles(&profile.account_id()).await;

        Ok(())
    }
}
