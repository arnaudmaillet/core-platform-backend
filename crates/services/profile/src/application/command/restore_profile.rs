use std::sync::Arc;

use cqrs::{Command, CommandHandler, Envelope};
use validate_core::{FieldViolation, Validate};

use crate::application::port::{EventPublisher, ProfileCache, ProfileRepository};
use crate::domain::value_object::ProfileId;
use crate::error::ProfileError;

#[derive(Debug, Clone)]
pub struct RestoreProfileCommand {
    pub profile_id: String,
}

impl Command for RestoreProfileCommand {}

impl Validate for RestoreProfileCommand {
    fn validate(&self) -> Result<(), Vec<FieldViolation>> {
        let mut violations = Vec::new();
        if self.profile_id.trim().is_empty() {
            violations.push(FieldViolation::new("profile_id", "VAL-3080", "profile_id must not be empty"));
        }
        if violations.is_empty() { Ok(()) } else { Err(violations) }
    }
}

pub struct RestoreProfileHandler {
    repo: Arc<dyn ProfileRepository>,
    cache: Arc<dyn ProfileCache>,
    publisher: Arc<dyn EventPublisher>,
}

impl RestoreProfileHandler {
    pub fn new(repo: Arc<dyn ProfileRepository>, cache: Arc<dyn ProfileCache>, publisher: Arc<dyn EventPublisher>) -> Self {
        Self { repo, cache, publisher }
    }
}

impl CommandHandler<RestoreProfileCommand> for RestoreProfileHandler {
    type Error = ProfileError;

    async fn handle(&self, envelope: Envelope<RestoreProfileCommand>) -> Result<(), Self::Error> {
        let cmd = &envelope.payload;
        let id = ProfileId::try_from(cmd.profile_id.as_str())?;

        let mut profile = self
            .repo
            .find_by_id(&id)
            .await?
            .ok_or_else(|| ProfileError::ProfileNotFound { id: cmd.profile_id.clone() })?;

        profile.restore(envelope.correlation_id)?;
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
