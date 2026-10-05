use std::sync::Arc;

use cqrs::{Command, CommandHandler, Envelope};
use validate_core::{FieldViolation, Validate};

use crate::application::port::{EventPublisher, ProfileCache, ProfileRepository};
use crate::domain::value_object::{Handle, ProfileId};
use crate::error::ProfileError;

#[derive(Debug, Clone)]
pub struct ChangeHandleCommand {
    pub profile_id: String,
    pub new_handle: String,
}

impl Command for ChangeHandleCommand {}

impl Validate for ChangeHandleCommand {
    fn validate(&self) -> Result<(), Vec<FieldViolation>> {
        let mut violations = Vec::new();
        if self.profile_id.trim().is_empty() {
            violations.push(FieldViolation::new("profile_id", "VAL-3020", "profile_id must not be empty"));
        }
        if self.new_handle.trim().is_empty() {
            violations.push(FieldViolation::new("new_handle", "VAL-3021", "new_handle must not be empty"));
        }
        if violations.is_empty() { Ok(()) } else { Err(violations) }
    }
}

pub struct ChangeHandleHandler {
    repo: Arc<dyn ProfileRepository>,
    cache: Arc<dyn ProfileCache>,
    publisher: Arc<dyn EventPublisher>,
}

impl ChangeHandleHandler {
    pub fn new(repo: Arc<dyn ProfileRepository>, cache: Arc<dyn ProfileCache>, publisher: Arc<dyn EventPublisher>) -> Self {
        Self { repo, cache, publisher }
    }
}

impl CommandHandler<ChangeHandleCommand> for ChangeHandleHandler {
    type Error = ProfileError;

    async fn handle(&self, envelope: Envelope<ChangeHandleCommand>) -> Result<(), Self::Error> {
        let cmd = &envelope.payload;

        let id = ProfileId::try_from(cmd.profile_id.as_str())?;
        let new_handle = Handle::new(&cmd.new_handle)?;

        let mut profile = self
            .repo
            .find_by_id(&id)
            .await?
            .ok_or_else(|| ProfileError::ProfileNotFound { id: cmd.profile_id.clone() })?;

        if !self.repo.handle_is_available(&new_handle, Some(profile.id())).await? {
            return Err(ProfileError::HandleAlreadyTaken {
                handle: new_handle.as_str().to_owned(),
            });
        }

        let old_handle = profile.change_handle(new_handle.clone(), envelope.correlation_id)?;

        let claimed = self
            .repo
            .claim_handle(&new_handle, profile.id(), profile.account_id())
            .await?;
        if !claimed {
            return Err(ProfileError::HandleAlreadyTaken {
                handle: new_handle.as_str().to_owned(),
            });
        }

        if let Err(e) = self.repo.save(&profile).await {
            if let Err(release) = self.repo.release_handle_claim(&new_handle, profile.id()).await {
                tracing::error!(error = %release, handle = new_handle.as_str(), "handle claim not released after a failed change");
            }
            return Err(e);
        }
        // The profile now carries the new handle: release the old one.
        self.repo.tombstone_handle(&old_handle, profile.verified()).await?;

        for event in profile.drain_events() {
            self.publisher.publish(&event).await?;
        }

        let _ = self.cache.invalidate_handle(old_handle.as_str()).await;
        let _ = self.cache.invalidate_handle(new_handle.as_str()).await;
        let _ = self.cache.invalidate_by_id(&id).await;
        let _ = self.cache.invalidate_account_profiles(&profile.account_id()).await;

        Ok(())
    }
}
