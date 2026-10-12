use std::sync::Arc;

use cqrs::{Command, CommandHandler, Envelope};
use validate_core::{FieldViolation, Validate};

use crate::application::port::{EventPublisher, ProfileCache, ProfileRepository};
use crate::domain::value_object::{MaskingReason, ProfileId};
use crate::error::ProfileError;

/// Internal command dispatched by the Kafka account event consumer.
///
/// Not exposed directly via gRPC to external callers; the `HideProfile` RPC
/// in the service definition is wired to this handler for internal admin use.
#[derive(Debug, Clone)]
pub struct HideProfileCommand {
    pub profile_id: String,
    pub masking_reason: String,
    pub suspension_reason: Option<String>,
}

impl Command for HideProfileCommand {}

impl Validate for HideProfileCommand {
    fn validate(&self) -> Result<(), Vec<FieldViolation>> {
        let mut violations = Vec::new();
        if self.profile_id.trim().is_empty() {
            violations.push(FieldViolation::new("profile_id", "VAL-3070", "profile_id must not be empty"));
        }
        if self.masking_reason.trim().is_empty() {
            violations.push(FieldViolation::new("masking_reason", "VAL-3071", "masking_reason must not be empty"));
        }
        if violations.is_empty() { Ok(()) } else { Err(violations) }
    }
}

pub struct HideProfileHandler {
    repo: Arc<dyn ProfileRepository>,
    cache: Arc<dyn ProfileCache>,
    publisher: Arc<dyn EventPublisher>,
}

impl HideProfileHandler {
    pub fn new(repo: Arc<dyn ProfileRepository>, cache: Arc<dyn ProfileCache>, publisher: Arc<dyn EventPublisher>) -> Self {
        Self { repo, cache, publisher }
    }
}

impl CommandHandler<HideProfileCommand> for HideProfileHandler {
    type Error = ProfileError;

    async fn handle(&self, envelope: Envelope<HideProfileCommand>) -> Result<(), Self::Error> {
        let cmd = &envelope.payload;
        let id = ProfileId::try_from(cmd.profile_id.as_str())?;
        let reason = MaskingReason::try_from(cmd.masking_reason.as_str())?;

        let mut profile = self
            .repo
            .find_by_id(&id)
            .await?
            .ok_or_else(|| ProfileError::ProfileNotFound { id: cmd.profile_id.clone() })?;

        profile.hide(reason, cmd.suspension_reason.clone(), envelope.correlation_id)?;
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
