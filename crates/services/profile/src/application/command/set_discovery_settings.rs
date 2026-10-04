use std::sync::Arc;

use cqrs::{Command, CommandHandler, Envelope};
use validate_core::{FieldViolation, Validate};

use crate::application::port::{EventPublisher, ProfileCache, ProfileRepository};
use crate::domain::value_object::{DiscoverySettings, ProfileId};
use crate::error::ProfileError;

/// The owner changes presence and discoverability (#661). An unset flag keeps
/// its value.
#[derive(Debug, Clone, Default)]
pub struct SetDiscoverySettingsCommand {
    pub profile_id:       String,
    pub activity_status:  Option<bool>,
    pub read_receipts:    Option<bool>,
    pub by_phone:         Option<bool>,
    pub by_email:         Option<bool>,
    pub by_handle_search: Option<bool>,
    pub by_qr:            Option<bool>,
    pub in_suggestions:   Option<bool>,
}

impl SetDiscoverySettingsCommand {
    fn apply(&self, current: DiscoverySettings) -> DiscoverySettings {
        DiscoverySettings {
            activity_status:  self.activity_status.unwrap_or(current.activity_status),
            read_receipts:    self.read_receipts.unwrap_or(current.read_receipts),
            by_phone:         self.by_phone.unwrap_or(current.by_phone),
            by_email:         self.by_email.unwrap_or(current.by_email),
            by_handle_search: self.by_handle_search.unwrap_or(current.by_handle_search),
            by_qr:            self.by_qr.unwrap_or(current.by_qr),
            in_suggestions:   self.in_suggestions.unwrap_or(current.in_suggestions),
        }
    }
}

impl Command for SetDiscoverySettingsCommand {}

impl Validate for SetDiscoverySettingsCommand {
    fn validate(&self) -> Result<(), Vec<FieldViolation>> {
        if self.profile_id.trim().is_empty() {
            return Err(vec![FieldViolation::new("profile_id", "VAL-3061", "profile_id must not be empty")]);
        }
        Ok(())
    }
}

pub struct SetDiscoverySettingsHandler {
    repo:      Arc<dyn ProfileRepository>,
    cache:     Arc<dyn ProfileCache>,
    publisher: Arc<dyn EventPublisher>,
}

impl SetDiscoverySettingsHandler {
    pub fn new(
        repo: Arc<dyn ProfileRepository>,
        cache: Arc<dyn ProfileCache>,
        publisher: Arc<dyn EventPublisher>,
    ) -> Self {
        Self { repo, cache, publisher }
    }
}

impl CommandHandler<SetDiscoverySettingsCommand> for SetDiscoverySettingsHandler {
    type Error = ProfileError;

    async fn handle(&self, envelope: Envelope<SetDiscoverySettingsCommand>) -> Result<(), Self::Error> {
        let cmd = &envelope.payload;
        let id = ProfileId::try_from(cmd.profile_id.as_str())?;
        let mut profile = self
            .repo
            .find_by_id(&id)
            .await?
            .ok_or_else(|| ProfileError::ProfileNotFound { id: cmd.profile_id.clone() })?;

        let settings = cmd.apply(profile.discovery());
        if !profile.set_discovery_settings(settings, envelope.correlation_id)? {
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
