use std::sync::Arc;

use cqrs::{Command, CommandHandler, Envelope};
use validate_core::{FieldViolation, Validate};

use crate::application::port::{EventPublisher, ProfileCache, ProfileRepository};
use crate::domain::value_object::{PostWindow, ProfileId, TabSettings};
use crate::error::ProfileError;

/// The owner changes the post history window and tab visibility (#664). An
/// unset field keeps its value.
#[derive(Debug, Clone, Default)]
pub struct SetTabSettingsCommand {
    pub profile_id:   String,
    pub post_window:  Option<PostWindow>,
    pub show_likes:   Option<bool>,
    pub show_saved:   Option<bool>,
    pub show_reposts: Option<bool>,
    pub show_places:  Option<bool>,
}

impl Command for SetTabSettingsCommand {}

impl Validate for SetTabSettingsCommand {
    fn validate(&self) -> Result<(), Vec<FieldViolation>> {
        if self.profile_id.trim().is_empty() {
            return Err(vec![FieldViolation::new("profile_id", "VAL-3061", "profile_id must not be empty")]);
        }
        Ok(())
    }
}

pub struct SetTabSettingsHandler {
    repo:      Arc<dyn ProfileRepository>,
    cache:     Arc<dyn ProfileCache>,
    publisher: Arc<dyn EventPublisher>,
}

impl SetTabSettingsHandler {
    pub fn new(
        repo: Arc<dyn ProfileRepository>,
        cache: Arc<dyn ProfileCache>,
        publisher: Arc<dyn EventPublisher>,
    ) -> Self {
        Self { repo, cache, publisher }
    }
}

impl CommandHandler<SetTabSettingsCommand> for SetTabSettingsHandler {
    type Error = ProfileError;

    async fn handle(&self, envelope: Envelope<SetTabSettingsCommand>) -> Result<(), Self::Error> {
        let cmd = &envelope.payload;
        let id = ProfileId::try_from(cmd.profile_id.as_str())?;
        let mut profile = self
            .repo
            .find_by_id(&id)
            .await?
            .ok_or_else(|| ProfileError::ProfileNotFound { id: cmd.profile_id.clone() })?;

        let current = profile.tab_settings();
        let settings = TabSettings {
            post_window:  cmd.post_window.unwrap_or(current.post_window),
            show_likes:   cmd.show_likes.unwrap_or(current.show_likes),
            show_saved:   cmd.show_saved.unwrap_or(current.show_saved),
            show_reposts: cmd.show_reposts.unwrap_or(current.show_reposts),
            show_places:  cmd.show_places.unwrap_or(current.show_places),
        };
        if !profile.set_tab_settings(settings, envelope.correlation_id)? {
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
