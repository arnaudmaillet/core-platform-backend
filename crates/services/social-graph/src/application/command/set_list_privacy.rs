use std::sync::Arc;

use cqrs::{Command, CommandHandler, Envelope};
use validate_core::{FieldViolation, Validate};

use crate::application::port::SocialGraphRepository;
use crate::domain::interaction::InteractionAudience;
use crate::domain::list_privacy::ListPrivacy;
use crate::domain::value_object::ProfileId;
use crate::error::SocialGraphError;

/// The owner changes who may see their follower / following lists. An unset
/// list keeps its audience.
#[derive(Debug, Clone)]
pub struct SetListPrivacyCommand {
    pub profile_id: String,
    pub followers:  Option<InteractionAudience>,
    pub following:  Option<InteractionAudience>,
}

impl Command for SetListPrivacyCommand {}

impl Validate for SetListPrivacyCommand {
    fn validate(&self) -> Result<(), Vec<FieldViolation>> {
        if self.profile_id.trim().is_empty() {
            return Err(vec![FieldViolation::new("profile_id", "VAL-4001", "profile_id must not be empty")]);
        }
        Ok(())
    }
}

pub struct SetListPrivacyHandler {
    repo: Arc<dyn SocialGraphRepository>,
}

impl SetListPrivacyHandler {
    pub fn new(repo: Arc<dyn SocialGraphRepository>) -> Self {
        Self { repo }
    }
}

impl CommandHandler<SetListPrivacyCommand> for SetListPrivacyHandler {
    type Error = SocialGraphError;

    async fn handle(&self, envelope: Envelope<SetListPrivacyCommand>) -> Result<(), Self::Error> {
        let cmd = &envelope.payload;
        let profile_id = ProfileId::try_from(cmd.profile_id.as_str())?;
        let current = self.repo.load_list_privacy(&profile_id).await?;
        let next = ListPrivacy {
            followers: cmd.followers.unwrap_or(current.followers),
            following: cmd.following.unwrap_or(current.following),
        };
        if next != current {
            self.repo.set_list_privacy(&profile_id, &next).await?;
        }
        Ok(())
    }
}
