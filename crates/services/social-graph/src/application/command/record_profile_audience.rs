use std::sync::Arc;

use cqrs::{Command, CommandHandler, Envelope};
use validate_core::{FieldViolation, Validate};

use crate::application::port::SocialGraphRepository;
use crate::domain::value_object::ProfileId;
use crate::error::SocialGraphError;

/// One fact about a profile's audience, from `profile.v1.events`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AudienceFact {
    /// The owner made the profile private (`true`) or public (`false`).
    Private(bool),
    /// The profile was hidden (`true`) or restored (`false`).
    Hidden(bool),
}

/// Records an audience fact in the projection the access check reads.
/// Dispatched by the `profile.v1.events` consumer, never by a client.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RecordProfileAudienceCommand {
    pub profile_id: String,
    pub fact:       AudienceFact,
}

impl Command for RecordProfileAudienceCommand {}

impl Validate for RecordProfileAudienceCommand {
    fn validate(&self) -> Result<(), Vec<FieldViolation>> {
        if self.profile_id.trim().is_empty() {
            return Err(vec![FieldViolation::new("profile_id", "VAL-4001", "profile_id must not be empty")]);
        }
        Ok(())
    }
}

pub struct RecordProfileAudienceHandler {
    repo: Arc<dyn SocialGraphRepository>,
}

impl RecordProfileAudienceHandler {
    pub fn new(repo: Arc<dyn SocialGraphRepository>) -> Self {
        Self { repo }
    }
}

impl CommandHandler<RecordProfileAudienceCommand> for RecordProfileAudienceHandler {
    type Error = SocialGraphError;

    /// A plain column upsert: idempotent, and each fact writes only its own
    /// column. profile.v1.events is keyed by `profile_id`, so a profile's facts
    /// arrive in order.
    async fn handle(&self, envelope: Envelope<RecordProfileAudienceCommand>) -> Result<(), Self::Error> {
        let cmd = &envelope.payload;
        let id = ProfileId::try_from(cmd.profile_id.as_str())?;
        match cmd.fact {
            AudienceFact::Private(private) => self.repo.set_profile_private(&id, private).await,
            AudienceFact::Hidden(hidden) => self.repo.set_profile_hidden(&id, hidden).await,
        }
    }
}
