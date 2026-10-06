//! The holder's controls over its interest tags (#662): remove one, or reset
//! them all.

use std::sync::Arc;

use cqrs::{Command, CommandHandler, Envelope};
use validate_core::{FieldViolation, Validate};

use crate::application::port::InterestStore;
use crate::domain::value_object::interest::normalize_tag;
use crate::domain::value_object::ProfileId;
use crate::error::TimelineError;

/// Drops a tag from the profile's interests and keeps it out.
#[derive(Debug, Clone, PartialEq)]
pub struct RemoveInterestCommand {
    pub profile_id: String,
    /// As listed, with or without its `#`.
    pub tag:        String,
}

impl Command for RemoveInterestCommand {}

impl Validate for RemoveInterestCommand {
    fn validate(&self) -> Result<(), Vec<FieldViolation>> {
        let mut v = Vec::new();
        if ProfileId::try_from(self.profile_id.as_str()).is_err() {
            v.push(FieldViolation::new("profile_id", "TML-VAL-040", "profile_id must be a UUID"));
        }
        if normalize_tag(&self.tag).is_none() {
            v.push(FieldViolation::new("tag", "TML-VAL-041", "tag must be a hashtag"));
        }
        if v.is_empty() { Ok(()) } else { Err(v) }
    }
}

/// Forgets the profile's interests (removed tags included): For You starts over.
#[derive(Debug, Clone, PartialEq)]
pub struct ResetInterestsCommand {
    pub profile_id: String,
}

impl Command for ResetInterestsCommand {}

impl Validate for ResetInterestsCommand {
    fn validate(&self) -> Result<(), Vec<FieldViolation>> {
        if ProfileId::try_from(self.profile_id.as_str()).is_err() {
            return Err(vec![FieldViolation::new("profile_id", "TML-VAL-040", "profile_id must be a UUID")]);
        }
        Ok(())
    }
}

pub struct ManageInterestsHandler {
    pub interests: Arc<dyn InterestStore>,
}

impl CommandHandler<RemoveInterestCommand> for ManageInterestsHandler {
    type Error = TimelineError;

    async fn handle(&self, envelope: Envelope<RemoveInterestCommand>) -> Result<(), TimelineError> {
        let cmd = &envelope.payload;
        let profile = ProfileId::try_from(cmd.profile_id.as_str())?;
        let Some(tag) = normalize_tag(&cmd.tag) else {
            return Ok(()); // refused by validation
        };
        self.interests.remove(&profile, &tag).await
    }
}

impl CommandHandler<ResetInterestsCommand> for ManageInterestsHandler {
    type Error = TimelineError;

    async fn handle(&self, envelope: Envelope<ResetInterestsCommand>) -> Result<(), TimelineError> {
        self.interests.reset(&ProfileId::try_from(envelope.payload.profile_id.as_str())?).await
    }
}
