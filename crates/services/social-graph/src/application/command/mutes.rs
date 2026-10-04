use std::sync::Arc;

use chrono::Utc;
use cqrs::{Command, CommandHandler, Envelope};
use validate_core::{FieldViolation, Validate};

use crate::application::port::MuteRepository;
use crate::domain::mute::{Mute, MuteScopes};
use crate::domain::value_object::ProfileId;
use crate::error::SocialGraphError;

/// `actor` mutes `target` for `scopes` (replacing any earlier mute). The
/// target is not told; follows are untouched.
#[derive(Debug, Clone)]
pub struct MuteProfileCommand {
    pub actor_id:  String,
    pub target_id: String,
    pub scopes:    MuteScopes,
}

impl Command for MuteProfileCommand {}

impl Validate for MuteProfileCommand {
    fn validate(&self) -> Result<(), Vec<FieldViolation>> {
        let mut v = Vec::new();
        if self.actor_id.trim().is_empty() {
            v.push(FieldViolation::new("actor_id", "VAL-4001", "actor_id must not be empty"));
        }
        if self.target_id.trim().is_empty() {
            v.push(FieldViolation::new("target_id", "VAL-4002", "target_id must not be empty"));
        }
        if !self.scopes.any() {
            v.push(FieldViolation::new("scopes", "VAL-4003", "a mute covers posts, stories or messages (use Unmute to lift it)"));
        }
        if v.is_empty() { Ok(()) } else { Err(v) }
    }
}

pub struct MuteProfileHandler {
    mutes: Arc<dyn MuteRepository>,
}

impl MuteProfileHandler {
    pub fn new(mutes: Arc<dyn MuteRepository>) -> Self {
        Self { mutes }
    }
}

impl CommandHandler<MuteProfileCommand> for MuteProfileHandler {
    type Error = SocialGraphError;

    async fn handle(&self, envelope: Envelope<MuteProfileCommand>) -> Result<(), Self::Error> {
        let cmd = &envelope.payload;
        let actor = ProfileId::try_from(cmd.actor_id.as_str())?;
        let target = ProfileId::try_from(cmd.target_id.as_str())?;
        if actor == target {
            return Err(SocialGraphError::SelfInteraction);
        }
        if !cmd.scopes.any() {
            return Err(SocialGraphError::DomainViolation {
                field:   "scopes".into(),
                message: "a mute covers posts, stories or messages (use Unmute to lift it)".into(),
            });
        }
        let mute = Mute { profile_id: target, scopes: cmd.scopes, muted_at: Utc::now() };
        self.mutes.upsert(&actor, &mute).await
    }
}

/// `actor` lifts its mute of `target` (none is fine).
#[derive(Debug, Clone)]
pub struct UnmuteProfileCommand {
    pub actor_id:  String,
    pub target_id: String,
}

impl Command for UnmuteProfileCommand {}

impl Validate for UnmuteProfileCommand {
    fn validate(&self) -> Result<(), Vec<FieldViolation>> {
        let mut v = Vec::new();
        if self.actor_id.trim().is_empty() {
            v.push(FieldViolation::new("actor_id", "VAL-4001", "actor_id must not be empty"));
        }
        if self.target_id.trim().is_empty() {
            v.push(FieldViolation::new("target_id", "VAL-4002", "target_id must not be empty"));
        }
        if v.is_empty() { Ok(()) } else { Err(v) }
    }
}

pub struct UnmuteProfileHandler {
    mutes: Arc<dyn MuteRepository>,
}

impl UnmuteProfileHandler {
    pub fn new(mutes: Arc<dyn MuteRepository>) -> Self {
        Self { mutes }
    }
}

impl CommandHandler<UnmuteProfileCommand> for UnmuteProfileHandler {
    type Error = SocialGraphError;

    async fn handle(&self, envelope: Envelope<UnmuteProfileCommand>) -> Result<(), Self::Error> {
        let cmd = &envelope.payload;
        let actor = ProfileId::try_from(cmd.actor_id.as_str())?;
        let target = ProfileId::try_from(cmd.target_id.as_str())?;
        self.mutes.delete(&actor, &target).await
    }
}
