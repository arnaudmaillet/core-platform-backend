use std::sync::Arc;

use chrono::Utc;
use cqrs::{Command, CommandHandler, Envelope};
use validate_core::{FieldViolation, Validate};

use crate::application::port::RestrictionRepository;
use crate::domain::value_object::ProfileId;
use crate::error::SocialGraphError;

fn ids(actor_id: &str, target_id: &str) -> Result<(), Vec<FieldViolation>> {
    let mut v = Vec::new();
    if actor_id.trim().is_empty() {
        v.push(FieldViolation::new("actor_id", "VAL-4001", "actor_id must not be empty"));
    }
    if target_id.trim().is_empty() {
        v.push(FieldViolation::new("target_id", "VAL-4002", "target_id must not be empty"));
    }
    if v.is_empty() { Ok(()) } else { Err(v) }
}

/// `actor` restricts `target`: the target's comments on the actor's posts are
/// seen only by the target and the actor. The target is not told; follows
/// are untouched. Restricting again is a no-op.
#[derive(Debug, Clone)]
pub struct RestrictProfileCommand {
    pub actor_id:  String,
    pub target_id: String,
}

impl Command for RestrictProfileCommand {}

impl Validate for RestrictProfileCommand {
    fn validate(&self) -> Result<(), Vec<FieldViolation>> {
        ids(&self.actor_id, &self.target_id)
    }
}

pub struct RestrictProfileHandler {
    restrictions: Arc<dyn RestrictionRepository>,
}

impl RestrictProfileHandler {
    pub fn new(restrictions: Arc<dyn RestrictionRepository>) -> Self {
        Self { restrictions }
    }
}

impl CommandHandler<RestrictProfileCommand> for RestrictProfileHandler {
    type Error = SocialGraphError;

    async fn handle(&self, envelope: Envelope<RestrictProfileCommand>) -> Result<(), Self::Error> {
        let cmd = &envelope.payload;
        let actor = ProfileId::try_from(cmd.actor_id.as_str())?;
        let target = ProfileId::try_from(cmd.target_id.as_str())?;
        if actor == target {
            return Err(SocialGraphError::SelfInteraction);
        }
        self.restrictions.add(&actor, &target, Utc::now()).await
    }
}

/// `actor` lifts its restriction of `target` (none is fine).
#[derive(Debug, Clone)]
pub struct UnrestrictProfileCommand {
    pub actor_id:  String,
    pub target_id: String,
}

impl Command for UnrestrictProfileCommand {}

impl Validate for UnrestrictProfileCommand {
    fn validate(&self) -> Result<(), Vec<FieldViolation>> {
        ids(&self.actor_id, &self.target_id)
    }
}

pub struct UnrestrictProfileHandler {
    restrictions: Arc<dyn RestrictionRepository>,
}

impl UnrestrictProfileHandler {
    pub fn new(restrictions: Arc<dyn RestrictionRepository>) -> Self {
        Self { restrictions }
    }
}

impl CommandHandler<UnrestrictProfileCommand> for UnrestrictProfileHandler {
    type Error = SocialGraphError;

    async fn handle(&self, envelope: Envelope<UnrestrictProfileCommand>) -> Result<(), Self::Error> {
        let cmd = &envelope.payload;
        let actor = ProfileId::try_from(cmd.actor_id.as_str())?;
        let target = ProfileId::try_from(cmd.target_id.as_str())?;
        self.restrictions.remove(&actor, &target).await
    }
}
