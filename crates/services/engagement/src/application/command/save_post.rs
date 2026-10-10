//! Saving and unsaving a post for one of the caller's profiles (#872). The
//! handler binds nothing: the gRPC layer has bound the account and the
//! profile to the edge token. Both are idempotent.

use std::sync::Arc;

use chrono::Utc;
use cqrs::{Command, CommandHandler, Envelope};
use validate_core::{FieldViolation, Validate};

use crate::application::port::SavedPosts;
use crate::domain::value_object::PostId;
use crate::error::EngagementError;

/// Save (`saved: true`) or unsave a post.
pub struct SavePostCommand {
    pub account_id: String,
    pub profile_id: String,
    pub post_id:    String,
    pub saved:      bool,
}

impl Command for SavePostCommand {}

impl Validate for SavePostCommand {
    fn validate(&self) -> Result<(), Vec<FieldViolation>> {
        let mut v = Vec::new();
        if uuid::Uuid::parse_str(&self.account_id).is_err() {
            v.push(FieldViolation::new("account_id", "ENG-VAL-002", "account_id must be a UUID"));
        }
        if self.profile_id.is_empty() || self.profile_id.len() > 64 {
            v.push(FieldViolation::new("profile_id", "ENG-VAL-003", "profile_id must be 1 to 64 characters"));
        }
        if self.post_id.trim().is_empty() {
            v.push(FieldViolation::new("post_id", "ENG-VAL-001", "post_id must not be empty"));
        }
        if v.is_empty() { Ok(()) } else { Err(v) }
    }
}

pub struct SavePostHandler {
    /// `None`: this instance runs without the durable store.
    pub saves: Option<Arc<dyn SavedPosts>>,
}

impl CommandHandler<SavePostCommand> for SavePostHandler {
    type Error = EngagementError;

    async fn handle(&self, envelope: Envelope<SavePostCommand>) -> Result<(), EngagementError> {
        let cmd = &envelope.payload;
        let post = PostId::try_from(cmd.post_id.as_str())?.to_string();
        let saves = self.saves.as_ref().ok_or(EngagementError::LedgerUnavailable)?;
        let now = Utc::now().timestamp_micros();
        match cmd.saved {
            true => saves.save(&cmd.account_id, &cmd.profile_id, &post, now).await,
            false => saves.unsave(&cmd.account_id, &cmd.profile_id, &post, now).await,
        }
    }
}
