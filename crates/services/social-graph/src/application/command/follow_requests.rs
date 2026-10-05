//! The owner's side of a private profile's follow requests (approve, decline)
//! and the requester's (cancel). A request is not a follow until approved.

use std::sync::Arc;

use cqrs::{Command, CommandHandler, Envelope};
use validate_core::{FieldViolation, Validate};

use super::follow_profile::record_new_follow;
use crate::application::port::{EventPublisher, SocialGraphCache, SocialGraphRepository};
use crate::domain::value_object::{ProfileId, TierThresholds};
use crate::error::SocialGraphError;

fn require(field: &'static str, code: &'static str, value: &str) -> Option<FieldViolation> {
    value
        .trim()
        .is_empty()
        .then(|| FieldViolation::new(field, code, format!("{field} must not be empty")))
}

fn ids(requester: &str, target: &str) -> Result<(ProfileId, ProfileId), SocialGraphError> {
    let requester = ProfileId::try_from(requester)?;
    let target = ProfileId::try_from(target)?;
    if requester == target {
        return Err(SocialGraphError::SelfInteraction);
    }
    Ok((requester, target))
}

// ── Approve ───────────────────────────────────────────────────────────────────

/// The owner of `owner_id` (a private profile) lets `requester_id` follow it.
#[derive(Debug, Clone)]
pub struct ApproveFollowRequestCommand {
    pub owner_id:     String,
    pub requester_id: String,
}

impl Command for ApproveFollowRequestCommand {}

impl Validate for ApproveFollowRequestCommand {
    fn validate(&self) -> Result<(), Vec<FieldViolation>> {
        let v: Vec<_> = [
            require("owner_id", "VAL-4010", &self.owner_id),
            require("requester_id", "VAL-4011", &self.requester_id),
        ]
        .into_iter()
        .flatten()
        .collect();
        if v.is_empty() { Ok(()) } else { Err(v) }
    }
}

pub struct ApproveFollowRequestHandler {
    repo:            Arc<dyn SocialGraphRepository>,
    cache:           Arc<dyn SocialGraphCache>,
    publisher:       Arc<dyn EventPublisher>,
    tier_thresholds: TierThresholds,
}

impl ApproveFollowRequestHandler {
    pub fn new(
        repo:            Arc<dyn SocialGraphRepository>,
        cache:           Arc<dyn SocialGraphCache>,
        publisher:       Arc<dyn EventPublisher>,
        tier_thresholds: TierThresholds,
    ) -> Self {
        Self { repo, cache, publisher, tier_thresholds }
    }
}

impl CommandHandler<ApproveFollowRequestCommand> for ApproveFollowRequestHandler {
    type Error = SocialGraphError;

    async fn handle(&self, envelope: Envelope<ApproveFollowRequestCommand>) -> Result<(), Self::Error> {
        let cmd = &envelope.payload;
        let (requester, owner) = ids(&cmd.requester_id, &cmd.owner_id)?;

        let mut relation = self.repo.load_relation(&requester, &owner).await?;
        let (requested_at, followed_at) = relation.approve_request()?;
        self.repo.approve_follow_request(&requester, &owner, requested_at, followed_at).await?;
        record_new_follow(
            &self.cache,
            &self.publisher,
            self.tier_thresholds,
            &requester,
            &owner,
            &mut relation,
        )
        .await;
        Ok(())
    }
}

// ── Decline / cancel ──────────────────────────────────────────────────────────

/// Drops a pending request: the owner declines it, or the requester cancels it.
/// Either way nothing is published — the requester is not told it was declined
/// (it simply stays "not following").
#[derive(Debug, Clone)]
pub struct WithdrawFollowRequestCommand {
    pub requester_id: String,
    pub target_id:    String,
}

impl Command for WithdrawFollowRequestCommand {}

impl Validate for WithdrawFollowRequestCommand {
    fn validate(&self) -> Result<(), Vec<FieldViolation>> {
        let v: Vec<_> = [
            require("requester_id", "VAL-4011", &self.requester_id),
            require("target_id", "VAL-4002", &self.target_id),
        ]
        .into_iter()
        .flatten()
        .collect();
        if v.is_empty() { Ok(()) } else { Err(v) }
    }
}

pub struct WithdrawFollowRequestHandler {
    repo:      Arc<dyn SocialGraphRepository>,
    publisher: Arc<dyn EventPublisher>,
}

impl WithdrawFollowRequestHandler {
    pub fn new(repo: Arc<dyn SocialGraphRepository>, publisher: Arc<dyn EventPublisher>) -> Self {
        Self { repo, publisher }
    }
}

impl CommandHandler<WithdrawFollowRequestCommand> for WithdrawFollowRequestHandler {
    type Error = SocialGraphError;

    async fn handle(&self, envelope: Envelope<WithdrawFollowRequestCommand>) -> Result<(), Self::Error> {
        let cmd = &envelope.payload;
        let (requester, target) = ids(&cmd.requester_id, &cmd.target_id)?;
        let mut relation = self.repo.load_relation(&requester, &target).await?;
        let requested_at = relation.withdraw_request()?;
        self.repo.delete_follow_request(&requester, &target, requested_at).await?;
        // Its notice is retracted (best-effort, like the request's event).
        for event in relation.take_events() {
            let _ = self.publisher.publish(&event).await;
        }
        Ok(())
    }
}
