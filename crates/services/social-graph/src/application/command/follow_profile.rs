use std::sync::Arc;

use chrono::Utc;
use cqrs::{Command, CommandHandler, Envelope};
use validate_core::{FieldViolation, Validate};

use crate::application::port::{EventPublisher, SocialGraphCache, SocialGraphRepository};
use crate::domain::aggregate::{FollowOutcome, Relation};
use crate::domain::event::{AuthorTierChanged, DomainEvent};
use crate::domain::value_object::{AuthorTier, ProfileId, TierThresholds};
use crate::error::SocialGraphError;

#[derive(Debug, Clone)]
pub struct FollowProfileCommand {
    pub actor_id:  String,
    pub target_id: String,
}

impl Command for FollowProfileCommand {}

impl Validate for FollowProfileCommand {
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

pub struct FollowProfileHandler {
    repo:            Arc<dyn SocialGraphRepository>,
    cache:           Arc<dyn SocialGraphCache>,
    publisher:       Arc<dyn EventPublisher>,
    tier_thresholds: TierThresholds,
}

impl FollowProfileHandler {
    pub fn new(
        repo:            Arc<dyn SocialGraphRepository>,
        cache:           Arc<dyn SocialGraphCache>,
        publisher:       Arc<dyn EventPublisher>,
        tier_thresholds: TierThresholds,
    ) -> Self {
        Self { repo, cache, publisher, tier_thresholds }
    }
}

impl CommandHandler<FollowProfileCommand> for FollowProfileHandler {
    type Error = SocialGraphError;

    async fn handle(
        &self,
        envelope: Envelope<FollowProfileCommand>,
    ) -> Result<(), Self::Error> {
        let cmd = &envelope.payload;

        let actor_id  = ProfileId::try_from(cmd.actor_id.as_str())?;
        let target_id = ProfileId::try_from(cmd.target_id.as_str())?;

        if actor_id == target_id {
            return Err(SocialGraphError::SelfInteraction);
        }

        // Load full bidirectional context (6 concurrent ScyllaDB point-lookups)
        // and whether the target is private (the profile_audience projection).
        let targets = [target_id];
        let (relation, facts) = tokio::join!(
            self.repo.load_relation(&actor_id, &target_id),
            self.repo.load_access_facts(&[], &targets),
        );
        let mut relation = relation?;
        let target_is_private = facts?.private.contains(&target_id);

        // Domain invariant enforcement (block-gate + idempotency guards); a
        // private target gets a pending request instead of a follow.
        match relation.follow(target_is_private)? {
            FollowOutcome::Requested { requested_at } => {
                self.repo.persist_follow_request(&actor_id, &target_id, requested_at).await?;
            }
            FollowOutcome::Followed { followed_at, cleared_request } => {
                // Persist the follow edge across the three adjacency tables.
                self.repo.persist_follow(&actor_id, &target_id, followed_at).await?;
                // A request from when the profile was private is moot now.
                if let Some(requested_at) = cleared_request {
                    self.repo.delete_follow_request(&actor_id, &target_id, requested_at).await?;
                }
                record_new_follow(
                    &self.cache,
                    &self.publisher,
                    self.tier_thresholds,
                    &actor_id,
                    &target_id,
                    &mut relation,
                )
                .await;
            }
        }
        Ok(())
    }
}

/// The side effects of a follow that just landed (a direct one, or an approved
/// request): Redis adjacency + counters, the author-tier signal, and the
/// `ProfileFollowed` event timeline fans out from. Best-effort: the durable
/// edge is already written.
pub(crate) async fn record_new_follow(
    cache:           &Arc<dyn SocialGraphCache>,
    publisher:       &Arc<dyn EventPublisher>,
    tier_thresholds: TierThresholds,
    actor_id:        &ProfileId,
    target_id:       &ProfileId,
    relation:        &mut Relation,
) {
    let _ = cache.add_following(actor_id, target_id).await;
    let _ = cache.incr_following_count(actor_id).await;

    // The followee gained a follower: bump the count and, if it crossed a tier
    // boundary, emit the author-tier signal. INCR returns the new count, so the
    // crossing check is `tier(new - 1)` vs `tier(new)` — exactly one follow ever
    // observes a given boundary.
    if let Ok(new_count) = cache.incr_followers_count(target_id).await
        && let Some(new_tier) = AuthorTier::crossing(new_count - 1, new_count, tier_thresholds)
    {
        let _ = publisher
            .publish(&DomainEvent::AuthorTierChanged(AuthorTierChanged {
                profile_id: *target_id,
                new_tier,
                follower_count: new_count,
                changed_at: Utc::now(),
            }))
            .await;
    }

    // Publish domain event to Kafka for downstream timeline fan-out engines.
    for event in relation.take_events() {
        let _ = publisher.publish(&event).await;
    }
}
