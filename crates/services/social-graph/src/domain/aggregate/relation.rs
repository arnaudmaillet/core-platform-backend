use chrono::{DateTime, Utc};

use crate::domain::event::{
    DomainEvent, FollowRequested, ProfileBlocked, ProfileFollowed, ProfileUnblocked, ProfileUnfollowed,
};
use crate::domain::value_object::{ProfileId, RelationStatus};
use crate::error::SocialGraphError;

/// The timestamps of follow edges severed by a block operation.
///
/// Returned by [`Relation::block`] so the command handler knows exactly which
/// ScyllaDB DELETEs and Redis SREMs to issue without re-querying the database.
#[derive(Debug, Clone)]
pub struct SeveredFollows {
    /// `Some(ts)` if the actor→target follow was severed; `ts` is `followed_at`
    /// needed to delete the clustering row from `followers` and `following`.
    pub actor_to_target: Option<DateTime<Utc>>,
    /// `Some(ts)` if the target→actor follow was severed.
    pub target_to_actor: Option<DateTime<Utc>>,
    /// `Some(requested_at)` if the actor's pending follow request was dropped.
    pub actor_request: Option<DateTime<Utc>>,
    /// `Some(requested_at)` if the target's pending follow request was dropped.
    pub target_request: Option<DateTime<Utc>>,
}

/// What a follow did: a public profile is followed at once; a private one gets
/// a pending request its owner approves or declines.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FollowOutcome {
    /// `cleared_request`: a request made while the profile was private, now
    /// moot (it went public) and to delete.
    Followed { followed_at: DateTime<Utc>, cleared_request: Option<DateTime<Utc>> },
    Requested { requested_at: DateTime<Utc> },
}

/// The raw bidirectional context used to reconstruct a [`Relation`].
///
/// Populated by [`SocialGraphRepository::load_relation`].
pub struct RelationContext {
    pub actor_follows_target_since:  Option<DateTime<Utc>>,
    pub target_follows_actor_since:  Option<DateTime<Utc>>,
    pub actor_blocks_target:         bool,
    pub target_blocks_actor:         bool,
    /// The actor's pending follow request to the target, if any.
    pub actor_requested_target_at:   Option<DateTime<Utc>>,
    /// The target's pending follow request to the actor, if any.
    pub target_requested_actor_at:   Option<DateTime<Utc>>,
}

/// Aggregate root for the bidirectional relationship between two profiles.
///
/// Enforces all social-graph invariants before any persistence call is made:
///   1. Self-interaction guard (actor == target is rejected at the handler level).
///   2. Block-gate: a follow is rejected if a block exists in either direction.
///   3. Idempotency guards: re-follow and re-block return domain errors.
///   4. Block-sever: `block()` automatically computes which existing follows
///      must be deleted, returning their timestamps as [`SeveredFollows`].
///
/// # Event sourcing
///
/// Domain events are accumulated in `pending_events` and drained by the command
/// handler after persistence succeeds. The handler owns the publish-or-discard
/// decision; the aggregate never publishes directly.
pub struct Relation {
    actor_id:  ProfileId,
    target_id: ProfileId,

    /// `None` = actor does not follow target.
    /// `Some(ts)` = actor follows target since `ts` (needed for DELETE key).
    actor_follows_target_since: Option<DateTime<Utc>>,

    /// `None` = target does not follow actor.
    /// `Some(ts)` = target follows actor since `ts`.
    target_follows_actor_since: Option<DateTime<Utc>>,

    actor_blocks_target: bool,
    target_blocks_actor: bool,

    /// A pending follow request (the target's profile is private): never a
    /// follow — `follow_status`, the adjacency lists and every audience check
    /// ignore it until the owner approves.
    actor_requested_target_at: Option<DateTime<Utc>>,
    target_requested_actor_at: Option<DateTime<Utc>>,

    pending_events: Vec<DomainEvent>,
}

impl Relation {
    pub fn from_context(
        actor_id:  ProfileId,
        target_id: ProfileId,
        ctx: RelationContext,
    ) -> Self {
        Self {
            actor_id,
            target_id,
            actor_follows_target_since: ctx.actor_follows_target_since,
            target_follows_actor_since: ctx.target_follows_actor_since,
            actor_blocks_target:        ctx.actor_blocks_target,
            target_blocks_actor:        ctx.target_blocks_actor,
            actor_requested_target_at:  ctx.actor_requested_target_at,
            target_requested_actor_at:  ctx.target_requested_actor_at,
            pending_events:             Vec::new(),
        }
    }

    // ── Commands ──────────────────────────────────────────────────────────────

    /// The actor follows the target — or, when the target's profile is
    /// private, asks to: a pending request its owner approves or declines.
    ///
    /// # Errors
    ///
    /// - [`SocialGraphError::AlreadyFollowing`] if the follow already exists.
    /// - [`SocialGraphError::AlreadyRequested`] if a request is already pending.
    /// - [`SocialGraphError::BlockGateDenied`] if a block exists in either direction.
    pub fn follow(&mut self, target_is_private: bool) -> Result<FollowOutcome, SocialGraphError> {
        self.block_gate()?;
        if self.actor_follows_target_since.is_some() {
            return Err(SocialGraphError::AlreadyFollowing {
                actor_id:  self.actor_id.as_str(),
                target_id: self.target_id.as_str(),
            });
        }
        let now = Utc::now();
        if target_is_private {
            if self.actor_requested_target_at.is_some() {
                return Err(SocialGraphError::AlreadyRequested {
                    actor_id:  self.actor_id.as_str(),
                    target_id: self.target_id.as_str(),
                });
            }
            self.actor_requested_target_at = Some(now);
            self.pending_events.push(DomainEvent::FollowRequested(FollowRequested {
                actor_id:     self.actor_id,
                target_id:    self.target_id,
                requested_at: now,
            }));
            return Ok(FollowOutcome::Requested { requested_at: now });
        }
        let cleared_request = self.actor_requested_target_at.take();
        self.start_following(now, false);
        Ok(FollowOutcome::Followed { followed_at: now, cleared_request })
    }

    /// The target's owner approves the actor's pending request: the actor now
    /// follows the target. Returns `(requested_at, followed_at)`.
    ///
    /// # Errors
    ///
    /// - [`SocialGraphError::NoFollowRequest`] if none is pending.
    /// - [`SocialGraphError::BlockGateDenied`] if a block exists in either direction.
    pub fn approve_request(&mut self) -> Result<(DateTime<Utc>, DateTime<Utc>), SocialGraphError> {
        self.block_gate()?;
        let requested_at = self.take_request()?;
        let now = Utc::now();
        self.start_following(now, true);
        Ok((requested_at, now))
    }

    /// Drops the actor's pending request — declined by the target's owner or
    /// cancelled by the actor. Returns its `requested_at` (the delete key).
    ///
    /// # Errors
    ///
    /// - [`SocialGraphError::NoFollowRequest`] if none is pending.
    pub fn withdraw_request(&mut self) -> Result<DateTime<Utc>, SocialGraphError> {
        self.take_request()
    }

    fn take_request(&mut self) -> Result<DateTime<Utc>, SocialGraphError> {
        self.actor_requested_target_at.take().ok_or_else(|| SocialGraphError::NoFollowRequest {
            actor_id:  self.actor_id.as_str(),
            target_id: self.target_id.as_str(),
        })
    }

    fn block_gate(&self) -> Result<(), SocialGraphError> {
        if self.actor_blocks_target || self.target_blocks_actor {
            return Err(SocialGraphError::BlockGateDenied {
                actor_id:  self.actor_id.as_str(),
                target_id: self.target_id.as_str(),
            });
        }
        Ok(())
    }

    fn start_following(&mut self, now: DateTime<Utc>, via_request: bool) {
        self.actor_follows_target_since = Some(now);
        self.pending_events.push(DomainEvent::ProfileFollowed(ProfileFollowed {
            actor_id:    self.actor_id,
            target_id:   self.target_id,
            followed_at: now,
            via_request,
        }));
    }

    /// Removes the actor→target follow.
    ///
    /// # Errors
    ///
    /// - [`SocialGraphError::NotFollowing`] if no follow exists.
    pub fn unfollow(&mut self) -> Result<DateTime<Utc>, SocialGraphError> {
        let followed_at = self.actor_follows_target_since.ok_or_else(|| {
            SocialGraphError::NotFollowing {
                actor_id:  self.actor_id.as_str(),
                target_id: self.target_id.as_str(),
            }
        })?;
        self.actor_follows_target_since = None;
        self.pending_events.push(DomainEvent::ProfileUnfollowed(ProfileUnfollowed {
            actor_id:      self.actor_id,
            target_id:     self.target_id,
            unfollowed_at: Utc::now(),
        }));
        Ok(followed_at)
    }

    /// Records that the actor blocks the target, severs any existing follows
    /// in both directions, and returns the timestamps of severed edges.
    ///
    /// # Errors
    ///
    /// - [`SocialGraphError::AlreadyBlocked`] if the block already exists.
    pub fn block(&mut self) -> Result<SeveredFollows, SocialGraphError> {
        if self.actor_blocks_target {
            return Err(SocialGraphError::AlreadyBlocked {
                actor_id:  self.actor_id.as_str(),
                target_id: self.target_id.as_str(),
            });
        }
        let severed = SeveredFollows {
            actor_to_target: self.actor_follows_target_since.take(),
            target_to_actor: self.target_follows_actor_since.take(),
            actor_request:   self.actor_requested_target_at.take(),
            target_request:  self.target_requested_actor_at.take(),
        };
        self.actor_blocks_target = true;
        let now = Utc::now();
        self.pending_events.push(DomainEvent::ProfileBlocked(ProfileBlocked {
            actor_id:              self.actor_id,
            target_id:             self.target_id,
            blocked_at:            now,
            severed_actor_follow:  severed.actor_to_target.is_some(),
            severed_target_follow: severed.target_to_actor.is_some(),
        }));
        Ok(severed)
    }

    /// Removes the block that the actor placed on the target.
    ///
    /// Does not restore severed follows (the user must re-follow explicitly).
    ///
    /// # Errors
    ///
    /// - [`SocialGraphError::NotBlocked`] if no block exists.
    pub fn unblock(&mut self) -> Result<(), SocialGraphError> {
        if !self.actor_blocks_target {
            return Err(SocialGraphError::NotBlocked {
                actor_id:  self.actor_id.as_str(),
                target_id: self.target_id.as_str(),
            });
        }
        self.actor_blocks_target = false;
        self.pending_events.push(DomainEvent::ProfileUnblocked(ProfileUnblocked {
            actor_id:     self.actor_id,
            target_id:    self.target_id,
            unblocked_at: Utc::now(),
        }));
        Ok(())
    }

    // ── Accessors ─────────────────────────────────────────────────────────────

    pub fn status(&self) -> RelationStatus {
        if self.actor_blocks_target {
            return RelationStatus::Blocking;
        }
        if self.target_blocks_actor {
            return RelationStatus::BlockedBy;
        }
        match (
            self.actor_follows_target_since.is_some(),
            self.target_follows_actor_since.is_some(),
        ) {
            (true,  true)  => RelationStatus::MutualFollow,
            (true,  false) => RelationStatus::Following,
            (false, true)  => RelationStatus::FollowedBy,
            (false, false) if self.actor_requested_target_at.is_some() => RelationStatus::Requested,
            (false, false) => RelationStatus::None,
        }
    }

    pub fn actor_follows_target_since(&self) -> Option<DateTime<Utc>> {
        self.actor_follows_target_since
    }

    pub fn target_follows_actor_since(&self) -> Option<DateTime<Utc>> {
        self.target_follows_actor_since
    }

    pub fn actor_blocks_target(&self) -> bool {
        self.actor_blocks_target
    }

    pub fn target_blocks_actor(&self) -> bool {
        self.target_blocks_actor
    }

    /// Drains and returns all accumulated domain events.
    pub fn take_events(&mut self) -> Vec<DomainEvent> {
        std::mem::take(&mut self.pending_events)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn relation(ctx: RelationContext) -> Relation {
        Relation::from_context(ProfileId::from_uuid(uuid::Uuid::now_v7()), ProfileId::from_uuid(uuid::Uuid::now_v7()), ctx)
    }

    fn none() -> RelationContext {
        RelationContext {
            actor_follows_target_since: None,
            target_follows_actor_since: None,
            actor_blocks_target:        false,
            target_blocks_actor:        false,
            actor_requested_target_at:  None,
            target_requested_actor_at:  None,
        }
    }

    #[test]
    fn a_private_profile_gets_a_request_not_a_follow() {
        let mut r = relation(none());
        let outcome = r.follow(true).unwrap();
        assert!(matches!(outcome, FollowOutcome::Requested { .. }));
        assert_eq!(r.status(), RelationStatus::Requested);
        assert_eq!(r.actor_follows_target_since(), None);
        assert!(
            matches!(r.take_events().as_slice(), [DomainEvent::FollowRequested(e)] if e.actor_id == r.actor_id),
            "a request announces itself (its owner is told), not a follow"
        );
        assert!(matches!(r.follow(true).unwrap_err(), SocialGraphError::AlreadyRequested { .. }));
    }

    #[test]
    fn approving_turns_the_request_into_a_follow() {
        let requested_at = Utc::now();
        let mut r = relation(RelationContext { actor_requested_target_at: Some(requested_at), ..none() });
        let (was, followed_at) = r.approve_request().unwrap();
        assert_eq!(was, requested_at);
        assert_eq!(r.actor_follows_target_since(), Some(followed_at));
        assert_eq!(r.status(), RelationStatus::Following);
        assert!(matches!(r.take_events().as_slice(), [DomainEvent::ProfileFollowed(e)] if e.via_request));
        assert!(matches!(r.approve_request().unwrap_err(), SocialGraphError::NoFollowRequest { .. }));
    }

    #[test]
    fn a_withdrawn_request_leaves_nothing() {
        let mut r = relation(RelationContext { actor_requested_target_at: Some(Utc::now()), ..none() });
        r.withdraw_request().unwrap();
        assert_eq!(r.status(), RelationStatus::None);
        assert!(matches!(r.withdraw_request().unwrap_err(), SocialGraphError::NoFollowRequest { .. }));
    }

    /// A request left from when the profile was private is moot once it is
    /// public: following clears it.
    #[test]
    fn following_a_now_public_profile_clears_the_old_request() {
        let requested_at = Utc::now();
        let mut r = relation(RelationContext { actor_requested_target_at: Some(requested_at), ..none() });
        let outcome = r.follow(false).unwrap();
        assert!(matches!(outcome, FollowOutcome::Followed { cleared_request: Some(at), .. } if at == requested_at));
        assert_eq!(r.status(), RelationStatus::Following);
    }

    #[test]
    fn a_block_drops_pending_requests_both_ways_and_gates_new_ones() {
        let mut r = relation(RelationContext {
            actor_requested_target_at: Some(Utc::now()),
            target_requested_actor_at: Some(Utc::now()),
            ..none()
        });
        let severed = r.block().unwrap();
        assert!(severed.actor_request.is_some() && severed.target_request.is_some());
        assert!(matches!(r.follow(true).unwrap_err(), SocialGraphError::BlockGateDenied { .. }));
    }
}
