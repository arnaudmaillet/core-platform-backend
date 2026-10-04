use async_trait::async_trait;
use chrono::{DateTime, Utc};

use crate::domain::access::AccessFacts;
use crate::domain::interaction::InteractionPolicy;
use crate::domain::list_privacy::ListPrivacy;
use crate::domain::aggregate::Relation;
use crate::domain::entity::{BlockEdge, FollowEdge};
use crate::domain::value_object::ProfileId;
use crate::error::SocialGraphError;

/// Persistence port for the social graph.
///
/// All methods are point-lookups or narrow partition scans — no ALLOW FILTERING.
/// The four ScyllaDB tables (followers, following, follow_status, blocks) are
/// partitioned to guarantee O(1) single-row access and O(page) list scans.
#[async_trait]
pub trait SocialGraphRepository: Send + Sync + 'static {
    /// Loads the full bidirectional relationship context for `(actor, target)`.
    ///
    /// Fires six concurrent ScyllaDB point-lookups:
    ///   1. `follow_status` WHERE `follower = actor  AND followee = target`
    ///   2. `follow_status` WHERE `follower = target AND followee = actor`
    ///   3. `blocks`        WHERE `blocker  = actor  AND blockee  = target`
    ///   4. `blocks`        WHERE `blocker  = target AND blockee  = actor`
    ///   5. `follow_request_status` actor → target
    ///   6. `follow_request_status` target → actor
    async fn load_relation(
        &self,
        actor_id:  &ProfileId,
        target_id: &ProfileId,
    ) -> Result<Relation, SocialGraphError>;

    /// Records a pending follow request atomically in `follow_requests` (the
    /// owner's inbox) and `follow_request_status` (the point lookup).
    async fn persist_follow_request(
        &self,
        requester_id: &ProfileId,
        target_id:    &ProfileId,
        requested_at: DateTime<Utc>,
    ) -> Result<(), SocialGraphError>;

    /// Removes a pending follow request (declined, cancelled, moot or severed
    /// by a block). `requested_at` must be the stored value.
    async fn delete_follow_request(
        &self,
        requester_id: &ProfileId,
        target_id:    &ProfileId,
        requested_at: DateTime<Utc>,
    ) -> Result<(), SocialGraphError>;

    /// Turns an approved request into a follow in ONE logged batch: the request
    /// rows go, the three follow rows appear — never both, never neither.
    async fn approve_follow_request(
        &self,
        requester_id: &ProfileId,
        target_id:    &ProfileId,
        requested_at: DateTime<Utc>,
        followed_at:  DateTime<Utc>,
    ) -> Result<(), SocialGraphError>;

    /// The pending requests to `target_id`, newest first (`followed_at` of each
    /// edge is the request time).
    async fn list_follow_requests(
        &self,
        target_id:  &ProfileId,
        limit:      i32,
        page_token: Option<&str>,
    ) -> Result<(Vec<FollowEdge>, Option<String>), SocialGraphError>;

    /// Writes a follow edge across three tables atomically as an unlogged batch:
    ///   - `following`     INSERT (follower_id, followed_at, followee_id)
    ///   - `followers`     INSERT (followee_id, followed_at, follower_id)
    ///   - `follow_status` INSERT (follower_id, followee_id, followed_at)
    async fn persist_follow(
        &self,
        actor_id:    &ProfileId,
        target_id:   &ProfileId,
        followed_at: DateTime<Utc>,
    ) -> Result<(), SocialGraphError>;

    /// Deletes a follow edge from three tables.
    ///
    /// `followed_at` must be provided by the caller (obtained from `load_relation`)
    /// because ScyllaDB requires the full clustering key for a targeted DELETE.
    async fn delete_follow(
        &self,
        actor_id:    &ProfileId,
        target_id:   &ProfileId,
        followed_at: DateTime<Utc>,
    ) -> Result<(), SocialGraphError>;

    /// Writes a block record to the `blocks` table.
    async fn persist_block(
        &self,
        blocker_id: &ProfileId,
        blockee_id: &ProfileId,
        blocked_at: DateTime<Utc>,
    ) -> Result<(), SocialGraphError>;

    /// Deletes a block record from the `blocks` table.
    async fn delete_block(
        &self,
        blocker_id: &ProfileId,
        blockee_id: &ProfileId,
    ) -> Result<(), SocialGraphError>;

    /// Paginated scan of the `followers` table (fan-in adjacency list).
    ///
    /// Page token encodes the `followed_at` millisecond timestamp of the last
    /// row returned. Absent on first page; present when `returned == limit`.
    async fn list_followers(
        &self,
        followee_id: &ProfileId,
        limit:       i32,
        page_token:  Option<&str>,
    ) -> Result<(Vec<FollowEdge>, Option<String>), SocialGraphError>;

    /// Paginated scan of the `following` table (fan-out adjacency list).
    async fn list_following(
        &self,
        follower_id: &ProfileId,
        limit:       i32,
        page_token:  Option<&str>,
    ) -> Result<(Vec<FollowEdge>, Option<String>), SocialGraphError>;

    /// Paginated scan of the `blocks` table for a given blocker.
    ///
    /// Page token encodes the UUID string of the last `blockee_id` returned.
    async fn list_blocks(
        &self,
        blocker_id: &ProfileId,
        limit:      i32,
        page_token: Option<&str>,
    ) -> Result<(Vec<BlockEdge>, Option<String>), SocialGraphError>;

    /// Loads what the access rule needs for `viewers` against `targets`, in four
    /// queries whatever the sizes: follows viewer→target, blocks both ways, and
    /// the targets' audience projection. Callers bound both slices.
    async fn load_access_facts(
        &self,
        viewers: &[ProfileId],
        targets: &[ProfileId],
    ) -> Result<AccessFacts, SocialGraphError>;

    /// Records whether the owner made `profile_id` private (audience projection).
    async fn set_profile_private(&self, profile_id: &ProfileId, private: bool) -> Result<(), SocialGraphError>;

    /// Records whether `profile_id` is hidden (audience projection).
    async fn set_profile_hidden(&self, profile_id: &ProfileId, hidden: bool) -> Result<(), SocialGraphError>;

    /// Records who may comment / mention / message `profile_id`.
    async fn set_interaction_policy(
        &self,
        profile_id: &ProfileId,
        policy: &InteractionPolicy,
    ) -> Result<(), SocialGraphError>;

    /// `profile_id`'s interaction policy; the default (everyone) when none.
    async fn load_interaction_policy(&self, profile_id: &ProfileId) -> Result<InteractionPolicy, SocialGraphError>;

    /// Records who may see `profile_id`'s follower / following lists.
    async fn set_list_privacy(&self, profile_id: &ProfileId, privacy: &ListPrivacy) -> Result<(), SocialGraphError>;

    /// `profile_id`'s list privacy; the default (everyone) when none.
    async fn load_list_privacy(&self, profile_id: &ProfileId) -> Result<ListPrivacy, SocialGraphError>;
}
