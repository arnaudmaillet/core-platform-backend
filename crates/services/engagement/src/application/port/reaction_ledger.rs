use async_trait::async_trait;

use crate::domain::value_object::{PostId, ProfileId, ReactionKind};
use crate::error::EngagementError;
use crate::infrastructure::persistence::model::ReactionRow;

/// One reaction of a profile (#653).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProfileReaction {
    pub post_id:        PostId,
    pub kind:           ReactionKind,
    pub reacted_at_ms:  i64,
}

/// Port for the ScyllaDB durable reaction ledger.
///
/// Write operations are called exclusively from background workers (not on the
/// gRPC hot path). The ledger is the source of truth for Redis cold-start recovery.
#[async_trait]
pub trait ReactionLedger: Send + Sync + 'static {
    /// Upserts a reaction record, stamped with the event's time: the latest
    /// **event** wins, whatever order the writes land in (a redelivered or
    /// late older event never overwrites a newer kind). Safe to retry.
    async fn upsert(
        &self,
        post_id:    &PostId,
        profile_id: &ProfileId,
        kind:       ReactionKind,
        weight:     i64,
        event_at_ms: i64,
    ) -> Result<(), EngagementError>;

    /// Deletes the reaction record for `(post_id, profile_id)`, stamped with
    /// the event's time like [`upsert`](Self::upsert): a reaction made after
    /// the removal survives it.
    async fn remove(
        &self,
        post_id:     &PostId,
        profile_id:  &ProfileId,
        event_at_ms: i64,
    ) -> Result<(), EngagementError>;

    /// A profile's reactions, by post id (#653: the GDPR export), from
    /// `reactions_by_profile`: up to `limit` after the post `after`.
    async fn list_by_profile(
        &self,
        profile_id: &ProfileId,
        limit:      i32,
        after:      Option<&PostId>,
    ) -> Result<Vec<ProfileReaction>, EngagementError>;

    /// Indexes every reaction of `post_reactions` by its profile (reactions
    /// from before `reactions_by_profile` existed). Idempotent; returns the
    /// rows written.
    async fn backfill_profile_index(&self) -> Result<u64, EngagementError>;

    /// Scans all reactions for `post_id`. Used during cold-start Redis reconstruction.
    async fn scan_for_recovery(
        &self,
        post_id: &PostId,
    ) -> Result<Vec<ReactionRow>, EngagementError>;

    /// Applies a view/share/comment counter delta to the ScyllaDB counter table.
    async fn apply_interaction_delta(
        &self,
        post_id:       &PostId,
        view_delta:    i64,
        share_delta:   i64,
        comment_delta: i64,
    ) -> Result<(), EngagementError>;
}
