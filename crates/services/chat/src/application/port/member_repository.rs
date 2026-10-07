use async_trait::async_trait;

use crate::domain::aggregate::Participant;
use crate::domain::value_object::{ConversationId, MessageId, ProfileId, Role};
use crate::error::ChatError;

/// Persistence port for the bounded Member Plane roster
/// (`chat.members_by_conversation`).
///
/// All operations are single-partition: the roster is bounded (<= 500), so the
/// full-roster `list` is a safe, token-aware read.
#[async_trait]
pub trait MemberRepository: Send + Sync + 'static {
    /// Adds a participant to the roster.
    async fn insert(
        &self,
        conversation_id: &ConversationId,
        participant:     &Participant,
    ) -> Result<(), ChatError>;

    /// Reads a single participant, or `None` if the profile is not a member.
    /// This is the authorization probe on the write/admin path.
    async fn find(
        &self,
        conversation_id: &ConversationId,
        member_id:       &ProfileId,
    ) -> Result<Option<Participant>, ChatError>;

    /// Advances a member's read-receipt horizon.
    async fn update_last_read(
        &self,
        conversation_id: &ConversationId,
        member_id:       &ProfileId,
        last_read:       MessageId,
    ) -> Result<(), ChatError>;

    /// Mutes (`Some`: until then) or unmutes a member's pushes from the
    /// conversation (#654). `false` when they are not a member.
    async fn set_muted_until(
        &self,
        conversation_id: &ConversationId,
        member_id:       &ProfileId,
        muted_until:     Option<chrono::DateTime<chrono::Utc>>,
    ) -> Result<bool, ChatError>;

    /// Lists the full (bounded) roster.
    async fn list(
        &self,
        conversation_id: &ConversationId,
    ) -> Result<Vec<Participant>, ChatError>;

    /// `participant` leaves at `at` (#656): off the roster, its membership
    /// kept with `left_at` (one logged batch).
    async fn leave(
        &self,
        conversation_id: &ConversationId,
        participant:     &Participant,
        at:              chrono::DateTime<chrono::Utc>,
    ) -> Result<(), ChatError>;

    /// `member_id`'s membership of `conversation_id`, current or ended.
    async fn find_membership(
        &self,
        member_id:       &ProfileId,
        conversation_id: &ConversationId,
    ) -> Result<Option<Membership>, ChatError>;

    /// Removes a participant from the roster, membership and all.
    async fn delete(
        &self,
        conversation_id: &ConversationId,
        member_id:       &ProfileId,
    ) -> Result<(), ChatError>;

    /// The conversations `member_id` is or was a member of (#653: the GDPR
    /// export), up to `limit` after the conversation `after`, by id.
    async fn list_by_member(
        &self,
        member_id: &ProfileId,
        limit:     i32,
        after:     Option<&ConversationId>,
    ) -> Result<Vec<Membership>, ChatError>;

    /// Indexes every roster row by its member (memberships from before
    /// `conversations_by_member` existed). Idempotent; returns rows written.
    async fn backfill_member_index(&self) -> Result<u64, ChatError>;
}

/// A profile's membership of one conversation (#653).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Membership {
    pub conversation_id: ConversationId,
    pub role:            Role,
    pub joined_at:       chrono::DateTime<chrono::Utc>,
    /// When the profile left (#656); `None` while a member.
    pub left_at:         Option<chrono::DateTime<chrono::Utc>>,
}
