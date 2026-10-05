use async_trait::async_trait;

use crate::domain::aggregate::Invitation;
use crate::domain::value_object::{ConversationId, ProfileId};
use crate::error::ChatError;

/// Persistence port for pending Member-Plane invitations
/// (`chat.invitations_by_conversation`).
///
/// Single-partition point operations. Invitations expire on their own (table
/// TTL), so there is no listing or reaping surface.
#[async_trait]
pub trait InvitationRepository: Send + Sync + 'static {
    /// Records (or refreshes) the invitation for its invitee. Re-inviting the
    /// same profile overwrites the row and restarts its expiry.
    async fn upsert(
        &self,
        conversation_id: &ConversationId,
        invitation:      &Invitation,
    ) -> Result<(), ChatError>;

    /// The pending invitation for `invitee_id`, or `None` if there is none (never
    /// issued, already consumed, or expired). This is the gate on the join path.
    async fn find(
        &self,
        conversation_id: &ConversationId,
        invitee_id:      &ProfileId,
    ) -> Result<Option<Invitation>, ChatError>;

    /// Consumes the invitation once the invitee has joined.
    async fn delete(
        &self,
        conversation_id: &ConversationId,
        invitee_id:      &ProfileId,
    ) -> Result<(), ChatError>;
}
