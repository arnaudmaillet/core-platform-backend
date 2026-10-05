use chrono::{DateTime, Utc};

use crate::domain::value_object::ProfileId;

/// A pending invitation for `invitee_id` to join a conversation's Member Plane —
/// a row in `invitations_by_conversation`.
///
/// It is the only way into a **private** conversation: issued by an
/// administering member through [`Conversation::invite`], checked and consumed
/// by [`Conversation::admit_joiner`]. It expires on its own (table TTL), so the
/// domain never reasons about its age.
///
/// [`Conversation::invite`]: super::Conversation::invite
/// [`Conversation::admit_joiner`]: super::Conversation::admit_joiner
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Invitation {
    invitee_id: ProfileId,
    inviter_id: ProfileId,
    invited_at: DateTime<Utc>,
}

impl Invitation {
    /// Only [`Conversation::invite`](super::Conversation::invite) mints new
    /// invitations, so the inviter's authority is checked at the sole entry point.
    pub(super) fn issue(invitee_id: ProfileId, inviter_id: ProfileId) -> Self {
        Self { invitee_id, inviter_id, invited_at: Utc::now() }
    }

    /// Reconstitutes an invitation from a persisted row.
    pub fn reconstitute(invitee_id: ProfileId, inviter_id: ProfileId, invited_at: DateTime<Utc>) -> Self {
        Self { invitee_id, inviter_id, invited_at }
    }

    pub fn invitee_id(&self) -> ProfileId     { self.invitee_id }
    pub fn inviter_id(&self) -> ProfileId     { self.inviter_id }
    pub fn invited_at(&self) -> DateTime<Utc> { self.invited_at }
}
