use chrono::{DateTime, Utc};

use crate::domain::value_object::{MessageId, ProfileId, Role};
use crate::error::ChatError;

/// A member of the bounded Member Plane — a row in `members_by_conversation`.
///
/// A `Participant` only ever holds a member-plane [`Role`] (owner/admin/member);
/// construction rejects audience roles, structurally guaranteeing that
/// subscribers and guests can never enter the aggregate's roster. `last_read`
/// is the per-member read-receipt horizon, the only per-recipient state in the
/// system and bounded to O(members) by design.
pub struct Participant {
    profile_id: ProfileId,
    role:       Role,
    joined_at:  DateTime<Utc>,
    /// Read-receipt horizon: the newest `MessageId` this member has read. `None`
    /// until the member reads anything.
    last_read:  Option<MessageId>,
    /// No push from this conversation before this instant (#654);
    /// [`muted_forever`] until unmuted.
    muted_until: Option<DateTime<Utc>>,
}

/// A mute with no end, as stored: 9999-12-31 (millisecond-exact, so it reads
/// back equal).
pub fn muted_forever() -> DateTime<Utc> {
    DateTime::from_timestamp(253_402_214_400, 0).unwrap_or(DateTime::<Utc>::MAX_UTC)
}

impl Participant {
    /// Admits a profile to the Member Plane with `role`.
    ///
    /// Returns [`ChatError::InvalidParticipantRole`] if `role` is an audience
    /// role, enforcing the aggregate-boundary cut at the type's only entry point.
    pub fn new(profile_id: ProfileId, role: Role) -> Result<Self, ChatError> {
        if !role.is_member_plane() {
            return Err(ChatError::InvalidParticipantRole { role: role.as_str().to_owned() });
        }
        Ok(Self {
            profile_id,
            role,
            joined_at: Utc::now(),
            last_read: None,
            muted_until: None,
        })
    }

    /// Reconstitutes a participant from a persisted row.
    pub fn reconstitute(
        profile_id: ProfileId,
        role:       Role,
        joined_at:  DateTime<Utc>,
        last_read:  Option<MessageId>,
    ) -> Self {
        Self { profile_id, role, joined_at, last_read, muted_until: None }
    }

    /// The stored mute (`None`: not muted).
    pub fn with_muted_until(mut self, muted_until: Option<DateTime<Utc>>) -> Self {
        self.muted_until = muted_until;
        self
    }

    /// Advances the read-receipt horizon. Monotone: an out-of-order or stale
    /// acknowledgement (a `message_id` not newer than the current horizon) is
    /// ignored, so receipts never move backwards.
    pub fn mark_read(&mut self, message_id: MessageId) {
        match self.last_read {
            Some(current) if message_id <= current => {}
            _ => self.last_read = Some(message_id),
        }
    }

    pub fn profile_id(&self) -> ProfileId         { self.profile_id }
    pub fn role(&self)       -> Role              { self.role }
    pub fn joined_at(&self)  -> DateTime<Utc>     { self.joined_at }
    pub fn last_read(&self)  -> Option<MessageId> { self.last_read }
    pub fn muted_until(&self) -> Option<DateTime<Utc>> { self.muted_until }

    /// Whether this member's pushes from the conversation are off at `now`.
    pub fn is_muted(&self, now: DateTime<Utc>) -> bool {
        self.muted_until.is_some_and(|until| now < until)
    }

    /// Whether this participant may toggle the conversation's visibility.
    pub fn can_administer(&self) -> bool {
        self.role.can_administer()
    }

    /// Whether this participant may post messages.
    pub fn can_write(&self) -> bool {
        self.role.can_write()
    }
}
