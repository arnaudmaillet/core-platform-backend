use chrono::{DateTime, Duration, Utc};

use super::ProfileId;

/// How long a declined request keeps its sender from asking again (#656).
pub const DECLINE_COOLDOWN: Duration = Duration::days(30);

/// Where a direct conversation stands with its recipient (#656).
///
/// A conversation the recipient's settings admit is `Open` from the start. One
/// they do not is a request: the requester may send **one** message, which
/// waits in the recipient's requests until they accept (reply or accept) or
/// decline. A declined request looks pending to its sender, forever: they may
/// not write again, nor ask anew for [`DECLINE_COOLDOWN`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MessageRequest {
    Open,
    Pending { requester: ProfileId },
    Declined { requester: ProfileId, at: DateTime<Utc> },
}

impl MessageRequest {
    /// Stored as `tinyint`.
    pub fn as_tinyint(self) -> i8 {
        match self {
            Self::Open => 0,
            Self::Pending { .. } => 1,
            Self::Declined { .. } => 2,
        }
    }

    pub fn requester(self) -> Option<ProfileId> {
        match self {
            Self::Open => None,
            Self::Pending { requester } | Self::Declined { requester, .. } => Some(requester),
        }
    }

    /// Not yet accepted: no presence, typing or read receipts either way.
    pub fn is_unanswered(self) -> bool {
        !matches!(self, Self::Open)
    }

    /// Rebuilds the state from its stored columns; anything inconsistent (a
    /// request without its requester) reads as `Open`, as rows from before
    /// requests existed do.
    pub fn from_columns(state: Option<i8>, requester: Option<ProfileId>, declined_at: Option<DateTime<Utc>>) -> Self {
        match (state, requester) {
            (Some(1), Some(requester)) => Self::Pending { requester },
            (Some(2), Some(requester)) => Self::Declined { requester, at: declined_at.unwrap_or_default() },
            _ => Self::Open,
        }
    }

    /// May the declined requester ask again at `now`?
    pub fn cooled_down(self, now: DateTime<Utc>) -> bool {
        matches!(self, Self::Declined { at, .. } if now - at >= DECLINE_COOLDOWN)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_stored_form_round_trips_and_a_bare_state_is_open() {
        let requester = ProfileId::from_uuid(uuid::Uuid::now_v7());
        let at = Utc::now();
        for state in [
            MessageRequest::Open,
            MessageRequest::Pending { requester },
            MessageRequest::Declined { requester, at },
        ] {
            let back = MessageRequest::from_columns(Some(state.as_tinyint()), state.requester(), Some(at));
            assert_eq!(back, state);
        }
        assert_eq!(MessageRequest::from_columns(None, None, None), MessageRequest::Open);
        assert_eq!(MessageRequest::from_columns(Some(1), None, None), MessageRequest::Open);
    }

    #[test]
    fn a_decline_cools_down_after_thirty_days() {
        let requester = ProfileId::from_uuid(uuid::Uuid::now_v7());
        let now = Utc::now();
        let declined = |days| MessageRequest::Declined { requester, at: now - Duration::days(days) };
        assert!(!declined(29).cooled_down(now));
        assert!(declined(30).cooled_down(now));
        assert!(!MessageRequest::Pending { requester }.cooled_down(now));
    }
}
