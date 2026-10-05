use crate::error::NotificationError;

/// Semantic type of the action that generated the notification.
///
/// The integer representation is stored as `tinyint` in ScyllaDB and matches
/// the proto enum ordinal (proto value = domain tinyint).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum NotificationKind {
    Reaction,
    Comment,
    Reply,
    Mention,
    /// A profile started following the recipient (#755).
    Follow,
    /// A profile asked to follow the recipient's private profile (#755).
    FollowRequest,
    /// The recipient's follow request was accepted (#755).
    FollowAccepted,
}

impl NotificationKind {
    pub fn as_tinyint(self) -> i8 {
        match self {
            Self::Reaction => 1,
            Self::Comment  => 2,
            Self::Reply    => 3,
            Self::Mention  => 4,
            Self::Follow         => 5,
            Self::FollowRequest  => 6,
            Self::FollowAccepted => 7,
        }
    }

    pub fn from_tinyint(v: i8) -> Result<Self, NotificationError> {
        match v {
            1 => Ok(Self::Reaction),
            2 => Ok(Self::Comment),
            3 => Ok(Self::Reply),
            4 => Ok(Self::Mention),
            5 => Ok(Self::Follow),
            6 => Ok(Self::FollowRequest),
            7 => Ok(Self::FollowAccepted),
            n => Err(NotificationError::UnknownNotificationKind { kind: n.to_string() }),
        }
    }

    pub fn from_proto(v: i32) -> Result<Self, NotificationError> {
        Self::from_tinyint(v as i8)
    }

    pub fn as_str(self) -> &'static str {
        match self {
            Self::Reaction => "reaction",
            Self::Comment  => "comment",
            Self::Reply    => "reply",
            Self::Mention  => "mention",
            Self::Follow         => "follow",
            Self::FollowRequest  => "follow_request",
            Self::FollowAccepted => "follow_accepted",
        }
    }
}
