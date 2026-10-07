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
    /// The recipient's appeal was decided: the decision stands (#744).
    AppealUpheld,
    /// The recipient's appeal was decided in their favour (#744).
    AppealOverturned,
    /// Family supervision (#670): the recipient and the sender's account paired.
    SupervisionStarted,
    /// The other side ended the supervision, or their account was erased.
    SupervisionEnded,
    /// The supervision ended: the teen turned 18.
    SupervisionCameOfAge,
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
            Self::AppealUpheld     => 8,
            Self::AppealOverturned => 9,
            Self::SupervisionStarted   => 10,
            Self::SupervisionEnded     => 11,
            Self::SupervisionCameOfAge => 12,
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
            8 => Ok(Self::AppealUpheld),
            9 => Ok(Self::AppealOverturned),
            10 => Ok(Self::SupervisionStarted),
            11 => Ok(Self::SupervisionEnded),
            12 => Ok(Self::SupervisionCameOfAge),
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
            Self::AppealUpheld     => "appeal_upheld",
            Self::AppealOverturned => "appeal_overturned",
            Self::SupervisionStarted   => "supervision_started",
            Self::SupervisionEnded     => "supervision_ended",
            Self::SupervisionCameOfAge => "supervision_came_of_age",
        }
    }
}
