use crate::error::NotificationError;

/// Discriminates the entity acted upon — drives deep-link routing on the client.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum SubjectKind {
    Post,
    Comment,
    /// A profile (follows, follow requests): the other profile.
    Profile,
    /// A moderation appeal (its outcome, #744).
    Appeal,
    /// An account: the other side of a family supervision (#670).
    Account,
}

impl SubjectKind {
    pub fn as_tinyint(self) -> i8 {
        match self {
            Self::Post    => 1,
            Self::Comment => 2,
            Self::Profile => 3,
            Self::Appeal  => 4,
            Self::Account => 5,
        }
    }

    /// The wire name, as a push's payload carries it (#654).
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Post    => "post",
            Self::Comment => "comment",
            Self::Profile => "profile",
            Self::Appeal  => "appeal",
            Self::Account => "account",
        }
    }

    pub fn from_tinyint(v: i8) -> Result<Self, NotificationError> {
        match v {
            1 => Ok(Self::Post),
            2 => Ok(Self::Comment),
            3 => Ok(Self::Profile),
            4 => Ok(Self::Appeal),
            5 => Ok(Self::Account),
            n => Err(NotificationError::UnknownSubjectKind { kind: n.to_string() }),
        }
    }

    pub fn from_proto(v: i32) -> Result<Self, NotificationError> {
        Self::from_tinyint(v as i8)
    }
}
