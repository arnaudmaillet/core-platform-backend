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
}

impl SubjectKind {
    pub fn as_tinyint(self) -> i8 {
        match self {
            Self::Post    => 1,
            Self::Comment => 2,
            Self::Profile => 3,
            Self::Appeal  => 4,
        }
    }

    pub fn from_tinyint(v: i8) -> Result<Self, NotificationError> {
        match v {
            1 => Ok(Self::Post),
            2 => Ok(Self::Comment),
            3 => Ok(Self::Profile),
            4 => Ok(Self::Appeal),
            n => Err(NotificationError::UnknownSubjectKind { kind: n.to_string() }),
        }
    }

    pub fn from_proto(v: i32) -> Result<Self, NotificationError> {
        Self::from_tinyint(v as i8)
    }
}
