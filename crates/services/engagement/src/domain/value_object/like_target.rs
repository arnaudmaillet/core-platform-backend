use std::fmt;

use crate::error::EngagementError;

/// What a like lands on (#665: a like is a point staked in the wallet).
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum LikeTarget {
    Post(String),
    Comment(String),
}

impl LikeTarget {
    /// From the wallet's `target_kind` and `target_id`.
    pub fn parse(kind: &str, id: &str) -> Result<Self, EngagementError> {
        if id.is_empty() || id.len() > 64 {
            return Err(EngagementError::InvalidLikeTarget { value: format!("{kind}:{id}") });
        }
        match kind {
            "post" => Ok(Self::Post(id.to_owned())),
            "comment" => Ok(Self::Comment(id.to_owned())),
            _ => Err(EngagementError::InvalidLikeTarget { value: format!("{kind}:{id}") }),
        }
    }

    pub fn kind(&self) -> &'static str {
        match self {
            Self::Post(_) => "post",
            Self::Comment(_) => "comment",
        }
    }

    pub fn id(&self) -> &str {
        match self {
            Self::Post(id) | Self::Comment(id) => id,
        }
    }
}

impl fmt::Display for LikeTarget {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}:{}", self.kind(), self.id())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn targets_are_posts_or_comments() {
        assert_eq!(LikeTarget::parse("post", "p1").unwrap().to_string(), "post:p1");
        assert_eq!(LikeTarget::parse("comment", "c1").unwrap(), LikeTarget::Comment("c1".into()));
        assert!(LikeTarget::parse("profile", "x").is_err());
        assert!(LikeTarget::parse("post", "").is_err());
    }
}
