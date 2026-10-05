use crate::domain::value_object::ProfileId;
use crate::error::PostError;

const MAX_CAPTION_CHARS: usize = 2200;

/// The most profiles a caption may mention.
pub const MAX_MENTIONS: usize = 20;

/// The link a client writes for a mention: `[@handle](profile:<uuid>)`.
const MENTION_LINK: &str = "](profile:";

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Caption(String);

impl Caption {
    pub fn new(s: impl Into<String>) -> Result<Self, PostError> {
        let s = s.into();
        let len = s.chars().count();
        if len > MAX_CAPTION_CHARS {
            return Err(PostError::DomainViolation {
                field:   "caption".into(),
                message: format!("caption must be at most {MAX_CAPTION_CHARS} characters (got {len})"),
            });
        }
        Ok(Self(s))
    }

    pub fn empty() -> Self {
        Self(String::new())
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }

    /// The profiles the caption mentions (`[@handle](profile:<uuid>)`), each
    /// once, in order. A malformed link is plain text, not a mention.
    pub fn mentions(&self) -> Vec<ProfileId> {
        let mut found: Vec<ProfileId> = Vec::new();
        let mut rest = self.0.as_str();
        while let Some(at) = rest.find(MENTION_LINK) {
            rest = &rest[at + MENTION_LINK.len()..];
            let Some(end) = rest.find(')') else { break };
            if let Ok(id) = uuid::Uuid::parse_str(&rest[..end]) {
                let id = ProfileId::from_uuid(id);
                if !found.contains(&id) {
                    found.push(id);
                }
            }
            rest = &rest[end..];
        }
        found
    }
}

impl From<Caption> for String {
    fn from(c: Caption) -> Self {
        c.0
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn mentions_are_read_from_the_links_each_once() {
        let (a, b) = (uuid::Uuid::now_v7(), uuid::Uuid::now_v7());
        let caption = Caption::new(format!(
            "hi [@ann](profile:{a}) and [@bob](profile:{b}), again [@ann](profile:{a}); not [@x](profile:nope) nor [@y](profile:"
        ))
        .unwrap();
        assert_eq!(caption.mentions(), vec![ProfileId::from_uuid(a), ProfileId::from_uuid(b)]);
        assert!(Caption::new("no mentions @here").unwrap().mentions().is_empty());
    }
}
