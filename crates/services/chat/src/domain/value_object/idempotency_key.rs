use std::fmt;

use crate::error::ChatError;

/// A client's key for one message, reused on every retry of its send (#875):
/// 8–64 characters of letters, digits, `-` and `_` (the shape of
/// `wallet.v1`'s keys). Scoped by the caller to its sender and conversation,
/// so two senders' keys never collide.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct IdempotencyKey(String);

impl IdempotencyKey {
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl TryFrom<&str> for IdempotencyKey {
    type Error = ChatError;

    fn try_from(s: &str) -> Result<Self, Self::Error> {
        let valid = (8..=64).contains(&s.len())
            && s.bytes().all(|b| b.is_ascii_alphanumeric() || b == b'-' || b == b'_');
        if valid { Ok(Self(s.to_owned())) } else { Err(ChatError::InvalidIdempotencyKey) }
    }
}

impl fmt::Display for IdempotencyKey {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn keys_are_8_to_64_url_safe_characters() {
        let uuid = uuid::Uuid::now_v7().to_string();
        assert_eq!(IdempotencyKey::try_from(uuid.as_str()).unwrap().as_str(), uuid);
        assert!(IdempotencyKey::try_from("abc_DEF-").is_ok());
        assert!(IdempotencyKey::try_from("a".repeat(64).as_str()).is_ok());

        for bad in ["", "short", "a".repeat(65).as_str(), "has space1", "colon:key", "accent-é-key"] {
            assert!(
                matches!(IdempotencyKey::try_from(bad), Err(ChatError::InvalidIdempotencyKey)),
                "{bad:?} must be refused",
            );
        }
    }
}
