use std::fmt;

use crate::error::MediaError;

/// A client's key for one upload, reused on every retry of its ticket (#876):
/// 1–128 printable ASCII characters, no spaces. Scoped to its owner. The iOS
/// client sends its intent's key and a prefix of the content's SHA-256
/// (`<intent>:<sha16>`), so changed bytes come under a new key.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct UploadKey(String);

impl UploadKey {
    pub const MAX_LEN: usize = 128;

    pub fn new(value: &str) -> Result<Self, MediaError> {
        let valid = (1..=Self::MAX_LEN).contains(&value.len()) && value.bytes().all(|b| b.is_ascii_graphic());
        if valid { Ok(Self(value.to_owned())) } else { Err(MediaError::InvalidUploadKey) }
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for UploadKey {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn keys_are_1_to_128_printable_ascii_characters() {
        for good in ["k", "bubble-7", "0193e7c2-7d4e-7c1a-9b1e-2f4a6c8e0a1b:9f86d081884c7d65", &"a".repeat(128)] {
            assert_eq!(UploadKey::new(good).unwrap().as_str(), good);
        }
        for bad in ["", "has space", "tab\there", "accent-é", &"a".repeat(129)] {
            assert!(matches!(UploadKey::new(bad), Err(MediaError::InvalidUploadKey)), "{bad:?} must be refused");
        }
    }
}
