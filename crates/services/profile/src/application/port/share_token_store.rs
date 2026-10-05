use async_trait::async_trait;

use crate::domain::value_object::ProfileId;
use crate::error::ProfileError;

/// Each profile's QR / share-link token (#661).
#[async_trait]
pub trait ShareTokenStore: Send + Sync + 'static {
    /// `profile`'s current token, if one was issued.
    async fn current(&self, profile: &ProfileId) -> Result<Option<String>, ProfileError>;

    /// Issues `token` to `profile` unless it holds one already; the token it
    /// holds afterwards (theirs if a concurrent issue won).
    async fn issue(&self, profile: &ProfileId, token: &str) -> Result<String, ProfileError>;

    /// Replaces `current` with `next`, only if `current` is still the
    /// profile's token (compare-and-set); `false` when it moved on.
    async fn replace(&self, profile: &ProfileId, current: &str, next: &str) -> Result<bool, ProfileError>;

    /// The profile `token` belongs to — only while it is that profile's
    /// current token (a rotated-out one resolves to nothing).
    async fn resolve(&self, token: &str) -> Result<Option<ProfileId>, ProfileError>;
}

/// A fresh token: 128 random bits, URL-safe base64 (22 characters).
pub fn new_share_token() -> String {
    use base64::Engine as _;
    use rand::RngCore as _;
    let mut bytes = [0u8; 16];
    rand::rng().fill_bytes(&mut bytes);
    base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(bytes)
}

/// Whether `token` looks like one [`new_share_token`] makes (cheap refusal of
/// junk before any read).
pub fn is_share_token(token: &str) -> bool {
    token.len() == 22 && token.bytes().all(|b| b.is_ascii_alphanumeric() || b == b'-' || b == b'_')
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn tokens_are_url_safe_distinct_and_recognised() {
        let (a, b) = (new_share_token(), new_share_token());
        assert_ne!(a, b);
        assert!(is_share_token(&a) && is_share_token(&b));
        assert!(!is_share_token("short"));
        assert!(!is_share_token("has/slash+plus========"));
    }
}
