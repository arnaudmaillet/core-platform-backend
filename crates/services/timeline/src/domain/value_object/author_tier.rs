/// Tier classification for a post's author, mirrored from geo-discovery.
///
/// Stored as `tinyint` (i8) in ScyllaDB `posts_by_author.author_tier`.
/// Denormalized into every `PostPublished` event of `post.v1.events` by services/post
/// (sourced from services/profile via geo-discovery). Timeline consumes it
/// directly — no synchronous tier lookup required on the write path.
///
/// Fan-out routing rule (hard domain invariant):
///   Standard | Premium → `FanOutMode::Write`  (materialized Redis feeds)
///   Vip              → `FanOutMode::Read`   (per-author VIP ZSET, merged at query time)
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum AuthorTier {
    Standard = 0,
    Premium  = 1,
    Vip      = 2,
}

impl AuthorTier {
    pub fn from_u8(v: u8) -> Self {
        match v {
            1 => Self::Premium,
            2 => Self::Vip,
            _ => Self::Standard,
        }
    }

    pub fn as_u8(self) -> u8 {
        self as u8
    }

    pub fn as_i8(self) -> i8 {
        self as u8 as i8
    }

    /// Returns the fan-out routing mode for this tier.
    pub fn fan_out_mode(self) -> FanOutMode {
        match self {
            Self::Vip => FanOutMode::Read,
            _         => FanOutMode::Write,
        }
    }

    pub fn is_vip(self) -> bool {
        matches!(self, Self::Vip)
    }
}

impl From<i8> for AuthorTier {
    fn from(v: i8) -> Self {
        Self::from_u8(v as u8)
    }
}

impl From<AuthorTier> for i8 {
    fn from(t: AuthorTier) -> i8 {
        t.as_i8()
    }
}

/// Fan-out strategy derived from `AuthorTier`.
///
/// `Write`: posts are pushed to all follower feeds at publish time (hot path
/// for Standard/Premium authors with manageable follower counts).
///
/// `Read`: posts are registered in the author's per-VIP ZSET and merged into
/// each follower's feed at query time (used for VIP authors with millions of
/// followers to prevent write-amplification spikes).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FanOutMode {
    Write,
    Read,
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The hard fan-out invariant the author-tier initiative exists to enforce:
    /// a VIP author routes to the READ path (no write-fan-out to millions of
    /// feeds); Standard/Premium route to the WRITE path. With the producer chain
    /// (social-graph → profile → post) now supplying `author_tier` on the event,
    /// this routing finally fires for real authors instead of always seeing 0.
    #[test]
    fn vip_routes_to_read_path_others_to_write() {
        assert_eq!(AuthorTier::from_u8(2).fan_out_mode(), FanOutMode::Read);
        assert!(AuthorTier::from_u8(2).is_vip());

        assert_eq!(AuthorTier::from_u8(0).fan_out_mode(), FanOutMode::Write);
        assert_eq!(AuthorTier::from_u8(1).fan_out_mode(), FanOutMode::Write);
        assert!(!AuthorTier::from_u8(0).is_vip());

        // Unknown tiers degrade to Standard (write path), never accidentally VIP.
        assert_eq!(AuthorTier::from_u8(9).fan_out_mode(), FanOutMode::Write);
    }
}
