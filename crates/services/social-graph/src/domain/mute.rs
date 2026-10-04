//! Mutes (#659): what a profile chose to stop seeing of another. The muted
//! profile is not told and nothing is severed (unlike a block); the feeds
//! leave out a muted author's posts for the muter.

use chrono::{DateTime, Utc};

use crate::domain::value_object::ProfileId;

/// One kind of content a mute covers.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MuteScope {
    /// The muted profile's posts, in the muter's feeds.
    Posts,
    /// Their stories (no stories surface yet: stored for the client).
    Stories,
    /// Their messages (no message notifications yet: stored for the client).
    Messages,
}

/// The scopes one mute covers. A mute covers at least one.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct MuteScopes {
    pub posts:    bool,
    pub stories:  bool,
    pub messages: bool,
}

impl MuteScopes {
    pub fn any(&self) -> bool {
        self.posts || self.stories || self.messages
    }

    pub fn covers(&self, scope: MuteScope) -> bool {
        match scope {
            MuteScope::Posts => self.posts,
            MuteScope::Stories => self.stories,
            MuteScope::Messages => self.messages,
        }
    }
}

/// A profile the muter muted, and how.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Mute {
    pub profile_id: ProfileId,
    pub scopes:     MuteScopes,
    pub muted_at:   DateTime<Utc>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn scopes_cover_exactly_what_they_name() {
        let posts_only = MuteScopes { posts: true, ..MuteScopes::default() };
        assert!(posts_only.any());
        assert!(posts_only.covers(MuteScope::Posts));
        assert!(!posts_only.covers(MuteScope::Stories));
        assert!(!posts_only.covers(MuteScope::Messages));
        assert!(!MuteScopes::default().any());
    }
}
