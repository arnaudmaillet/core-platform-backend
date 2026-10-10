use async_trait::async_trait;

use crate::error::EngagementError;

/// A profile tab engagement serves.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Tab {
    /// The posts it liked (#829): shown unless the owner hides it.
    Likes,
    /// The posts it saved (#872): hidden unless the owner shows it.
    Saved,
}

/// Which tabs the owner shows others.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TabFlags {
    pub likes: bool,
    pub saved: bool,
}

impl Default for TabFlags {
    fn default() -> Self {
        Self { likes: true, saved: false }
    }
}

/// Which tabs each profile's owner shows (#829, #872), from profile's
/// `ProfileTabSettingsChanged`; [`TabFlags::default`] when never told.
#[async_trait]
pub trait ProfileTabs: Send + Sync + 'static {
    async fn shows(&self, profile_id: &str, tab: Tab) -> Result<bool, EngagementError>;

    async fn set_tabs(&self, profile_id: &str, flags: TabFlags) -> Result<(), EngagementError>;

    /// Forgets a deleted profile (#873): its flags and its Likes tab's rows,
    /// as of `at_micros` (the deletion's time).
    async fn forget(&self, profile_id: &str, at_micros: i64) -> Result<(), EngagementError>;
}

/// Whether readers may see a profile's content (social-graph `CheckAccess`:
/// a private profile they don't follow, a block either way, a hidden
/// profile). `None` (not wired): nobody but the owner and the mesh.
#[async_trait]
pub trait ProfileAccess: Send + Sync + 'static {
    /// Whether `viewers` (the reader's profiles; none for a guest) may see
    /// `profile_id`'s content.
    async fn visible(&self, viewers: &[String], profile_id: &str) -> Result<bool, EngagementError>;

    /// Which of `profile_ids` `viewers` may see (#873: the authors of a
    /// Likes-tab page); at most [`MAX_ACCESS_TARGETS`] per call.
    async fn visible_among(&self, viewers: &[String], profile_ids: &[String]) -> Result<Vec<String>, EngagementError>;
}

/// social-graph `CheckAccess` caps the targets per call.
pub const MAX_ACCESS_TARGETS: usize = 100;
