use async_trait::async_trait;

use crate::error::EngagementError;

/// Which tabs each profile's owner shows (#829), from profile's
/// `ProfileTabSettingsChanged`; shown when never told otherwise.
#[async_trait]
pub trait ProfileTabs: Send + Sync + 'static {
    async fn shows_likes(&self, profile_id: &str) -> Result<bool, EngagementError>;

    async fn set_shows_likes(&self, profile_id: &str, shown: bool) -> Result<(), EngagementError>;

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
