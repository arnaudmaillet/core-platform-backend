use async_trait::async_trait;

use crate::domain::value_object::{AudioId, PostId, ProfileId};
use crate::error::PostError;

/// An author's interaction defaults that bear on their posts: remix /
/// original-sound reuse (#669), downloads and like counts (#809).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ReuseDefaults {
    pub allow_remix:       bool,
    pub allow_sound_reuse: bool,
    /// Others may save the author's posts (#809).
    pub allow_downloads:   bool,
    /// Others see the author's posts' like counts (#809).
    pub show_like_counts:  bool,
}

impl Default for ReuseDefaults {
    fn default() -> Self {
        Self { allow_remix: true, allow_sound_reuse: true, allow_downloads: true, show_like_counts: true }
    }
}

/// What reuse checks need (#669): the authors' defaults, projected from
/// `profile.v1.events` (`ProfileInteractionSettingsChanged`), and which post an
/// original sound belongs to.
#[async_trait]
pub trait ReuseRegistry: Send + Sync + 'static {
    /// The author's defaults; allowed when the projection has none.
    async fn defaults(&self, author: &ProfileId) -> Result<ReuseDefaults, PostError>;

    async fn set_defaults(&self, author: &ProfileId, defaults: ReuseDefaults) -> Result<(), PostError>;

    /// Records that `audio` is the original sound of `post` by `author`.
    async fn record_origin(&self, audio: &AudioId, post: &PostId, author: &ProfileId) -> Result<(), PostError>;

    /// The post (and its author) an original sound belongs to, if known.
    async fn origin(&self, audio: &AudioId) -> Result<Option<(PostId, ProfileId)>, PostError>;
}
