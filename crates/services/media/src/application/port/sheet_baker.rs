//! The animated-avatar engine (#945, #948): bakes a short clip into sprite
//! sheets that loop seamlessly, and a still of their last frame. Runs in the
//! `media-worker`, beside the video transcoder.

use async_trait::async_trait;

use crate::application::port::TranscodeOutput;
use crate::domain::value_object::{ContentHash, ObjectRoot, StorageKey};
use crate::error::MediaError;

#[async_trait]
pub trait SheetBaker: Send + Sync + 'static {
    /// Bakes the validated clip at `source` into the `SheetSmall` and
    /// `SheetLarge` sheets and the `Poster` still (the sheets' last frame),
    /// written under `root`. A clip that cannot make a sheet (shorter than 2 s,
    /// or whose loop seam stays visible) is [`MediaError::CorruptMedia`]: the
    /// asset fails, nothing is retried.
    async fn bake(&self, source: &StorageKey, hash: &ContentHash, root: &ObjectRoot) -> Result<TranscodeOutput, MediaError>;
}
