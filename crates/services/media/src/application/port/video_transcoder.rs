//! The video transformation engine (Plane B) — the sibling of [`ImageProcessor`]
//! for video. It reads the validated source object and produces the adaptive
//! HLS ladder (a master playlist over per-rung media playlists + segments), a still
//! poster, and a BlurHash placeholder, writing every object to the content-
//! addressed store and returning the deliverable renditions.
//!
//! This runs in the dedicated `media-worker` (ffmpeg is CPU-heavy) rather than the
//! request path; the concrete adapter lands in the infrastructure layer.
//!
//! [`ImageProcessor`]: crate::application::port::ImageProcessor

use async_trait::async_trait;

use crate::domain::aggregate::Rendition;
use crate::domain::value_object::{Blurhash, ContentHash, ObjectRoot, StorageKey};
use crate::error::MediaError;

/// The derivatives produced for a video asset: the deliverable renditions (the
/// `Manifest` playback entry point and the `Poster` still) plus a BlurHash derived
/// from the poster frame. The per-rung playlists and segments are written to the
/// store and referenced by the master playlist, not returned individually.
#[derive(Debug, Clone)]
pub struct TranscodeOutput {
    pub renditions: Vec<Rendition>,
    pub blurhash: Blurhash,
}

#[async_trait]
pub trait VideoTranscoder: Send + Sync + 'static {
    /// Transcodes the validated video master at `source` into the adaptive HLS
    /// ladder + poster, writing every object to the store under `root` (the
    /// content-addressed tree of `hash`, or a SIGNED asset's restricted tree,
    /// #919). Returns the deliverable renditions + poster BlurHash.
    async fn transcode(
        &self,
        source: &StorageKey,
        hash: &ContentHash,
        root: &ObjectRoot,
    ) -> Result<TranscodeOutput, MediaError>;
}
