use std::process::Command;
use std::sync::Arc;

use async_trait::async_trait;
use serde::Deserialize;
use sha2::{Digest, Sha256};

use crate::application::port::{MediaProbe, MediaProbeReport};
use crate::domain::value_object::{ContentHash, Dimensions, MimeType, StorageKey};
use crate::error::MediaError;

use crate::infrastructure::store::S3Client;

/// The video codecs we accept for ingest. H.264 is the universal-playback baseline
/// (Phase 2's transcode ladder targets it); HEVC covers iOS-native captures. AV1 /
/// VP9 are a later additive rung. A stream in any other codec is rejected as
/// `UnsupportedCodec` at finalize.
const ALLOWED_VIDEO_CODECS: &[&str] = &["h264", "hevc"];

/// ffprobe-backed [`MediaProbe`] for video. Downloads the uploaded object, runs
/// `ffprobe` over it for the verified facts (a real video stream in an allowed
/// codec, true dimensions), and returns them — the "never trust the client" gate
/// for the video plane, mirroring [`super::ImageMediaProbe`] for images.
///
/// The probe runs synchronously in the finalize path, bounded by the video
/// [`MediaKind::max_bytes`](crate::domain::value_object::MediaKind::max_bytes)
/// ceiling; the `ffprobe` subprocess is offloaded with `spawn_blocking`.
pub struct VideoMediaProbe {
    store: Arc<S3Client>,
}

impl VideoMediaProbe {
    pub fn new(store: Arc<S3Client>) -> Self {
        Self { store }
    }
}

#[async_trait]
impl MediaProbe for VideoMediaProbe {
    async fn probe(
        &self,
        key: &StorageKey,
        declared_mime: &MimeType,
    ) -> Result<MediaProbeReport, MediaError> {
        let bytes = self.store.get_bytes(key.as_str()).await?;

        // SHA-256 of the original bytes (content-addressing + dedup), same as the
        // image path.
        let mut hasher = Sha256::new();
        hasher.update(&bytes);
        let hex: String = hasher.finalize().iter().map(|b| format!("{b:02x}")).collect();
        let content_hash = ContentHash::new(hex.clone())?;
        let byte_size = bytes.len() as u64;

        // ffprobe needs a seekable file (an MP4 `moov` atom may sit at the tail),
        // so stage the bytes to a temp file and run the subprocess off the async
        // runtime. The staged file is removed regardless of outcome.
        let facts =
            tokio::task::spawn_blocking(move || probe_bytes_with_ffprobe(&bytes, &hex)).await.map_err(
                |e| MediaError::ProcessingFailed { reason: format!("probe task panicked: {e}") },
            )??;

        let dimensions = Dimensions::new(facts.width, facts.height)?;

        Ok(MediaProbeReport {
            // ffprobe verifies the *substance* (a real video stream in an allowed
            // codec, real dimensions). The MP4/QuickTime container subtype isn't
            // cleanly self-distinguishing in ISO-BMFF, so we carry the declared
            // type — already validated against the kind's allowlist at ticket time.
            mime_type: declared_mime.clone(),
            byte_size,
            dimensions: Some(dimensions),
            content_hash,
        })
    }
}

/// Stages `bytes` to a temp file, runs `ffprobe`, and interprets the result.
/// Blocking: intended to run inside `spawn_blocking`.
fn probe_bytes_with_ffprobe(bytes: &[u8], name: &str) -> Result<VideoFacts, MediaError> {
    let path = std::env::temp_dir().join(format!("media-probe-{name}.bin"));
    std::fs::write(&path, bytes)
        .map_err(|e| MediaError::ProcessingFailed { reason: format!("failed to stage probe input: {e}") })?;

    let result = run_ffprobe(&path);
    // Best-effort cleanup on every path.
    let _ = std::fs::remove_file(&path);

    let json = result?;
    interpret_ffprobe(&json)
}

/// Executes `ffprobe` over a staged file, returning its JSON stdout.
fn run_ffprobe(path: &std::path::Path) -> Result<String, MediaError> {
    let output = Command::new("ffprobe")
        .args(["-v", "error", "-print_format", "json", "-show_format", "-show_streams"])
        .arg(path)
        .output()
        .map_err(|e| MediaError::ProcessingFailed { reason: format!("ffprobe could not run: {e}") })?;

    if !output.status.success() {
        // ffprobe rejects the input itself (not a decodable media container).
        let stderr = String::from_utf8_lossy(&output.stderr);
        return Err(MediaError::CorruptMedia {
            reason: format!("ffprobe rejected the input: {}", stderr.trim()),
        });
    }
    String::from_utf8(output.stdout)
        .map_err(|e| MediaError::ProcessingFailed { reason: format!("ffprobe emitted non-UTF8: {e}") })
}

/// The verified facts we extract from an `ffprobe` report.
#[derive(Debug, PartialEq)]
struct VideoFacts {
    width: u32,
    height: u32,
}

#[derive(Deserialize)]
struct FfprobeOutput {
    #[serde(default)]
    streams: Vec<FfprobeStream>,
}

#[derive(Deserialize)]
struct FfprobeStream {
    codec_type: Option<String>,
    codec_name: Option<String>,
    width: Option<u32>,
    height: Option<u32>,
}

/// Parses and validates an `ffprobe` JSON report: there must be a video stream,
/// its codec must be allowed, and it must carry real dimensions.
fn interpret_ffprobe(json: &str) -> Result<VideoFacts, MediaError> {
    let parsed: FfprobeOutput = serde_json::from_str(json)
        .map_err(|e| MediaError::CorruptMedia { reason: format!("unparseable ffprobe report: {e}") })?;

    let video = parsed
        .streams
        .iter()
        .find(|s| s.codec_type.as_deref() == Some("video"))
        .ok_or_else(|| MediaError::CorruptMedia {
            reason: "no video stream found in the uploaded object".to_owned(),
        })?;

    let codec = video.codec_name.as_deref().unwrap_or("unknown");
    if !ALLOWED_VIDEO_CODECS.contains(&codec) {
        return Err(MediaError::UnsupportedCodec { codec: codec.to_owned() });
    }

    match (video.width, video.height) {
        (Some(w), Some(h)) if w > 0 && h > 0 => Ok(VideoFacts { width: w, height: h }),
        _ => Err(MediaError::CorruptMedia {
            reason: "video stream is missing valid dimensions".to_owned(),
        }),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn report(codec: &str, w: &str, h: &str) -> String {
        format!(
            r#"{{"streams":[
                {{"codec_type":"audio","codec_name":"aac"}},
                {{"codec_type":"video","codec_name":"{codec}","width":{w},"height":{h}}}
            ],"format":{{"format_name":"mov,mp4,m4a","duration":"12.5"}}}}"#
        )
    }

    #[test]
    fn accepts_an_h264_video_stream_with_dimensions() {
        let facts = interpret_ffprobe(&report("h264", "1080", "1920")).unwrap();
        assert_eq!(facts, VideoFacts { width: 1080, height: 1920 });
    }

    #[test]
    fn accepts_hevc_from_ios_captures() {
        assert!(interpret_ffprobe(&report("hevc", "720", "1280")).is_ok());
    }

    #[test]
    fn rejects_a_disallowed_codec_as_unsupported() {
        let err = interpret_ffprobe(&report("vp9", "1080", "1920")).unwrap_err();
        assert!(matches!(err, MediaError::UnsupportedCodec { codec } if codec == "vp9"));
    }

    #[test]
    fn rejects_a_file_with_no_video_stream() {
        // An audio-only (or image-masquerading) upload — no video stream.
        let json = r#"{"streams":[{"codec_type":"audio","codec_name":"aac"}],"format":{}}"#;
        assert!(matches!(interpret_ffprobe(json).unwrap_err(), MediaError::CorruptMedia { .. }));
    }

    #[test]
    fn rejects_a_video_stream_without_dimensions() {
        let json = r#"{"streams":[{"codec_type":"video","codec_name":"h264"}],"format":{}}"#;
        assert!(matches!(interpret_ffprobe(json).unwrap_err(), MediaError::CorruptMedia { .. }));
    }

    #[test]
    fn rejects_unparseable_output() {
        assert!(matches!(interpret_ffprobe("not json").unwrap_err(), MediaError::CorruptMedia { .. }));
    }

    /// End-to-end over the real `ffprobe` binary and temp-file plumbing. Ignored by
    /// default (needs `ffmpeg`/`ffprobe` on PATH); run with:
    /// `cargo test -p media -- --ignored real_ffprobe`.
    #[test]
    #[ignore]
    fn real_ffprobe_reads_a_generated_h264_clip() {
        // Generate a tiny vertical H.264 clip with ffmpeg.
        let out = std::env::temp_dir().join("media-probe-fixture.mp4");
        let status = Command::new("ffmpeg")
            .args(["-y", "-f", "lavfi", "-i", "testsrc=duration=1:size=1080x1920:rate=30"])
            .args(["-c:v", "libx264", "-pix_fmt", "yuv420p", "-t", "1"])
            .arg(&out)
            .status()
            .expect("ffmpeg should run");
        assert!(status.success());
        let bytes = std::fs::read(&out).expect("read fixture");
        let _ = std::fs::remove_file(&out);

        let facts = probe_bytes_with_ffprobe(&bytes, "e2e-fixture").expect("probe should accept");
        assert_eq!(facts, VideoFacts { width: 1080, height: 1920 });
    }
}
