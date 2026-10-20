use std::path::Path;
use std::process::Command;
use std::sync::Arc;

use async_trait::async_trait;

use crate::application::port::{TranscodeOutput, VideoTranscoder};
use crate::domain::aggregate::Rendition;
use crate::domain::value_object::{Blurhash, ContentHash, Dimensions, MimeType, ObjectRoot, StorageKey};
use crate::error::MediaError;

use crate::infrastructure::store::S3Client;

/// One rung of the adaptive ladder: a target width (the source's 9:16 aspect is
/// preserved, height derived) and the H.264 video bitrate.
struct Rung {
    label: &'static str,
    width: u32,
    bitrate_k: u32,
}

/// The vertical short-form ladder (widest → narrowest). Rungs wider than the
/// source are dropped so nothing is upscaled.
const LADDER: [Rung; 3] = [
    Rung { label: "1080w", width: 1080, bitrate_k: 4500 },
    Rung { label: "720w", width: 720, bitrate_k: 2200 },
    Rung { label: "480w", width: 480, bitrate_k: 900 },
];

const HLS_SEGMENT_SECS: u32 = 4;
const AUDIO_BITRATE_K: u32 = 128;
const MANIFEST_MIME: &str = "application/vnd.apple.mpegurl";

/// ffmpeg-backed [`VideoTranscoder`]: turns a validated video master into the
/// 3-rung CMAF/HLS ladder (per-rung fMP4 playlists + segments under a hand-built
/// master playlist), a still poster, and a poster-derived BlurHash, writing every
/// content-addressed object to the store.
///
/// Heavy (CPU-bound ffmpeg) — runs in the dedicated `media-worker`, off the
/// request path; the subprocess work is offloaded with `spawn_blocking`.
pub struct FfmpegVideoTranscoder {
    store: Arc<S3Client>,
}

impl FfmpegVideoTranscoder {
    pub fn new(store: Arc<S3Client>) -> Self {
        Self { store }
    }
}

#[async_trait]
impl VideoTranscoder for FfmpegVideoTranscoder {
    async fn transcode(
        &self,
        source: &StorageKey,
        hash: &ContentHash,
        root: &ObjectRoot,
    ) -> Result<TranscodeOutput, MediaError> {
        let bytes = self.store.get_bytes(source.as_str()).await?;
        let hash_hex = hash.as_str().to_owned();

        // All ffmpeg + filesystem work happens off the async runtime.
        let artifacts = tokio::task::spawn_blocking(move || generate_artifacts(&bytes, &hash_hex))
            .await
            .map_err(|e| MediaError::ProcessingFailed { reason: format!("transcode task panicked: {e}") })??;

        // Publish the whole tree to the store, under its root (#919).
        for file in &artifacts.files {
            let key = root.object(&file.relative);
            self.store.put_bytes(key.as_str(), file.bytes.clone(), file.content_type).await?;
        }

        let manifest = Rendition::new(
            crate::domain::value_object::RenditionKind::Manifest,
            MimeType::new(MANIFEST_MIME)?,
            root.object("master.m3u8"),
            artifacts.top_dimensions,
            artifacts.master_size,
        );
        let poster = Rendition::new(
            crate::domain::value_object::RenditionKind::Poster,
            MimeType::new("image/jpeg")?,
            root.object("poster.jpg"),
            artifacts.poster_dimensions,
            artifacts.poster_size,
        );

        Ok(TranscodeOutput {
            renditions: vec![manifest, poster],
            blurhash: Blurhash::new(artifacts.blurhash)?,
        })
    }
}

/// One object in the HLS output tree, relative to the asset's content-addressed
/// prefix (e.g. `master.m3u8`, `1080w/seg_000.m4s`).
struct OutputFile {
    relative: String,
    bytes: Vec<u8>,
    content_type: &'static str,
}

/// The in-memory result of a transcode, ready to upload — isolated from the store
/// so it can be exercised end-to-end against real ffmpeg in tests.
struct TranscodeArtifacts {
    files: Vec<OutputFile>,
    top_dimensions: Dimensions,
    poster_dimensions: Dimensions,
    master_size: u64,
    poster_size: u64,
    blurhash: String,
}

/// Stages the source bytes, extracts the poster, transcodes each applicable rung,
/// builds the master playlist, and collects every output object into memory.
/// Blocking: runs inside `spawn_blocking`.
fn generate_artifacts(source: &[u8], hash_hex: &str) -> Result<TranscodeArtifacts, MediaError> {
    let work = std::env::temp_dir().join(format!("media-transcode-{hash_hex}"));
    let _ = std::fs::remove_dir_all(&work);
    std::fs::create_dir_all(&work)
        .map_err(|e| MediaError::ProcessingFailed { reason: format!("workdir: {e}") })?;
    let input = work.join("input");
    std::fs::write(&input, source)
        .map_err(|e| MediaError::ProcessingFailed { reason: format!("stage input: {e}") })?;

    let result = build_ladder(&input, &work);
    let _ = std::fs::remove_dir_all(&work);
    result
}

fn build_ladder(input: &Path, work: &Path) -> Result<TranscodeArtifacts, MediaError> {
    // Poster (first frame) — also the source of the true source dimensions + BlurHash.
    let poster_path = work.join("poster.jpg");
    run_ffmpeg(&[
        "-y", "-i", &input.to_string_lossy(), "-frames:v", "1", "-q:v", "3",
        &poster_path.to_string_lossy(),
    ])?;
    let poster_bytes = std::fs::read(&poster_path)
        .map_err(|e| MediaError::ProcessingFailed { reason: format!("read poster: {e}") })?;
    let poster_img = image::load_from_memory(&poster_bytes)
        .map_err(|e| MediaError::CorruptMedia { reason: format!("poster decode: {e}") })?;
    let (src_w, src_h) = (poster_img.width(), poster_img.height());
    let poster_dimensions = Dimensions::new(src_w, src_h)?;

    let rungs = plan_ladder(src_w);
    let mut files = vec![OutputFile {
        relative: "poster.jpg".to_owned(),
        content_type: "image/jpeg",
        bytes: poster_bytes.clone(),
    }];

    // Transcode each rung and collect its playlist + init + segments.
    let mut variants = Vec::new();
    for rung in &rungs {
        let out_h = even_height(rung.width, src_w, src_h);
        let dir = work.join(rung.label);
        std::fs::create_dir_all(&dir)
            .map_err(|e| MediaError::ProcessingFailed { reason: format!("rung dir: {e}") })?;
        let maxrate = rung.bitrate_k * 107 / 100;
        let bufsize = rung.bitrate_k * 3 / 2;
        run_ffmpeg(&[
            "-y", "-i", &input.to_string_lossy(),
            "-vf", &format!("scale={}:-2", rung.width),
            "-c:v", "libx264", "-profile:v", "high",
            "-b:v", &format!("{}k", rung.bitrate_k),
            "-maxrate", &format!("{maxrate}k"), "-bufsize", &format!("{bufsize}k"),
            "-c:a", "aac", "-b:a", &format!("{AUDIO_BITRATE_K}k"), "-ac", "2",
            "-f", "hls", "-hls_time", &HLS_SEGMENT_SECS.to_string(),
            "-hls_playlist_type", "vod", "-hls_segment_type", "fmp4",
            "-hls_fmp4_init_filename", "init.mp4",
            "-hls_segment_filename", &dir.join("seg_%03d.m4s").to_string_lossy(),
            &dir.join("playlist.m3u8").to_string_lossy(),
        ])?;
        collect_dir(&dir, rung.label, &mut files)?;
        variants.push(Variant { rung, height: out_h });
    }

    // Hand-built master playlist over the produced rungs.
    let master = build_master_playlist(&variants);
    let master_bytes = master.into_bytes();
    files.push(OutputFile {
        relative: "master.m3u8".to_owned(),
        content_type: MANIFEST_MIME,
        bytes: master_bytes.clone(),
    });

    // BlurHash from the poster (4×3 components, as for images).
    let small = poster_img.thumbnail(64, 64).to_rgba8();
    let blurhash = blurhash::encode(4, 3, small.width(), small.height(), small.as_raw())
        .map_err(|e| MediaError::ProcessingFailed { reason: format!("blurhash: {e}") })?;

    let top = variants.first().ok_or_else(|| MediaError::ProcessingFailed {
        reason: "no rungs produced".to_owned(),
    })?;
    let top_dimensions = Dimensions::new(top.rung.width, top.height)?;

    Ok(TranscodeArtifacts {
        master_size: master_bytes.len() as u64,
        poster_size: poster_bytes.len() as u64,
        files,
        top_dimensions,
        poster_dimensions,
        blurhash,
    })
}

struct Variant {
    rung: &'static Rung,
    height: u32,
}

/// Which rungs to produce for a source of the given width: never wider than the
/// source (no upscaling), always at least the narrowest rung.
fn plan_ladder(src_w: u32) -> Vec<&'static Rung> {
    let applicable: Vec<&'static Rung> = LADDER.iter().filter(|r| r.width <= src_w).collect();
    if applicable.is_empty() {
        vec![&LADDER[LADDER.len() - 1]]
    } else {
        applicable
    }
}

/// The even output height for a target width preserving the source aspect ratio
/// (H.264 requires even dimensions; `scale=W:-2` does the same in ffmpeg).
fn even_height(width: u32, src_w: u32, src_h: u32) -> u32 {
    if src_w == 0 {
        return 0;
    }
    let h = (width as u64 * src_h as u64 / src_w as u64) as u32;
    h - (h % 2)
}

fn build_master_playlist(variants: &[Variant]) -> String {
    let mut out = String::from("#EXTM3U\n#EXT-X-VERSION:7\n");
    for v in variants {
        // Peak bandwidth ≈ video maxrate + audio, in bits/s.
        let bandwidth = (v.rung.bitrate_k * 107 / 100 + AUDIO_BITRATE_K) * 1000;
        out.push_str(&format!(
            "#EXT-X-STREAM-INF:BANDWIDTH={},RESOLUTION={}x{},CODECS=\"avc1.640028,mp4a.40.2\"\n{}/playlist.m3u8\n",
            bandwidth, v.rung.width, v.height, v.rung.label
        ));
    }
    out
}

/// Recursively collects every file under `dir` into `files`, keyed by
/// `{prefix}/{name}` (matching the HLS references in the rung playlist).
fn collect_dir(dir: &Path, prefix: &str, files: &mut Vec<OutputFile>) -> Result<(), MediaError> {
    let entries = std::fs::read_dir(dir)
        .map_err(|e| MediaError::ProcessingFailed { reason: format!("read rung dir: {e}") })?;
    for entry in entries {
        let entry = entry.map_err(|e| MediaError::ProcessingFailed { reason: format!("dir entry: {e}") })?;
        let path = entry.path();
        let name = entry.file_name().to_string_lossy().into_owned();
        let bytes = std::fs::read(&path)
            .map_err(|e| MediaError::ProcessingFailed { reason: format!("read {name}: {e}") })?;
        files.push(OutputFile {
            relative: format!("{prefix}/{name}"),
            content_type: content_type_for(&name),
            bytes,
        });
    }
    Ok(())
}

fn content_type_for(name: &str) -> &'static str {
    match name.rsplit('.').next() {
        Some("m3u8") => "application/vnd.apple.mpegurl",
        Some("m4s") => "video/iso.segment",
        Some("mp4") => "video/mp4",
        Some("jpg") | Some("jpeg") => "image/jpeg",
        _ => "application/octet-stream",
    }
}

pub(crate) fn run_ffmpeg(args: &[&str]) -> Result<(), MediaError> {
    let output = Command::new("ffmpeg")
        .args(args)
        .output()
        .map_err(|e| MediaError::ProcessingFailed { reason: format!("ffmpeg could not run: {e}") })?;
    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr);
        return Err(MediaError::TranscodeFailed {
            reason: stderr.lines().last().unwrap_or("ffmpeg failed").to_owned(),
        });
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ladder_drops_rungs_wider_than_the_source() {
        assert_eq!(plan_ladder(1080).iter().map(|r| r.label).collect::<Vec<_>>(), ["1080w", "720w", "480w"]);
        assert_eq!(plan_ladder(720).iter().map(|r| r.label).collect::<Vec<_>>(), ["720w", "480w"]);
        assert_eq!(plan_ladder(500).iter().map(|r| r.label).collect::<Vec<_>>(), ["480w"]);
    }

    #[test]
    fn ladder_never_empty_even_for_tiny_sources() {
        // A source narrower than the smallest rung still gets one rung (the floor).
        assert_eq!(plan_ladder(240).iter().map(|r| r.label).collect::<Vec<_>>(), ["480w"]);
    }

    #[test]
    fn even_height_preserves_aspect_and_stays_even() {
        // 9:16 source at width 1080 → 1920; at 720 → 1280; at 480 → 854 (even).
        assert_eq!(even_height(1080, 1080, 1920), 1920);
        assert_eq!(even_height(720, 1080, 1920), 1280);
        assert_eq!(even_height(480, 1080, 1920), 852); // 853.33 floored to even
        assert_eq!(even_height(480, 1080, 1920) % 2, 0);
    }

    #[test]
    fn master_playlist_lists_every_variant_with_resolution() {
        let variants = vec![
            Variant { rung: &LADDER[0], height: 1920 },
            Variant { rung: &LADDER[1], height: 1280 },
        ];
        let master = build_master_playlist(&variants);
        assert!(master.starts_with("#EXTM3U"));
        assert_eq!(master.matches("#EXT-X-STREAM-INF").count(), 2);
        assert!(master.contains("RESOLUTION=1080x1920"));
        assert!(master.contains("1080w/playlist.m3u8"));
        assert!(master.contains("720w/playlist.m3u8"));
    }

    #[test]
    fn content_types_cover_the_hls_tree() {
        assert_eq!(content_type_for("master.m3u8"), "application/vnd.apple.mpegurl");
        assert_eq!(content_type_for("seg_000.m4s"), "video/iso.segment");
        assert_eq!(content_type_for("init.mp4"), "video/mp4");
        assert_eq!(content_type_for("poster.jpg"), "image/jpeg");
    }

    /// End-to-end over the real ffmpeg binary. Ignored by default (needs
    /// `ffmpeg` on PATH); run with:
    /// `cargo test -p media -- --ignored real_ffmpeg_transcode`.
    #[test]
    #[ignore]
    fn real_ffmpeg_transcode_produces_the_full_ladder() {
        // Generate a 2s 1080×1920 H.264 clip with audio.
        let src = std::env::temp_dir().join("media-transcode-src.mp4");
        let ok = Command::new("ffmpeg")
            .args(["-y", "-f", "lavfi", "-i", "testsrc=duration=2:size=1080x1920:rate=30"])
            .args(["-f", "lavfi", "-i", "sine=frequency=440:duration=2"])
            .args(["-c:v", "libx264", "-profile:v", "high", "-pix_fmt", "yuv420p", "-c:a", "aac", "-shortest"])
            .arg(&src)
            .status()
            .expect("ffmpeg gen");
        assert!(ok.success());
        let bytes = std::fs::read(&src).expect("read src");
        let _ = std::fs::remove_file(&src);

        let art = generate_artifacts(&bytes, "e2e-transcode").expect("transcode");

        // Full 3-rung ladder + master + poster, with a non-empty blurhash.
        assert!(art.files.iter().any(|f| f.relative == "master.m3u8"));
        assert!(art.files.iter().any(|f| f.relative == "poster.jpg"));
        for rung in ["1080w", "720w", "480w"] {
            assert!(art.files.iter().any(|f| f.relative == format!("{rung}/playlist.m3u8")));
            assert!(art.files.iter().any(|f| f.relative == format!("{rung}/init.mp4")));
            assert!(art.files.iter().any(|f| f.relative.starts_with(&format!("{rung}/seg_"))));
        }
        assert_eq!(art.top_dimensions, Dimensions::new(1080, 1920).unwrap());
        assert!(!art.blurhash.is_empty());
    }
}
