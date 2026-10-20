//! [`SheetBaker`] over ffmpeg and `image` (#945, #948): an animated avatar's
//! clip becomes two AVIF sprite sheets (128 / 256 px cells) that loop
//! seamlessly, and a JPEG still of their last frame.
//!
//! 1. ffmpeg takes the clip's first 3 s, muted, at [`SHEET_FPS`], centre-cropped
//!    square ([`MAX_FRAMES`] frames at most).
//! 2. The window of [`SHEET_FRAMES`] frames whose start best matches the frame
//!    right after its end is chosen, and the frames after it are cross-faded
//!    into its head, so the last cell flows into the first (#948 §2).
//! 3. The seam (last cell → first cell) is measured; above [`MAX_SEAM`] the
//!    clip is refused.
//! 4. The sheets are composed (each frame inside a transparent gutter) and
//!    encoded; the still is the last cell.

use std::path::Path;
use std::sync::Arc;

use async_trait::async_trait;
use image::{imageops, DynamicImage, GrayImage, ImageEncoder, Rgba, RgbaImage};

use crate::application::port::{SheetBaker, TranscodeOutput};
use crate::domain::aggregate::Rendition;
use crate::domain::value_object::sheet_geometry::{SHEET_FPS, SHEET_FRAMES};
use crate::domain::value_object::{
    Blurhash, ContentHash, Dimensions, MimeType, ObjectRoot, RenditionKind, SheetGeometry, StorageKey,
};
use crate::error::MediaError;
use crate::infrastructure::processor::ffmpeg_video_transcoder::run_ffmpeg;
use crate::infrastructure::store::S3Client;

/// The clip's seconds used.
const CLIP_SECS: u32 = 3;
/// Frames extracted at most: [`CLIP_SECS`] at [`SHEET_FPS`].
const MAX_FRAMES: usize = (CLIP_SECS * SHEET_FPS) as usize;
/// Frames after the window cross-faded into its head.
const CROSSFADE: usize = 4;
/// The largest cell, extracted once and scaled down for the others.
const EXTRACT_PX: u32 = 256;
/// The loop seam's mean luma difference (0–1) above which a clip is refused.
const MAX_SEAM: f64 = 0.18;
/// AVIF encoding: rav1e speed (1 slow – 10 fast) and quality (1–100).
const AVIF_SPEED: u8 = 8;
const AVIF_QUALITY: u8 = 70;

pub struct FfmpegSheetBaker {
    store: Arc<S3Client>,
}

impl FfmpegSheetBaker {
    pub fn new(store: Arc<S3Client>) -> Self {
        Self { store }
    }
}

#[async_trait]
impl SheetBaker for FfmpegSheetBaker {
    async fn bake(&self, source: &StorageKey, hash: &ContentHash, root: &ObjectRoot) -> Result<TranscodeOutput, MediaError> {
        let bytes = self.store.get_bytes(source.as_str()).await?;
        let hash_hex = hash.as_str().to_owned();
        let baked = tokio::task::spawn_blocking(move || bake_clip(&bytes, &hash_hex))
            .await
            .map_err(|e| MediaError::ProcessingFailed { reason: format!("bake task panicked: {e}") })??;

        let mut renditions = Vec::new();
        for sheet in &baked.sheets {
            let key = root.object(&sheet.file);
            self.store.put_bytes(key.as_str(), sheet.bytes.clone(), "image/avif").await?;
            renditions.push(Rendition::new(sheet.kind, MimeType::new("image/avif")?, key, sheet.dimensions, sheet.bytes.len() as u64));
        }
        let still_key = root.object("still.jpg");
        self.store.put_bytes(still_key.as_str(), baked.still.clone(), "image/jpeg").await?;
        renditions.push(Rendition::new(
            RenditionKind::Poster,
            MimeType::new("image/jpeg")?,
            still_key,
            Dimensions::new(EXTRACT_PX, EXTRACT_PX)?,
            baked.still.len() as u64,
        ));
        Ok(TranscodeOutput { renditions, blurhash: Blurhash::new(baked.blurhash)? })
    }
}

/// A sheet, encoded.
struct BakedSheet {
    kind:       RenditionKind,
    file:       String,
    bytes:      Vec<u8>,
    dimensions: Dimensions,
}

struct Baked {
    sheets:   Vec<BakedSheet>,
    still:    Vec<u8>,
    blurhash: String,
}

/// Blocking: extracts the frames, loops them, composes and encodes.
fn bake_clip(source: &[u8], hash_hex: &str) -> Result<Baked, MediaError> {
    let work = std::env::temp_dir().join(format!("media-sheet-{hash_hex}"));
    let _ = std::fs::remove_dir_all(&work);
    std::fs::create_dir_all(&work).map_err(|e| MediaError::ProcessingFailed { reason: format!("workdir: {e}") })?;
    let result = (|| {
        let input = work.join("input");
        std::fs::write(&input, source).map_err(|e| MediaError::ProcessingFailed { reason: format!("stage input: {e}") })?;
        let frames = extract_frames(&input, &work)?;
        bake_frames(&frames)
    })();
    let _ = std::fs::remove_dir_all(&work);
    result
}

fn extract_frames(input: &Path, work: &Path) -> Result<Vec<RgbaImage>, MediaError> {
    let filter = format!(
        "fps={SHEET_FPS},scale={EXTRACT_PX}:{EXTRACT_PX}:force_original_aspect_ratio=increase,crop={EXTRACT_PX}:{EXTRACT_PX}"
    );
    run_ffmpeg(&[
        "-y", "-t", &CLIP_SECS.to_string(), "-i", &input.to_string_lossy(), "-an", "-vf", &filter,
        "-frames:v", &MAX_FRAMES.to_string(), &work.join("f_%03d.png").to_string_lossy(),
    ])?;
    let mut paths: Vec<_> = std::fs::read_dir(work)
        .map_err(|e| MediaError::ProcessingFailed { reason: format!("read frames: {e}") })?
        .filter_map(Result::ok)
        .map(|e| e.path())
        .filter(|p| p.file_name().is_some_and(|n| n.to_string_lossy().starts_with("f_")))
        .collect();
    paths.sort();
    paths
        .iter()
        .map(|p| {
            image::open(p)
                .map(|img| img.to_rgba8())
                .map_err(|e| MediaError::CorruptMedia { reason: format!("frame decode: {e}") })
        })
        .collect()
}

/// The frames → sheets + still (pure but for encoding; tested on synthetic
/// frames).
fn bake_frames(frames: &[RgbaImage]) -> Result<Baked, MediaError> {
    let looped = seamless_loop(frames)?;
    let seam = seam(&looped[looped.len() - 1], &looped[0]);
    if seam > MAX_SEAM {
        return Err(MediaError::CorruptMedia { reason: format!("the clip does not loop (seam {seam:.3} > {MAX_SEAM})") });
    }
    let mut sheets = Vec::new();
    for kind in [RenditionKind::SheetSmall, RenditionKind::SheetLarge] {
        let geometry = SheetGeometry::of(kind).ok_or_else(|| MediaError::ProcessingFailed { reason: "not a sheet".into() })?;
        let sheet = compose(&looped, &geometry);
        let (w, h) = sheet.dimensions();
        sheets.push(BakedSheet {
            kind,
            file: format!("sheet_{}.avif", geometry.cell_px),
            bytes: encode_avif(&sheet)?,
            dimensions: Dimensions::new(w, h)?,
        });
    }
    let last = DynamicImage::ImageRgba8(looped[looped.len() - 1].clone()).to_rgb8();
    let mut still = Vec::new();
    image::codecs::jpeg::JpegEncoder::new_with_quality(&mut still, 85)
        .encode_image(&last)
        .map_err(|e| MediaError::ProcessingFailed { reason: format!("still encode: {e}") })?;
    let small = imageops::thumbnail(&looped[looped.len() - 1], 64, 64);
    let blurhash = blurhash::encode(4, 3, small.width(), small.height(), small.as_raw())
        .map_err(|e| MediaError::ProcessingFailed { reason: format!("blurhash: {e}") })?;
    Ok(Baked { sheets, still, blurhash })
}

/// [`SHEET_FRAMES`] frames that loop: the window whose start best matches
/// the frame after its end, then that frame and the next ones (up to
/// [`CROSSFADE`]) cross-faded into its head — the head starts as the tail's
/// continuation and blends back into the clip, so cell N-1 → cell 0 is the
/// clip's own next step.
fn seamless_loop(frames: &[RgbaImage]) -> Result<Vec<RgbaImage>, MediaError> {
    let n = SHEET_FRAMES as usize;
    if frames.len() < n {
        return Err(MediaError::CorruptMedia {
            reason: format!("the clip is shorter than {} s ({} frames)", n as u32 / SHEET_FPS, frames.len()),
        });
    }
    let k = CROSSFADE.min(frames.len() - n);
    let thumbs: Vec<GrayImage> = frames.iter().map(luma_thumb).collect();
    let start = if k == 0 {
        0
    } else {
        (0..=frames.len() - n - k)
            .min_by(|a, b| {
                let da = thumb_diff(&thumbs[a + n], &thumbs[*a]);
                let db = thumb_diff(&thumbs[b + n], &thumbs[*b]);
                da.total_cmp(&db)
            })
            .unwrap_or(0)
    };
    Ok((0..n)
        .map(|j| {
            let own = &frames[start + j];
            if j < k {
                // Weight of the clip's own head grows from 1/(k+1) to k/(k+1).
                let alpha = (j + 1) as f32 / (k + 1) as f32;
                blend(&frames[start + n + j], own, alpha)
            } else {
                own.clone()
            }
        })
        .collect())
}

/// `a` faded into `b` by `alpha` (0: `a`, 1: `b`).
fn blend(a: &RgbaImage, b: &RgbaImage, alpha: f32) -> RgbaImage {
    RgbaImage::from_fn(a.width(), a.height(), |x, y| {
        let (pa, pb) = (a.get_pixel(x, y).0, b.get_pixel(x, y).0);
        Rgba(std::array::from_fn(|i| (pa[i] as f32 * (1.0 - alpha) + pb[i] as f32 * alpha).round() as u8))
    })
}

fn luma_thumb(frame: &RgbaImage) -> GrayImage {
    imageops::grayscale(&imageops::resize(frame, 32, 32, imageops::FilterType::Triangle))
}

fn thumb_diff(a: &GrayImage, b: &GrayImage) -> f64 {
    let total: u64 = a.pixels().zip(b.pixels()).map(|(p, q)| p.0[0].abs_diff(q.0[0]) as u64).sum();
    total as f64 / (a.len() as f64 * 255.0)
}

/// The mean luma difference (0–1) between two frames, on 32 × 32 thumbnails.
fn seam(last: &RgbaImage, first: &RgbaImage) -> f64 {
    thumb_diff(&luma_thumb(last), &luma_thumb(first))
}

/// The frames laid out row by row, each inside its cell's transparent gutter.
fn compose(frames: &[RgbaImage], geometry: &SheetGeometry) -> RgbaImage {
    let (w, h) = geometry.size();
    let mut sheet = RgbaImage::from_pixel(w, h, Rgba([0, 0, 0, 0]));
    let inner = geometry.cell_px - 2 * geometry.gutter_px;
    for (i, frame) in frames.iter().enumerate() {
        let (col, row) = (i as u32 % geometry.columns, i as u32 / geometry.columns);
        let cell = imageops::resize(frame, inner, inner, imageops::FilterType::Lanczos3);
        let (x, y) = (col * geometry.cell_px + geometry.gutter_px, row * geometry.cell_px + geometry.gutter_px);
        imageops::overlay(&mut sheet, &cell, i64::from(x), i64::from(y));
    }
    sheet
}

fn encode_avif(sheet: &RgbaImage) -> Result<Vec<u8>, MediaError> {
    let mut bytes = Vec::new();
    image::codecs::avif::AvifEncoder::new_with_speed_quality(&mut bytes, AVIF_SPEED, AVIF_QUALITY)
        .write_image(sheet.as_raw(), sheet.width(), sheet.height(), image::ExtendedColorType::Rgba8)
        .map_err(|e| MediaError::ProcessingFailed { reason: format!("sheet encode: {e}") })?;
    Ok(bytes)
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A uniform frame of grey level `v`.
    fn flat(v: u8) -> RgbaImage {
        RgbaImage::from_pixel(EXTRACT_PX, EXTRACT_PX, Rgba([v, v, v, 255]))
    }

    /// A ramp: brightness grows a step a frame (an unlooped pan).
    fn ramp(len: usize) -> Vec<RgbaImage> {
        (0..len).map(|i| flat((i * 7).min(255) as u8)).collect()
    }

    #[test]
    fn a_clip_shorter_than_two_seconds_is_refused() {
        assert!(matches!(seamless_loop(&ramp(23)), Err(MediaError::CorruptMedia { .. })));
    }

    #[test]
    fn the_tail_is_cross_faded_into_the_head_so_the_seam_is_small() {
        let frames = ramp(MAX_FRAMES);
        let looped = seamless_loop(&frames).unwrap();
        assert_eq!(looped.len(), SHEET_FRAMES as usize);
        let raw = seam(&frames[SHEET_FRAMES as usize - 1], &frames[0]);
        let baked = seam(&looped[looped.len() - 1], &looped[0]);
        assert!(baked < raw / 3.0, "seam {baked} vs raw {raw}");
        assert!(baked < MAX_SEAM);
    }

    #[test]
    fn the_window_whose_end_meets_its_start_is_chosen() {
        // 36 frames: 4 dark ones, then a 12-frame cycle: only a window
        // starting in the cycle meets its start again 24 frames later.
        let frames: Vec<RgbaImage> =
            (0..36).map(|i| if i < 4 { flat(0) } else { flat(100 + (i % 12) as u8 * 10) }).collect();
        let looped = seamless_loop(&frames).unwrap();
        // The chosen window starts in the cycle, not in the dark stretch.
        assert!(looped.iter().all(|f| f.get_pixel(0, 0).0[0] >= 90), "the dark stretch is skipped");
    }

    #[test]
    fn an_exact_two_second_clip_that_cuts_is_refused() {
        // No frame after the window to fade in, and a hard cut black → white.
        let mut frames: Vec<RgbaImage> = (0..12).map(|_| flat(0)).collect();
        frames.extend((0..12).map(|_| flat(255)));
        assert!(matches!(bake_frames(&frames), Err(MediaError::CorruptMedia { .. })));
    }

    #[test]
    fn a_sheet_holds_each_frame_inside_a_transparent_gutter() {
        let geometry = SheetGeometry::of(RenditionKind::SheetSmall).unwrap();
        let frames: Vec<RgbaImage> = (0..SHEET_FRAMES).map(|_| flat(200)).collect();
        let sheet = compose(&frames, &geometry);
        assert_eq!(sheet.dimensions(), (512, 768));
        assert_eq!(sheet.get_pixel(0, 0).0[3], 0, "the gutter is transparent");
        assert_eq!(sheet.get_pixel(127, 64).0[3], 0, "on the cell's right edge too");
        assert_eq!(sheet.get_pixel(64, 64).0, [200, 200, 200, 255], "the frame inside");
        assert_eq!(sheet.get_pixel(128 * 3 + 64, 128 * 5 + 64).0[3], 255, "the 24th cell is filled");
    }

    /// End-to-end over the real ffmpeg binary (and the AVIF encoder). Ignored
    /// by default; run with `cargo test -p media -- --ignored real_ffmpeg_bake`.
    #[test]
    #[ignore]
    fn real_ffmpeg_bake_makes_two_sheets_and_a_still() {
        let src = std::env::temp_dir().join("media-sheet-src.mp4");
        let ok = std::process::Command::new("ffmpeg")
            .args(["-y", "-f", "lavfi", "-i", "testsrc=duration=3:size=720x1280:rate=30"])
            .args(["-c:v", "libx264", "-pix_fmt", "yuv420p"])
            .arg(&src)
            .status()
            .expect("ffmpeg gen");
        assert!(ok.success());
        let bytes = std::fs::read(&src).expect("read src");
        let _ = std::fs::remove_file(&src);
        // testsrc scrolls without looping: the bake either loops it under the
        // seam limit or refuses it — never anything else.
        match bake_clip(&bytes, "e2e-sheet") {
            Ok(baked) => {
                eprintln!(
                    "baked: sheets {:?} bytes, still {} bytes",
                    baked.sheets.iter().map(|s| s.bytes.len()).collect::<Vec<_>>(),
                    baked.still.len()
                );
                assert_eq!(baked.sheets.len(), 2);
                assert_eq!(baked.sheets[1].dimensions, Dimensions::new(1024, 1536).unwrap());
                assert!(baked.sheets.iter().all(|s| !s.bytes.is_empty()));
                assert!(!baked.still.is_empty() && !baked.blurhash.is_empty());
            }
            Err(MediaError::CorruptMedia { reason }) => {
                eprintln!("refused: {reason}");
                assert!(reason.contains("loop"), "{reason}");
            }
            Err(other) => panic!("unexpected: {other}"),
        }
    }
}
