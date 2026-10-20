//! The transformation engine (Plane B): derive the image resize ladder / video HLS
//! ladder + a BlurHash from the validated master, writing each content-addressed
//! derivative back to the object store.

pub mod ffmpeg_sheet_baker;
pub mod ffmpeg_video_transcoder;
pub mod image_rendition_processor;

pub use ffmpeg_sheet_baker::FfmpegSheetBaker;
pub use ffmpeg_video_transcoder::FfmpegVideoTranscoder;
pub use image_rendition_processor::ImageRenditionProcessor;
