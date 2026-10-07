//! Outbound ports — the only contracts the application layer holds against the
//! outside world. Concrete adapters (S3/MinIO object storage, Postgres metadata
//! SoR, Redis delivery cache, the image processor, the CDN signer, the moderation
//! gRPC client, the Kafka publisher) live in `infrastructure` (Phase 4) and are
//! injected at the composition root. Each is an `async_trait` so it can be held as
//! `Arc<dyn …>`; in-memory fakes back the unit tests.
//!
//! The defining boundary: the [`ObjectStore`] and [`CdnGateway`] ports broker the
//! *byte plane* in URLs and keys — they never pass bytes through the application
//! layer. A handler asks for a pre-signed URL or a delivery URL; the bytes travel
//! client ⇄ store ⇄ CDN, out of band.

pub mod access_log;
pub mod asset_repository;
pub mod cdn_gateway;
pub mod delivery_cache;
pub mod event_publisher;
pub mod image_processor;
pub mod malware_scanner;
pub mod media_probe;
pub mod moderation_screen;
pub mod object_store;
pub mod video_transcoder;

pub use access_log::{AccessLog, DocumentView};
pub use asset_repository::AssetRepository;
pub use cdn_gateway::{CdnGateway, ResolvedUrl};
pub use delivery_cache::DeliveryCache;
pub use event_publisher::EventPublisher;
pub use image_processor::{DerivedRenditions, ImageProcessor};
pub use malware_scanner::{MalwareScanner, ScanVerdict};
pub use media_probe::{MediaProbe, MediaProbeReport};
pub use moderation_screen::{ModerationScreen, ScreenDecision};
pub use object_store::{ObjectHead, ObjectStore, PresignedUpload};
pub use video_transcoder::{TranscodeOutput, VideoTranscoder};
