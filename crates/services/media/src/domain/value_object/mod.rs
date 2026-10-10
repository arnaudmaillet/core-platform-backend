//! Value objects for the media domain — small, validated, immutable types the
//! [`Asset`](crate::domain::aggregate::Asset) aggregate is built from. Each
//! enforces its own invariants at construction so an invalid value is
//! unrepresentable upstream.

pub mod asset_state;
pub mod blurhash;
pub mod constraints;
pub mod content_hash;
pub mod delivery_visibility;
pub mod dimensions;
pub mod ids;
pub mod media_kind;
pub mod mime_type;
pub mod rendition_kind;
pub mod storage_key;
pub mod upload_key;
pub mod upload_ticket;

pub use asset_state::AssetState;
pub use blurhash::Blurhash;
pub use constraints::UploadConstraints;
pub use content_hash::ContentHash;
pub use delivery_visibility::DeliveryVisibility;
pub use dimensions::Dimensions;
pub use ids::{AssetId, OwnerId};
pub use media_kind::MediaKind;
pub use mime_type::MimeType;
pub use rendition_kind::RenditionKind;
pub use storage_key::StorageKey;
pub use upload_key::UploadKey;
pub use upload_ticket::UploadTicket;
