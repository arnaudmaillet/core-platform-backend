//! Read use-cases — `cqrs::QueryHandler` implementations that ride the query bus.
//! `GetAsset` returns the metadata SoR; `ResolveDelivery` brokers CDN/signed URLs
//! and fails OPEN (a not-ready or partially-resolvable asset yields a placeholder
//! marked `degraded`, never an error on the read path).

pub mod get_asset;
pub mod list_assets_by_owner;
pub mod resolve_delivery;

pub use get_asset::{GetAssetHandler, GetAssetQuery};
pub use list_assets_by_owner::{ListAssetsByOwnerHandler, ListAssetsByOwnerQuery, OwnedAsset, MAX_DOWNLOAD_TTL_SECS};
pub use resolve_delivery::{
    DeliveredMediaView, DeliveredRenditionView, ResolveDeliveryHandler, ResolveDeliveryQuery,
};
