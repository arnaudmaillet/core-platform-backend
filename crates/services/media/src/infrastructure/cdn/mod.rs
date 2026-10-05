//! The delivery-plane adapter: turn a content-addressed key into a CDN URL.
//!
//! Public media gets a stable, immutable URL off the CDN base; private media gets a
//! short-lived signed URL minted via the object store. Edge invalidation is the
//! takedown-only path: CloudFront `CreateInvalidation` when a distribution is
//! configured, a log line otherwise (local runs, no CDN).

pub mod cloudfront_cdn_gateway;
pub mod cloudfront_invalidator;
mod sigv4;

pub use cloudfront_cdn_gateway::CloudFrontCdnGateway;
pub use cloudfront_invalidator::{CloudFrontInvalidator, CloudFrontInvalidatorConfig};
