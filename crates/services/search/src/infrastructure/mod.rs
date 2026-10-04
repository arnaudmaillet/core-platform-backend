//! Infrastructure adapters — the concrete implementations of the application ports,
//! injected at the composition root (Phase 5).
//!
//! Phase 4 delivers the two outbound-facing pieces:
//!
//! - [`index`] — the OpenSearch `SearchIndex` + `IndexAdmin` adapter and its versioned mappings (verified against a live engine in Phase 6).
//! - [`decode`] — the pure inbound wire→`SourceEvent` decode layer.
//!
//! Deferred (documented in the blueprint, wired/built later):
//!
//! - the gRPC **content hydrator** turning a [`decode::ContentRef`] into a fat `SourceEvent` (thin `post.v1.events` carry no content) — Phase 5;
//! - `profile.v1.events` ingestion — profile does not yet publish a Kafka stream (an upstream prerequisite);
//! - an optional Meilisearch adapter (local-velocity only, never a relevance target).

pub mod audience;
pub mod consumer;
pub mod decode;
pub mod grpc;
pub mod hydrate;
pub mod index;
