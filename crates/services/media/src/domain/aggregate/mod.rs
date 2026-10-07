//! Aggregates for the media domain — the consistency boundary that owns its
//! invariants and emits domain events.
//!
//! * [`Asset`] — the lifecycle-bearing System-of-Record root (state machine +
//!   rendition catalog + compliance hold). [`Rendition`] is its child entity.

pub mod asset;

pub use asset::{Asset, AssetSnapshot, FinalizeParams, Rendition, ReserveParams, PRIVATE_DOCUMENT_BACKSTOP};
