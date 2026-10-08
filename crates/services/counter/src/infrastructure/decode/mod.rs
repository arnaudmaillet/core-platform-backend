//! The inbound event-decode layer: counter-owned wire DTOs ([`wire`]) and the pure
//! `map_*` distillation into domain [`Observation`](crate::domain::Observation)s
//! ([`decoder`]). Engine-free and unit-tested; the consumer (Phase 5) deserializes
//! the bytes and drives these mappers.

pub mod decoder;
pub mod wire;

pub use decoder::{map_click, map_follow, map_impression, map_stake, map_view};
pub use wire::{FollowWire, HitWire, WalletWire};
