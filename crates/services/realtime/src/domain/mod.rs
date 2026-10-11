//! The pure domain layer for the realtime delivery plane — the connection model
//! and the rules that govern a live socket.
//!
//! The centre of gravity is the [`connection::Connection`] aggregate: a
//! clock-injected, I/O-free model of one client socket that owns its pinned
//! [`session::Session`], its bounded [`subscription::SubscriptionSet`], its
//! lifecycle state machine, and its heartbeat freshness. Every decision the plane
//! makes about a socket is a method here and is unit-testable without a broker, a
//! WebSocket, Redis, or a wall clock.
//!
//! Two rules are load-bearing and live in this layer:
//! * **Channel-scope authorization** ([`session::Session::authorize`]) — the
//!   plane's only authorization: a connection may subscribe only to channels
//!   whose key it owns. Content visibility was decided upstream at emit time.
//! * **Per-`(connection, channel)` sequencing** ([`value_object::SequenceState`])
//!   — the monotonic stream sequence + ack watermark that drive client dedup and
//!   at-least-once acknowledgement, with no durable buffer (resume gap-fill is the
//!   client's job against the owning SoR).
//!
//! The proto types (`realtime-api`) are deliberately absent here; the mapping
//! between these pure types and the generated wire types lives in the
//! infrastructure tier.

pub mod connection;
pub mod session;
pub mod subscription;
pub mod value_object;

pub use connection::{Connection, ConnectionState, CloseReason};
pub use session::{Session, TokenIds};
pub use subscription::SubscriptionSet;
pub use value_object::{
    ChannelClass, ChannelKey, ChannelRef, ConnectionId, DeliveryGuarantee, DeviceId, NodeId,
    PresenceState, SequenceState, StreamSeq, UserId,
};
